import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io
import time

# ---------- Optional cache ----------
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

st.set_page_config(page_title="FTTH Geocoding & Matching (v5)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v5")

# ========== Sidebar ==========
with st.sidebar:
    st.header("Ρυθμίσεις")
    geocoder = st.selectbox("Geocoder", ["Nominatim (δωρεάν)", "Google (API key)"])
    google_key = st.text_input("Google API key", type="password", help="Αν μείνει κενό, χρησιμοποιείται Nominatim.")
    country = st.text_input("Country code", "gr")
    lang = st.text_input("Language", "el")
    throttle = st.slider("Καθυστέρηση (sec) [Nominatim]", 0.5, 2.0, 1.0, 0.5)
    distance_limit = st.number_input("📏 Μέγιστη απόσταση (m)", min_value=1, max_value=500, value=150)

    st.subheader("Πηγή Επιχειρήσεων")
    biz_source = st.radio("Επιλογή", ["Upload Excel/CSV", "ΓΕΜΗ (OpenData API)"], index=0)
    gemi_key = st.text_input("GΕΜΗ API Key", type="password") if biz_source == "ΓΕΜΗ (OpenData API)" else None

# ========== Uploads & Inputs ==========
st.subheader("📥 Αρχεία")
biz_file = st.file_uploader("Excel/CSV Επιχειρήσεων", type=["xlsx", "csv"]) if biz_source == "Upload Excel/CSV" else None
ftth_file = st.file_uploader("FTTH σημεία Nova (Excel/CSV) – υποστηρίζει ελληνικές στήλες λ/φ και πολλαπλά sheets", type=["xlsx", "csv"])
prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά) – Excel/CSV με στήλες: Address, Latitude, Longitude", type=["xlsx", "csv"])

# ---------- Helpers ----------
def load_table(uploaded):
    if uploaded is None:
        return None
    name = uploaded.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded)
    return pd.read_excel(uploaded)

def pick_first_series(df: pd.DataFrame, candidates):
    """Επιστρέφει μία Series από την πρώτη ταιριαστή στήλη (αν υπάρχουν διπλές, παίρνει την 1η)."""
    for cand in candidates:
        exact = [c for c in df.columns if c.lower() == cand.lower()]
        if exact:
            col = df[exact]
            return col.iloc[:, 0] if isinstance(col, pd.DataFrame) else col
        loose = df.filter(regex=fr"(?i)^{cand}$")
        if loose.shape[1] > 0:
            return loose.iloc[:, 0]
    return pd.Series([""] * len(df), index=df.index, dtype="object")

def _clean_col(s: str) -> str:
    return (
        str(s).lower()
        .replace("(", " ").replace(")", " ")
        .replace("[", " ").replace("]", " ")
        .replace(".", " ").replace(",", " ")
        .replace("ά","α").replace("έ","ε").replace("ή","η")
        .replace("ί","ι").replace("ό","ο").replace("ύ","υ").replace("ώ","ω")
        .strip()
    )

def _find_col(df: pd.DataFrame, patterns: list[str]) -> str | None:
    cleaned = {c: _clean_col(c) for c in df.columns}
    for p in patterns:
        for orig, cl in cleaned.items():
            if p in cl:
                return orig
    return None

def normalize_ftth(df: pd.DataFrame) -> pd.DataFrame:
    """Πιάνει EN/GR: latitude/longitude ή γεωγραφικο πλατος (φ) / μηκος (λ), κόμμα→τελεία, float."""
    lat_col = _find_col(df, ["latitude", "lat", "πλατος", "γεωγραφικο πλατος", "φ"])
    lon_col = _find_col(df, ["longitude", "lon", "long", "μηκος", "γεωγραφικο μηκος", "λ"])
    if not lat_col or not lon_col:
        raise ValueError("Δεν βρέθηκαν στήλες latitude/longitude (δοκιμάστηκαν και ελληνικά: Πλάτος/Μήκος).")
    out = df[[lat_col, lon_col]].rename(columns={lat_col: "latitude", lon_col: "longitude"}).copy()
    out["latitude"]  = pd.to_numeric(out["latitude"].astype(str).str.replace(",", "."), errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"].astype(str).str.replace(",", "."), errors="coerce")
    out = out.dropna(subset=["latitude","longitude"])
    return out

def _first_key(d: dict, keys: list[str], default=""):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return default

def _to_excel_bytes(df: pd.DataFrame):
    output = io.BytesIO()
    if df is None or df.empty:
        df = pd.DataFrame([{"info": "no data"}])
    df.columns = [str(c) for c in df.columns]
    for c in df.columns:
        df[c] = df[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
    with pd.ExcelWriter(output, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    output.seek(0)
    return output

# ---------- FTTH load (Nova) ----------
ftth_df = None
if ftth_file is not None:
    if ftth_file.name.lower().endswith(".xlsx"):
        xls = pd.ExcelFile(ftth_file)
        st.caption("Nova: Διάλεξε sheet που περιέχει τις συντεταγμένες (λ/φ).")
        sheet_coords = st.selectbox("📄 Sheet συντεταγμένων (Nova)", xls.sheet_names, index=0)
        df_coords = pd.read_excel(xls, sheet_name=sheet_coords)
        ftth_df = normalize_ftth(df_coords)
    else:
        raw = load_table(ftth_file)
        ftth_df = normalize_ftth(raw)

# ---------- Biz source ----------
biz_df = None
if biz_source == "Upload Excel/CSV":
    biz_df = load_table(biz_file) if biz_file else None

# ---------- GEMI (OpenData API) ----------
GEMI_BASE = "https://opendata-api.businessportal.gr/opendata"

def gemi_params(api_key, what):
    headers = {"X-API-Key": api_key}
    r = requests.get(f"{GEMI_BASE}/params/{what}", headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def gemi_search(api_key, nomos_id=None, dimos_id=None, status_id=None, name_part=None, kad_list=None, page=1, page_size=200):
    headers = {"X-API-Key": api_key}
    payload = {
        "page": page, "page_size": page_size,
        "nomos_id": nomos_id, "dimos_id": dimos_id,
        "status_id": status_id, "name_part": name_part, "kad": kad_list or []
    }
    r = requests.post(f"{GEMI_BASE}/search", json=payload, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

gemi_df = None
if biz_source == "ΓΕΜΗ (OpenData API)":
    if not gemi_key:
        st.warning("🔑 Βάλε GΕΜΗ API Key για να ενεργοποιηθεί η αναζήτηση.")
    else:
        try:
            nomoi = gemi_params(gemi_key, "nomoi")
            statuses = gemi_params(gemi_key, "statuses")

            nomos_names = [n["name"] for n in nomoi]
            sel_nomos = st.selectbox("Νομός", nomos_names, index=0)
            nomos_id = next(n["id"] for n in nomoi if n["name"] == sel_nomos)

            dimoi = gemi_params(gemi_key, f"dimoi/{nomos_id}")
            dimos_names = [d["name"] for d in dimoi]
            sel_dimos = st.selectbox("Δήμος", dimos_names, index=0)
            dimos_id = next(d["id"] for d in dimoi if d["name"] == sel_dimos)

            status_names = [s["name"] for s in statuses]
            default_status = next((i for i,s in enumerate(statuses) if "ενεργ" in s["name"].lower()), 0)
            sel_status = st.selectbox("Κατάσταση", status_names, index=default_status)
            status_id = next(s["id"] for s in statuses if s["name"] == sel_status)

            name_part = st.text_input("Κομμάτι επωνυμίας (προαιρετικό)", "")

            if st.button("🔎 Αναζήτηση ΓΕΜΗ"):
                data = gemi_search(gemi_key, nomos_id=nomos_id, dimos_id=dimos_id, status_id=status_id, name_part=name_part)
                rows = []
                for it in data.get("items", []):
                    name  = _first_key(it, ["name", "company_name"])
                    addr  = _first_key(it, ["address", "postal_address", "registered_address"])
                    city  = _first_key(it, ["municipality", "dimos_name", "city"])
                    afm   = _first_key(it, ["afm", "vat_number", "tin"])
                    gemi  = _first_key(it, ["gemi_number", "registry_number", "commercial_registry_no"])
                    phone = _first_key(it, ["phone", "telephone", "contact_phone", "phone_number"])
                    email = _first_key(it, ["email", "contact_email", "email_address"])
                    rows.append({
                        "name": name, "address": addr, "city": city,
                        "afm": afm, "gemi": gemi, "phone": phone, "email": email
                    })
                gemi_df = pd.DataFrame(rows)
                if gemi_df.empty:
                    st.warning("Δεν βρέθηκαν εγγραφές από ΓΕΜΗ με τα φίλτρα που έβαλες.")
                else:
                    st.success(f"Βρέθηκαν {len(gemi_df)} εγγραφές από ΓΕΜΗ.")
                    st.dataframe(gemi_df, use_container_width=True)
                    st.download_button(
                        "⬇️ Κατέβασμα επιχειρήσεων ΓΕΜΗ (Excel)",
                        _to_excel_bytes(gemi_df),
                        file_name="gemi_businesses.xlsx"
                    )
        except Exception as e:
            st.error(f"Σφάλμα ΓΕΜΗ: {e}")

# Αν επιλεγεί ΓΕΜΗ, χρησιμοποίησε αυτά τα δεδομένα ως πηγή επιχειρήσεων
if biz_source == "ΓΕΜΗ (OpenData API)":
    biz_df = gemi_df

# ---------- Geocode cache ----------
if CACHE_OK:
    requests_cache.install_cache("geocode_cache", backend="sqlite", expire_after=60*60*24*14)

session = requests.Session()
session.headers.update({"User-Agent": "ftth-app/1.0 (+contact: user)"})

def geocode_nominatim(address, cc="gr", lang="el"):
    params = {"q": address, "format": "json", "limit": 1, "countrycodes": cc, "accept-language": lang}
    r = session.get("https://nominatim.openstreetmap.org/search", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data:
        return float(data[0]["lat"]), float(data[0]["lon"])
    return None, None

def geocode_google(address, api_key, lang="el"):
    params = {"address": address, "key": api_key, "language": lang}
    r = session.get("https://maps.googleapis.com/maps/api/geocode/json", params=params, timeout=15)
    r.raise_for_status()
    js = r.json()
    if js.get("status") == "OK" and js.get("results"):
        loc = js["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"])
    return None, None

def geocode_address(address, provider, api_key=None, cc="gr", lang="el", throttle_sec=1.0):
    lat, lon = (None, None)
    if provider.startswith("Google") and api_key:
        lat, lon = geocode_google(address, api_key, lang=lang)
    else:
        lat, lon = geocode_nominatim(address, cc, lang)
        # throttle μόνο σε πραγματικό network call (όχι cache)
        if not getattr(session, "cache_disabled", True):
            time.sleep(throttle_sec)
    if lat is None and "greece" not in address.lower() and "ελλάδα" not in address.lower():
        fallback = f"{address}, Greece"
        if provider.startswith("Google") and api_key:
            lat, lon = geocode_google(fallback, api_key, lang=lang)
        else:
            lat, lon = geocode_nominatim(fallback, cc, lang)
            if not getattr(session, "cache_disabled", True):
                time.sleep(throttle_sec)
    return lat, lon

# ========== Main ==========
start = st.button("🚀 Ξεκίνα geocoding & matching")

if start and biz_df is not None and ftth_df is not None:
    work = biz_df.copy()

    # Επιλογή πιθανών στηλών διεύθυνσης/πόλης για κάθε είδος upload
    addr_series = pick_first_series(work, ["address", "site.company_insights.address", "διεύθυνση", "οδός", "διευθυνση"])
    city_series = pick_first_series(work, ["city", "site.company_insights.city", "πόλη"])

    # Τελική διεύθυνση προς geocoding (χωρίς φίλτρο πόλης μετά)
    base_addr = addr_series.astype(str).str.strip()
    from_input_city = city_series.astype(str).str.strip()
    # Αν λείπει city στο αρχείο, κρατάμε μόνο τη διεύθυνση — Google/Nominatim θα το βγάλουν από context
    work["Address"] = (base_addr + (", " + from_input_city).where(from_input_city.ne(""), "")).str.replace(r"\s+", " ", regex=True)

    # Αφαίρεση εντελώς κενών διευθύνσεων
    work = work[work["Address"].str.len() > 3].copy()

    # ----- Line-by-line geocoding (ΟΛΕΣ οι γραμμές) -----
    total = len(work)
    progress = st.progress(0, text=f"0 / {total}")
    errs = 0

    # cache από prev_df (αν δόθηκε)
    geo_map = {}
    if prev_geo_file is not None:
        prev_df = load_table(prev_geo_file)
    else:
        prev_df = None

    if prev_df is not None and {"Address","Latitude","Longitude"}.issubset({c.title() if c.islower() else c for c in prev_df.columns}):
        cols = {c.lower(): c for c in prev_df.columns}
        p = prev_df.rename(columns={cols.get("address","address"): "Address",
                                    cols.get("latitude","latitude"): "Latitude",
                                    cols.get("longitude","longitude"): "Longitude"})
        p["Latitude"]  = pd.to_numeric(p["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
        p["Longitude"] = pd.to_numeric(p["Longitude"].astype(str).str.replace(",", "."), errors="coerce")
        p = p.dropna(subset=["Latitude","Longitude"])
        for _, r in p.iterrows():
            geo_map[str(r["Address"])] = (float(r["Latitude"]), float(r["Longitude"]))

    # Προετοιμασία στηλών
    work["Latitude"] = pd.NA
    work["Longitude"] = pd.NA

    for i, (idx, row) in enumerate(work.iterrows(), start=1):
        addr = str(row["Address"]).strip()
        if addr in geo_map:
            lat, lon = geo_map[addr]
        else:
            lat, lon = geocode_address(addr, geocoder, api_key=google_key, cc=country, lang=lang, throttle_sec=throttle)
            if lat is not None and lon is not None:
                geo_map[addr] = (lat, lon)
            else:
                errs += 1
                lat, lon = (None, None)

        work.at[idx, "Latitude"]  = lat
        work.at[idx, "Longitude"] = lon
        progress.progress(i/max(1,total), text=f"{i} / {total} γεωκωδικοποιημένα...")

    # Κόμμα/τελεία -> float & καθάρισμα
    work["Latitude"]  = pd.to_numeric(work["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
    work["Longitude"] = pd.to_numeric(work["Longitude"].astype(str).str.replace(",", "."), errors="coerce")

    # merged = όλα τα geocoded rows (χωρίς φιλτράρισμα πόλης)
    merged = work.copy()

    # ----- Matching -----
    ftth_points = ftth_df[["latitude","longitude"]].dropna().to_numpy()
    matches = []
    for _, row in merged.dropna(subset=["Latitude","Longitude"]).iterrows():
        try:
            biz_lat = float(str(row["Latitude"]).replace(",", "."))
            biz_lon = float(str(row["Longitude"]).replace(",", "."))
        except Exception:
            continue
        biz_coords = (biz_lat, biz_lon)

        for ft_lat, ft_lon in ftth_points:
            d = geodesic(biz_coords, (float(ft_lat), float(ft_lon))).meters
            if d <= distance_limit:
                matches.append({
                    "name": row.get("name", ""),
                    "Address": row["Address"],
                    "Latitude": biz_lat,
                    "Longitude": biz_lon,
                    "FTTH_lat": float(ft_lat),
                    "FTTH_lon": float(ft_lon),
                    "Distance(m)": round(d, 2)
                })
                break

    result_df = pd.DataFrame(matches)
    if not result_df.empty and "Distance(m)" in result_df.columns:
        result_df = result_df.sort_values("Distance(m)").reset_index(drop=True)

    # ----- UI -----
    if result_df.empty:
        st.warning(f"⚠️ Δεν βρέθηκαν αντιστοιχίσεις εντός {distance_limit} m.")
    else:
        st.success(f"✅ Βρέθηκαν {len(result_df)} επιχειρήσεις εντός {distance_limit} m από FTTH.")
        st.dataframe(result_df, use_container_width=True)

    # ----- Robust Excel export -----
    def to_excel_bytes(df: pd.DataFrame):
        safe = df.copy()
        if safe is None or safe.empty:
            safe = pd.DataFrame([{"info": "no data"}])
        safe.columns = [str(c) for c in safe.columns]
        for c in safe.columns:
            safe[c] = safe[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            safe.to_excel(writer, index=False, sheet_name="Sheet1")
        output.seek(0)
        return output

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("⬇️ Geocoded διευθύνσεις (γραμμή-γραμμή)", to_excel_bytes(merged[["Address","Latitude","Longitude"]]), file_name="geocoded_addresses.xlsx")
    with c2:
        st.download_button("⬇️ Αποτελέσματα Matching", to_excel_bytes(result_df), file_name="ftth_matching_results.xlsx")
    with c3:
        st.download_button("⬇️ Όλα τα δεδομένα (merged)", to_excel_bytes(merged), file_name="merged_with_geocoded.xlsx")

elif start and (biz_df is None or ftth_df is None):
    st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
else:
    st.info("📄 Ανέβασε FTTH, επίλεξε πηγή επιχειρήσεων (Upload ή ΓΕΜΗ), και πάτα «🚀 Ξεκίνα».")