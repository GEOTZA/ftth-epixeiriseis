
import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io
import time

# ================= Setup =================
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

st.set_page_config(page_title="FTTH Geocoding & Matching (v4.1)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v4.1 (Ηράκλειο & κοντινά matches)")

with st.sidebar:
    st.header("Ρυθμίσεις")
    geocoder = st.selectbox("Geocoder", ["Nominatim (δωρεάν)", "Google (API key)"])
    google_key = st.text_input("Google API key", type="password", help="Αν το αφήσεις κενό, θα χρησιμοποιηθεί Nominatim.")
    country = st.text_input("Country code", "gr")
    lang = st.text_input("Language", "el")
    throttle = st.slider("Καθυστέρηση (sec) [Nominatim]", 0.5, 2.0, 1.0, 0.5)
    # ΝΕΟ: slider 1–20m (default 5m)
    distance_limit = st.slider("📏 Απόσταση (m) για κοντινά matches", min_value=1, max_value=20, value=5, step=1)
    city_filter = st.text_input("🏙 Πόλη", "Ηράκλειο Κρήτης")

st.subheader("📥 Αρχεία")
biz_file = st.file_uploader("Excel/CSV Επιχειρήσεων (στήλες: name, address, city ή Outscraper: name, site.company_insights.address, site.company_insights.city)", type=["xlsx", "csv"])
ftth_file = st.file_uploader("FTTH σημεία (Excel/CSV με στήλες: latitude, longitude)", type=["xlsx", "csv"])
prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά) – Excel/CSV με στήλες: Address, Latitude, Longitude", type=["xlsx", "csv"])

def load_table(uploaded):
    if uploaded is None: 
        return None
    name = uploaded.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded)
    return pd.read_excel(uploaded)

biz_df = load_table(biz_file) if biz_file else None
ftth_df = load_table(ftth_file) if ftth_file else None
prev_df = load_table(prev_geo_file) if prev_geo_file else None

# ================= Helpers =================
def pick_first_series(df: pd.DataFrame, candidates):
    for cand in candidates:
        exact = [c for c in df.columns if c.lower() == cand.lower()]
        if exact:
            col = df[exact]
            return col.iloc[:, 0] if isinstance(col, pd.DataFrame) else col
        loose = df.filter(regex=fr"(?i)^{cand}$")
        if loose.shape[1] > 0:
            return loose.iloc[:, 0]
    return pd.Series([""] * len(df), index=df.index, dtype="object")

def normalize_ftth(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    if "latitude" not in cols or "longitude" not in cols:
        raise ValueError("Το αρχείο FTTH πρέπει να έχει στήλες: latitude, longitude.")
    df = df.rename(columns={cols["latitude"]: "latitude", cols["longitude"]: "longitude"})[["latitude","longitude"]]
    # 🔧 Διόρθωση κόμματος/τελείας & μετατροπή σε float
    df["latitude"]  = pd.to_numeric(df["latitude"].astype(str).str.replace(",", "."), errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"].astype(str).str.replace(",", "."), errors="coerce")
    return df.dropna()

# ================= FTTH normalize =================
if ftth_df is not None:
    try:
        ftth_df = normalize_ftth(ftth_df)
    except Exception as e:
        st.error(str(e))
        st.stop()

# ================= Caching & session =================
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

def _norm(s: str) -> str:
    return (s or "").lower().replace(" κρητης", "").replace(" κρήτης", "").strip()

start = st.button("🚀 Ξεκίνα geocoding & matching")

if start and biz_df is not None and ftth_df is not None:
    work = biz_df.copy()
    addr_series = pick_first_series(work, ["address", "site.company_insights.address", "διεύθυνση"])
    city_series = pick_first_series(work, ["city", "site.company_insights.city", "πόλη"])

    base_addr = addr_series.astype(str).str.strip()
    fallback_city = city_filter.strip()
    from_input_city = city_series.astype(str).str.strip().replace("", fallback_city)
    work["Address"] = (base_addr + ", " + from_input_city).str.replace(r"\s+", " ", regex=True)

    work = work[work["Address"].str.len() > 3].copy()
    unique_addresses = sorted(work["Address"].dropna().unique().tolist())

    # Resume από προηγούμενο
    geo_map = {}
    if prev_geo_file is not None and prev_df is not None and {"Address","Latitude","Longitude"}.issubset(prev_df.columns):
        for _, r in prev_df.iterrows():
            geo_map[r["Address"]] = (r["Latitude"], r["Longitude"])

    total = len(unique_addresses)
    progress = st.progress(0, text=f"0 / {total}")
    errs = 0

    for i, addr in enumerate(unique_addresses, start=1):
        if addr in geo_map:
            lat, lon = geo_map[addr]
        else:
            query = f"{addr}, {city_filter}" if city_filter and city_filter.lower() not in addr.lower() else addr
            lat, lon = geocode_address(query, geocoder, api_key=google_key, cc=country, lang=lang, throttle_sec=throttle)
            if lat is not None and lon is not None:
                geo_map[addr] = (lat, lon)
            else:
                errs += 1
        progress.progress(i/total, text=f"{i} / {total} γεωκωδικοποιημένα...")

    geocoded_df = pd.DataFrame([{"Address": a, "Latitude": v[0], "Longitude": v[1]} for a, v in geo_map.items() if v[0] is not None])

    # Join back
    merged = work.merge(geocoded_df, on="Address", how="left")

    # 🔧 Κανονικοποίηση δεκαδικών και μετατροπή σε float στις γεωκωδικοποιημένες
    if "Latitude" in merged.columns and "Longitude" in merged.columns:
        merged["Latitude"]  = pd.to_numeric(merged["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
        merged["Longitude"] = pd.to_numeric(merged["Longitude"].astype(str).str.replace(",", "."), errors="coerce")

    # Προαιρετικό & ανεκτικό φιλτράρισμα πόλης (δεν πετάμε αν δεν περιλαμβάνει ακριβώς)
    if city_filter.strip():
        cf = _norm(city_filter)
        merged = merged[merged["Address"].fillna("").apply(lambda x: cf in _norm(x))]

    # ============== Matching μόνο κοντινά (<= slider) ==============
    ftth_points = ftth_df[["latitude","longitude"]].dropna().to_numpy()
    matches = []
    exact_count = 0

    for _, row in merged.dropna(subset=["Latitude","Longitude"]).iterrows():
        biz_coords = (row["Latitude"], row["Longitude"])
        best_d = None
        best_ft = None
        # βρες το κοντινότερο FTTH
        for ft_lat, ft_lon in ftth_points:
            d = geodesic(biz_coords, (ft_lat, ft_lon)).meters
            if best_d is None or d < best_d:
                best_d = d
                best_ft = (ft_lat, ft_lon)
        # κράτα μόνο αν είναι εντός του slider
        if best_d is not None and best_d <= distance_limit:
            if round(row["Latitude"], 5) == round(best_ft[0], 5) and round(row["Longitude"], 5) == round(best_ft[1], 5):
                exact = True
                exact_count += 1
            else:
                exact = False
            matches.append({
                "name": row.get("name", ""),
                "Address": row["Address"],
                "Latitude": row["Latitude"],
                "Longitude": row["Longitude"],
                "FTTH_lat": best_ft[0],
                "FTTH_lon": best_ft[1],
                "Distance(m)": round(best_d, 2),
                "Exact(5dp)": exact
            })

    result_df = pd.DataFrame(matches).sort_values(["Exact(5dp)", "Distance(m)"], ascending=[False, True]).reset_index(drop=True)
    st.success(f"✅ Κοντινά matches (<= {distance_limit} m): {len(result_df)} • 🎯 Exact (5dp): {exact_count}")
    st.dataframe(result_df, use_container_width=True)

    # -------- Excel export --------
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

    col1, col2 = st.columns(2)
    with col1:
        st.download_button("⬇️ Κοντινά matches (Excel)", to_excel_bytes(result_df), file_name=f"ftth_matches_{distance_limit}m.xlsx")
    with col2:
        st.download_button("⬇️ Geocoded διευθύνσεις", to_excel_bytes(geocoded_df), file_name="geocoded_addresses.xlsx")

elif start and (biz_df is None or ftth_df is None):
    st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
else:
    st.info("📄 Ανέβασε αρχεία, συμπλήρωσε πόλη (προεπιλογή: Ηράκλειο Κρήτης) και πάτα «🚀 Ξεκίνα».")
