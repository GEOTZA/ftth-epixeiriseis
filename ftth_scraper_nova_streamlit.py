import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io
import time

# Προσπάθεια φόρτωσης cache
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

st.set_page_config(page_title="FTTH Geocoding & Matching (v4)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v4")

# Sidebar Ρυθμίσεις
with st.sidebar:
    st.header("Ρυθμίσεις")
    geocoder = st.selectbox("Geocoder", ["Nominatim (δωρεάν)", "Google (API key)"])
    google_key = st.text_input("Google API key", type="password", help="Αν το αφήσεις κενό, θα χρησιμοποιηθεί Nominatim.")
    country = st.text_input("Country code", "gr")
    lang = st.text_input("Language", "el")
    throttle = st.slider("Καθυστέρηση (sec) [Nominatim]", 0.5, 2.0, 1.0, 0.5)
    distance_limit = st.number_input("📏 Μέγιστη απόσταση (m)", min_value=1, max_value=1000, value=50)
    city_filter = st.text_input("🏙 Πόλη", "Ηράκλειο Κρήτης")

# Upload αρχεία
st.subheader("📥 Αρχεία")
biz_file = st.file_uploader("Excel/CSV Επιχειρήσεων", type=["xlsx", "csv"])
ftth_file = st.file_uploader("FTTH σημεία (Excel/CSV με στήλες: latitude, longitude)", type=["xlsx", "csv"])
prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά)", type=["xlsx", "csv"])

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

def pick_first_series(df: pd.DataFrame, candidates):
    """Επιστρέφει μία Series από την πρώτη ταιριαστή στήλη (αν υπάρχουν διπλές, παίρνει την 1η)."""
    for cand in candidates:
        # ακριβές ταίριασμα
        exact = [c for c in df.columns if c.lower() == cand.lower()]
        if exact:
            col = df[exact]
            return col.iloc[:, 0] if isinstance(col, pd.DataFrame) else col
        # loose (regex)
        loose = df.filter(regex=fr"(?i)^{cand}$")
        if loose.shape[1] > 0:
            return loose.iloc[:, 0]
    return pd.Series([""] * len(df), index=df.index, dtype="object")

def normalize_ftth(df: pd.DataFrame) -> pd.DataFrame:
    """Rename -> latitude/longitude, dropna, ΚΑΙ διόρθωση κόμμα/τελεία -> float."""
    cols = {c.lower(): c for c in df.columns}
    if "latitude" not in cols or "longitude" not in cols:
        raise ValueError("Το αρχείο FTTH πρέπει να έχει στήλες: latitude, longitude.")
    out = df.rename(columns={cols["latitude"]: "latitude", cols["longitude"]: "longitude"})[["latitude", "longitude"]].dropna()
    # κόμμα -> τελεία και σε float
    out["latitude"] = pd.to_numeric(out["latitude"].astype(str).str.replace(",", "."), errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"].astype(str).str.replace(",", "."), errors="coerce")
    out = out.dropna(subset=["latitude", "longitude"])
    return out

if ftth_df is not None:
    try:
        ftth_df = normalize_ftth(ftth_df)
    except Exception as e:
        st.error(str(e))
        st.stop()

# Cache για geocoding
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
        # ευγένεια στον Nominatim μόνο σε πραγματικά network calls
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

# Start button
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

    # Resume από προηγούμενο geocoded
    geo_map = {}
    if prev_df is not None and {"Address","Latitude","Longitude"}.issubset(prev_df.columns):
        # Κόμμα/τελεία fix & coercion
        prev_df = prev_df.rename(columns={"Latitude":"Latitude","Longitude":"Longitude","Address":"Address"})
        prev_df["Latitude"]  = pd.to_numeric(prev_df["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
        prev_df["Longitude"] = pd.to_numeric(prev_df["Longitude"].astype(str).str.replace(",", "."), errors="coerce")
        prev_df = prev_df.dropna(subset=["Latitude","Longitude"])
        for _, r in prev_df.iterrows():
            geo_map[str(r["Address"])] = (float(r["Latitude"]), float(r["Longitude"]))

    total = len(unique_addresses)
    progress = st.progress(0, text=f"0 / {total}")
    errs = 0

    for i, addr in enumerate(unique_addresses, start=1):
        if addr in geo_map:
            lat, lon = geo_map[addr]
        else:
            # προσθέτουμε και την πόλη του sidebar για σταθερότητα
            query = f"{addr}, {city_filter}" if city_filter and city_filter.lower() not in addr.lower() else addr
            lat, lon = geocode_address(query, geocoder, api_key=google_key, cc=country, lang=lang, throttle_sec=throttle)
            if lat is not None and lon is not None:
                geo_map[addr] = (lat, lon)
            else:
                errs += 1
        progress.progress(i/total, text=f"{i} / {total} γεωκωδικοποιημένα...")

    geocoded_df = pd.DataFrame([{"Address": a, "Latitude": v[0], "Longitude": v[1]} for a, v in geo_map.items() if v[0] is not None])

    # Κόμμα/τελεία fix στα geocoded πριν το merge (ασφάλεια)
    if not geocoded_df.empty:
        geocoded_df["Latitude"]  = pd.to_numeric(geocoded_df["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
        geocoded_df["Longitude"] = pd.to_numeric(geocoded_df["Longitude"].astype(str).str.replace(",", "."), errors="coerce")
        geocoded_df = geocoded_df.dropna(subset=["Latitude","Longitude"])

    # Join back
    merged = work.merge(geocoded_df, on="Address", how="left")

    # Φιλτράρισμα στην πόλη του sidebar
    merged = merged[merged["Address"].str.contains(city_filter, case=False, na=False)]

    # Matching
    ftth_points = ftth_df[["latitude","longitude"]].dropna().to_numpy()
    matches = []
    for _, row in merged.dropna(subset=["Latitude","Longitude"]).iterrows():
        # κόμμα/τελεία fix & coercion επιπλέον ασφάλεια
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

    # Safe sort (αν υπάρχουν οι στήλες)
    if not result_df.empty and all(c in result_df.columns for c in ["Distance(m)"]):
        result_df = result_df.sort_values(["Distance(m)"], ascending=[True])
    result_df = result_df.reset_index(drop=True)

    if result_df.empty:
        st.warning(f"⚠️ Δεν βρέθηκαν επιχειρήσεις στην πόλη '{city_filter}' εντός {distance_limit} m από FTTH.")
    else:
        st.success(f"✅ Βρέθηκαν {len(result_df)} επιχειρήσεις στην πόλη '{city_filter}' εντός {distance_limit} m από FTTH.")
        st.dataframe(result_df, use_container_width=True)

    # -------- Robust Excel export --------
    def to_excel_bytes(df: pd.DataFrame):
        safe = df.copy()
        if safe is None or safe.empty:
            safe = pd.DataFrame([{"info": "no data"}])
        # ονόματα στηλών ως string
        safe.columns = [str(c) for c in safe.columns]
        # διασφάλιση scalar τιμών
        for c in safe.columns:
            safe[c] = safe[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            safe.to_excel(writer, index=False, sheet_name="Sheet1")
        output.seek(0)
        return output
    # -------------------------------------

    col1, col2, col3 = st.columns(3)
    with col1:
        st.download_button("⬇️ Geocoded διευθύνσεις", to_excel_bytes(geocoded_df), file_name="geocoded_addresses.xlsx")
    with col2:
        st.download_button("⬇️ Αποτελέσματα Matching", to_excel_bytes(result_df), file_name="ftth_matching_results.xlsx")
    with col3:
        st.download_button("⬇️ Όλα τα δεδομένα (merged)", to_excel_bytes(merged), file_name="merged_with_geocoded.xlsx")

elif start and (biz_df is None or ftth_df is None):
    st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
else:
    st.info("📄 Ανέβασε αρχεία, συμπλήρωσε πόλη και πάτα «🚀 Ξεκίνα».")
