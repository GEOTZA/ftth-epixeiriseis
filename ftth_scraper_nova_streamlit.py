
import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io
import time
import math

try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

st.set_page_config(page_title="FTTH Geocoding & Matching (v3 fast)", layout="wide")
st.title("⚡ FTTH Geocoding & Matching (γρήγορο & ανθεκτικό)")

st.markdown("""
- Υποστηρίζει **Excel/CSV FTTH** (latitude, longitude)
- Κάνει **geocoding διευθύνσεων** (Ελληνικά & Greeklish)
- **Caching** για να μην ξαναζητάει τις ίδιες διευθύνσεις
- **Αποφυγή διπλότυπων**: γεωκωδικοποιεί κάθε μοναδική διεύθυνση μία φορά
- Επιλογή **Nominatim (δωρεάν)** ή **Google Geocoding (με API key)**
""")

# -----------------------------
# Sidebar options
# -----------------------------
with st.sidebar:
    st.header("Ρυθμίσεις")
    geocoder = st.selectbox("Geocoder", ["Nominatim (δωρεάν)", "Google (API key)"])
    google_key = st.text_input("Google API key", type="password", help="Αν αφήσεις κενό, θα χρησιμοποιηθεί μόνο Nominatim.")
    country = st.text_input("Country code", "gr")
    lang = st.text_input("Language (IETF tag)", "el")
    throttle = st.slider("Καθυστέρηση ανά αίτημα (sec) [Nominatim μόνο]", 0.5, 2.0, 1.0, 0.5)
    distance_limit = st.number_input("📏 Μέγιστη απόσταση (m)", min_value=1, max_value=1000, value=50, step=1)

# -----------------------------
# File uploads
# -----------------------------
st.subheader("📥 Αρχεία εισόδου")
biz_file = st.file_uploader("Excel Επιχειρήσεων (στήλες: name, address, city **ή** name, site.company_insights.address, site.company_insights.city)", type=["xlsx", "csv"])
ftth_file = st.file_uploader("FTTH σημεία (CSV ή Excel με στήλες: latitude, longitude)", type=["csv", "xlsx"])

# Optional resume: upload previously geocoded addresses
prev_geo_file = st.file_uploader("🧠 (Προαιρετικό) Προηγούμενα geocoded αποτελέσματα για resume (xlsx/csv με στήλες: Address, Latitude, Longitude)", type=["xlsx", "csv"])

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

# -----------------------------
# Normalize business input
# -----------------------------
def normalize_biz_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    # Try outscraper style first
    if "site.company_insights.address" in df.columns and "site.company_insights.city" in df.columns and "name" in df.columns:
        df = df.rename(columns={
            "site.company_insights.address": "address",
            "site.company_insights.city": "city"
        })
        return df
    # Try lowercase variants
    elif "address" in cols and "city" in cols and "name" in cols:
        # nothing
        return df.rename(columns={cols["address"]: "address", cols["city"]: "city", cols["name"]: "name"})
    else:
        raise ValueError("Το αρχείο επιχειρήσεων πρέπει να έχει στήλες: name, address, city (ή name, site.company_insights.address, site.company_insights.city).")

def normalize_ftth(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    if "latitude" not in cols or "longitude" not in cols:
        raise ValueError("Το αρχείο FTTH πρέπει να έχει στήλες: latitude, longitude.")
    return df.rename(columns={cols["latitude"]: "latitude", cols["longitude"]: "longitude"})[["latitude","longitude"]].dropna()

def maybe_load_prev(prev: pd.DataFrame):
    if prev is None: 
        return None
    cols = {c.lower(): c for c in prev.columns}
    needed = {"address","latitude","longitude"}
    if not needed.issubset(set(cols.keys())):
        return None
    return prev.rename(columns={cols["address"]: "Address", cols["latitude"]: "Latitude", cols["longitude"]: "Longitude"})[["Address","Latitude","Longitude"]]

if biz_df is not None:
    try:
        biz_df = normalize_biz_columns(biz_df)
    except Exception as e:
        st.error(str(e))
        st.stop()

if ftth_df is not None:
    try:
        ftth_df = normalize_ftth(ftth_df)
    except Exception as e:
        st.error(str(e))
        st.stop()

prev_df = maybe_load_prev(prev_df)

# -----------------------------
# Geocoding helpers
# -----------------------------
if CACHE_OK:
    requests_cache.install_cache("geocode_cache", backend="sqlite", expire_after=60*60*24*14)  # 14 days

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

def geocode_google(address, api_key):
    params = {"address": address, "key": api_key, "language": lang}
    r = session.get("https://maps.googleapis.com/maps/api/geocode/json", params=params, timeout=15)
    r.raise_for_status()
    js = r.json()
    if js.get("status") == "OK" and js.get("results"):
        loc = js["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"])
    return None, None

def geocode_address(address, provider, api_key=None, cc="gr", lang="el", throttle_sec=1.0):
    # Try exact
    lat, lon = (None, None)
    if provider.startswith("Google") and api_key:
        lat, lon = geocode_google(address, api_key)
    else:
        lat, lon = geocode_nominatim(address, cc, lang)
        if not getattr(session, "cache_disabled", True):
            time.sleep(throttle_sec)  # be polite only for live calls (requests-cache skips network)
    # Fallback: add ", Greece" if not there
    if lat is None and "greece" not in address.lower() and "ελλάδα" not in address.lower():
        fallback = f"{address}, Greece"
        if provider.startswith("Google") and api_key:
            lat, lon = geocode_google(fallback, api_key)
        else:
            lat, lon = geocode_nominatim(fallback, cc, lang)
            if not getattr(session, "cache_disabled", True):
                time.sleep(throttle_sec)
    return lat, lon

# -----------------------------
# Main run
# -----------------------------
if biz_df is not None and ftth_df is not None:
    st.subheader("🔄 Geocoding διευθύνσεων")
    # Φτιάξε πλήρη διεύθυνση
    work = biz_df.copy()
    work["Address"] = (work["address"].astype(str).str.strip() + ", " + work["city"].astype(str).str.strip()).str.replace(r"\s+", " ", regex=True)

    # Remove rows with empty Address
    work = work[work["Address"].str.len() > 3]

    # Deduplicate
    unique_addresses = sorted(work["Address"].dropna().unique().tolist())

    # Start from previous results if given
    geo_map = {}
    if prev_df is not None:
        for _, r in prev_df.iterrows():
            geo_map[r["Address"]] = (r["Latitude"], r["Longitude"])

    total = len(unique_addresses)
    progress = st.progress(0, text=f"0 / {total}")
    results = []
    errs = 0

    for i, addr in enumerate(unique_addresses, start=1):
        if addr in geo_map:
            lat, lon = geo_map[addr]
        else:
            lat, lon = geocode_address(addr, geocoder, api_key=google_key, cc=country, lang=lang, throttle_sec=throttle)
            if lat is not None and lon is not None:
                geo_map[addr] = (lat, lon)
            else:
                errs += 1
        progress.progress(i/total, text=f"{i} / {total} γεωκωδικοποιημένα...")

    # Build geocoded table
    geo_rows = [{"Address": a, "Latitude": v[0], "Longitude": v[1]} for a, v in geo_map.items() if v[0] is not None]
    geocoded_df = pd.DataFrame(geo_rows)

    st.write("🧠 Cache ενεργή:", CACHE_OK)
    st.write("📦 Γεωκωδικοποιημένες μοναδικές διευθύνσεις:", len(geocoded_df), "/", total, "(αποτυχίες:", errs, ")")

    # Join back to businesses
    merged = work.merge(geocoded_df, on="Address", how="left")

    # Matching με FTTH
    st.subheader("📡 Matching με FTTH")
    ftth_points = ftth_df[["latitude","longitude"]].dropna().to_numpy()

    matches = []
    for _, row in merged.dropna(subset=["Latitude","Longitude"]).iterrows():
        biz_coords = (row["Latitude"], row["Longitude"])
        # Fast coarse check: break early at first match within threshold
        for ft_lat, ft_lon in ftth_points:
            d = geodesic(biz_coords, (ft_lat, ft_lon)).meters
            if d <= distance_limit:
                matches.append({
                    "name": row.get("name", ""),
                    "Address": row["Address"],
                    "Latitude": row["Latitude"],
                    "Longitude": row["Longitude"],
                    "FTTH_lat": ft_lat,
                    "FTTH_lon": ft_lon,
                    "Distance(m)": round(d, 2)
                })
                break

    result_df = pd.DataFrame(matches)
    st.success(f"✅ Βρέθηκαν {len(result_df)} επιχειρήσεις εντός {distance_limit} m από FTTH.")
    st.dataframe(result_df, use_container_width=True)

    # Downloads
    col1, col2 = st.columns(2)
    with col1:
        out_geo = io.BytesIO()
        geocoded_df.to_excel(out_geo, index=False)
        out_geo.seek(0)
        st.download_button("⬇️ Κατέβασε τις γεωκωδικοποιημένες διευθύνσεις", out_geo, file_name="geocoded_addresses.xlsx")

    with col2:
        out_res = io.BytesIO()
        result_df.to_excel(out_res, index=False)
        out_res.seek(0)
        st.download_button("⬇️ Κατέβασε τα αποτελέσματα Matching", out_res, file_name="ftth_matching_results.xlsx")
else:
    st.info("📄 Ανέβασε αρχεία Επιχειρήσεων & FTTH για να ξεκινήσεις.")
