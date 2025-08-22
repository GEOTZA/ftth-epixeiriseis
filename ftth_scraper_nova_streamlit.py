# ftth_scraper_nova_streamlit.py
import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io
import time
import re

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

# ---------- GEMI (OpenData API) ----------
GEMI_BASE = "https://opendata-api.businessportal.gr/opendata"
GEMI_HEADER_NAME = "api_key"

def _gemi_headers(api_key: str):
    return {GEMI_HEADER_NAME: api_key, "Accept": "application/json"}

def gemi_params(api_key, what, *, nomos_id=None):
    """
    Φόρτωση παραμετρικών από ΓΕΜΗ με fallbacks σε εναλλακτικά slugs.
    """
    candidates = [f"{GEMI_BASE}/params/{what}"]
    if what == "nomoi":
        candidates += [
            f"{GEMI_BASE}/params/prefectures",
            f"{GEMI_BASE}/params/regional_units",
            f"{GEMI_BASE}/params/perifereiakes_enotites",
        ]
    if what == "dimoi":
        if nomos_id is not None:
            candidates += [
                f"{GEMI_BASE}/params/dimoi/{nomos_id}",
                f"{GEMI_BASE}/params/municipalities/{nomos_id}",
                f"{GEMI_BASE}/params/dimoi?nomosId={nomos_id}",
                f"{GEMI_BASE}/params/municipalities?prefectureId={nomos_id}",
            ]
        else:
            candidates += [
                f"{GEMI_BASE}/params/dimoi",
                f"{GEMI_BASE}/params/municipalities",
            ]
    if what == "statuses":
        candidates += [
            f"{GEMI_BASE}/params/status",
            f"{GEMI_BASE}/params/company_statuses",
        ]
    if what in ("kad", "kads"):
        candidates += [
            f"{GEMI_BASE}/params/kad",
            f"{GEMI_BASE}/params/kads",
            f"{GEMI_BASE}/params/activity_codes",
            f"{GEMI_BASE}/params/kad_codes",
            f"{GEMI_BASE}/params/nace",
        ]

    last_err = None
    for url in candidates:
        try:
            r = requests.get(url, headers=_gemi_headers(api_key), timeout=30)
            if r.status_code == 404:
                last_err = f"404 on {url}"
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last_err = str(e)
            continue
    raise RuntimeError(f"ΓΕΜΗ: δεν βρέθηκε endpoint για '{what}'. Τελευταίο σφάλμα: {last_err}")

def gemi_search(api_key, *, nomos_id=None, dimos_id=None, status_id=None,
                name_part=None, kad_list=None, date_from=None, date_to=None,
                page=1, page_size=200):
    """
    Αναζήτηση εταιρει
