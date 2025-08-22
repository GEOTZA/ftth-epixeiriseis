# ftth_scraper_nova_streamlit.py
# -*- coding: utf-8 -*-

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

st.set_page_config(page_title="FTTH Geocoding & Matching (v6)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v6")

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

    # --- Sidebar: API (ΓΕΜΗ) Ρυθμίσεις (πάντα ορατό) ---
    with st.expander("🔌 API (ΓΕΜΗ) Ρυθμίσεις", expanded=(biz_source == "ΓΕΜΗ (OpenData API)")):
        # Σύμφωνα με Swagger:
        # Base URL: https://opendata-api.businessportal.gr/api/opendata/v1
        default_base   = "https://opendata-api.businessportal.gr/api/opendata/v1"
        default_header = "api_key"

        gemi_base  = st.text_input("Base URL", value=st.session_state.get("gemi_base", default_base))
        gemi_hdr   = st.text_input("Header name", value=st.session_state.get("gemi_header", default_header))
        gemi_key   = st.text_input("GEMH API Key", type="password",
                                   value=st.session_state.get("gemi_key", ""))

        # save σε session_state
        st.session_state.update(gemi_base=gemi_base, gemi_header=gemi_hdr, gemi_key=gemi_key)

        # Μικρό διαγνωστικό
        if st.button("🧪 Test API (παραμετρικά)"):
            try:
                test_urls = [
                    f"{gemi_base.rstrip('/')}/params/regions",
                    f"{gemi_base.rstrip('/')}/params/perifereies",
                    f"{gemi_base.rstrip('/')}/params/peripheries",
                ]
                tried = []
                for u in test_urls:
                    r = requests.get(u, headers={gemi_hdr: gemi_key} if gemi_key else {}, timeout=15)
                    tried.append(u)
                    r.raise_for_status()
                st.success("OK: params endpoints απάντησαν.")
                st.code("\n".join(tried), language="text")
            except Exception as e:
                st.error(f"Σφάλμα params: {e}")

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

def _first_non_empty(d: dict, keys: list[str], default=""):
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return default

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

# ---------- Biz source (file) ----------
biz_df = None
if biz_source == "Upload Excel/CSV":
    biz_df = load_table(biz_file) if biz_file else None

# ---------- GEMI (OpenData API) ----------
# ΣΩΣΤΗ ΒΑΣΗ API (Swagger base): https://opendata-api.businessportal.gr/api/opendata/v1
GEMI_FALLBACK_BASES = [
    "https://opendata-api.businessportal.gr/api/opendata/v1",  # σωστό
    "https://opendata-api.businessportal.gr/opendata",          # legacy
    "https://opendata-api.businessportal.gr",                   # πολύ παλιό
]

TIMEOUT = 40

def _gemi_headers():
    hdr = st.session_state.get("gemi_header", "api_key")
    key = st.session_state.get("gemi_key", "")
    h = {"Accept": "application/json"}
    if key:
        h[hdr] = key
    return h

def _bases():
    first = st.session_state.get("gemi_base", "").strip()
    bases = []
    if first:
        bases.append(first.replace("οpendata", "opendata"))
    for b in GEMI_FALLBACK_BASES:
        if b not in bases:
            bases.append(b)
    return bases

def gemi_params(what: str, *, region_id=None):
    """
    Παίρνει παραμετρικά (regions / regional_units / dimoi / statuses / kad) από /params/*
    """
    endpoints = []
    if what == "regions":
        endpoints = ["params/regions", "params/perifereies", "params/peripheries"]
    elif what in ("regional_units", "perifereiakes_enotites"):
        if region_id is not None:
            endpoints = [
                f"params/regional_units/{region_id}",
                f"params/perifereiakes_enotites/{region_id}",
                f"params/periferiakes_enotites/{region_id}",
                f"params/prefectures/{region_id}",
            ]
        else:
            endpoints = ["params/regional_units", "params/perifereiakes_enotites", "params/periferiakes_enotites", "params/prefectures"]
    elif what in ("dimoi", "municipalities"):
        if region_id is not None:
            endpoints = [f"params/dimoi/{region_id}", f"params/municipalities/{region_id}"]
        else:
            endpoints = ["params/dimoi", "params/municipalities"]
    elif what in ("statuses",):
        endpoints = ["params/statuses", "params/status", "params/company_statuses"]
    elif what in ("kad", "kads"):
        endpoints = ["params/kad", "params/kads", "params/activity_codes", "params/kad_codes", "params/nace"]
    else:
        endpoints = [f"params/{what}"]

    last_err = None
    for base in _bases():
        for ep in endpoints:
            url = f"{base.rstrip('/')}/{ep.lstrip('/')}"
            try:
                r = requests.get(url, headers=_gemi_headers(), timeout=TIMEOUT)
                if r.status_code == 200:
                    return r.json()
                last_err = f"{r.status_code} on {url}"
            except Exception as e:
                last_err = str(e)
                continue
    raise RuntimeError(f"ΓΕΜΗ: δεν βρέθηκε endpoint για '{what}'. Τελευταίο: {last_err}")

def gemi_companies_search(*, page=1, per_page=100,
                          name_part=None,
                          region_id=None, regional_unit_id=None, municipality_id=None,
                          status_id=None, kad_list=None,
                          date_from=None, date_to=None):
    """
    Αναζήτηση εταιρειών με GET /companies (base: /api/opendata/v1).
    """
    variants = [
        {
            "page": page, "per_page": per_page,
            "name": name_part, "name_part": name_part,
            "region_id": region_id, "regional_unit_id": regional_unit_id, "municipality_id": municipality_id,
            "perifereia_id": region_id, "perifereiaki_enotita_id": regional_unit_id, "dimos_id": municipality_id,
            "status_id": status_id,
            "kad": ",".join(kad_list) if kad_list else None,
            "incorporation_date_from": date_from, "incorporation_date_to": date_to,
            "foundation_date_from": date_from, "foundation_date_to": date_to,
            "registration_date_from": date_from, "registration_date_to": date_to,
        },
        {
            "page": page, "page_size": per_page,
            "name": name_part, "name_part": name_part,
            "regionId": region_id, "regionalUnitId": regional_unit_id, "municipalityId": municipality_id,
            "nomosId": regional_unit_id, "dimosId": municipality_id,
            "statusId": status_id,
            "kad": ",".join(kad_list) if kad_list else None,
            "incorporationDateFrom": date_from, "incorporationDateTo": date_to,
            "foundationDateFrom": date_from, "foundationDateTo": date_to,
            "registrationDateFrom": date_from, "registrationDateTo": date_to,
        },
    ]

    last_err = None
    for base in _bases():
        url = f"{base.rstrip('/')}/companies"
        for params in variants:
            q = {k: v for k, v in params.items() if v not in (None, "", [], {})}
            try:
                r = requests.get(url, params=q, headers=_gemi_headers(), timeout=TIMEOUT)
                if r.status_code == 200:
                    return r.json()
                last_err = f"{r.status_code} on {url} keys={list(q.keys())}"
            except requests.RequestException as e:
                last_err = str(e)
                continue
    raise RuntimeError(f"ΓΕΜΗ: αναζήτηση απέτυχε. Τελευταίο σφάλμα: {last_err}")

def gemi_companies_all(**kwargs):
    per_page = kwargs.pop("per_page", 200)
    max_pages = kwargs.pop("max_pages", 100)
    sleep_sec = kwargs.pop("sleep_sec", 0.2)

    items = []
    for p in range(1, max_pages +
                   }
                   ]
                
