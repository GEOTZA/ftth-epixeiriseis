# ftth_scraper_nova_streamlit.py
# -*- coding: utf-8 -*-

import io
import re
import time
import pandas as pd
import requests
import streamlit as st
from geopy.distance import geodesic

# ---------- Optional cache ----------
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

# ---------- Streamlit page ----------
st.set_page_config(page_title="FTTH Geocoding & Matching (v5)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v5")

# ---------- Safe defaults for GEMI ----------
DEFAULT_GEMI_BASE = "https://publicity.businessportal.gr"  # δουλεύει χωρίς api_key
DEFAULT_HEADER_NAME = "api_key"

if "GEMI_BASE_URL" not in st.session_state:
    st.session_state["GEMI_BASE_URL"] = DEFAULT_GEMI_BASE
if "GEMI_HEADER_NAME" not in st.session_state:
    st.session_state["GEMI_HEADER_NAME"] = DEFAULT_HEADER_NAME
if "GEMI_API_KEY" not in st.session_state:
    st.session_state["GEMI_API_KEY"] = ""
if "GEMI_SLUGS_REGIONS" not in st.session_state:
    st.session_state["GEMI_SLUGS_REGIONS"] = "regions,perifereies,peripheries"
if "GEMI_SLUGS_REGUNITS" not in st.session_state:
    st.session_state["GEMI_SLUGS_REGUNITS"] = "regional_units,perifereiakes_enotites,periferiakes_enotites,prefectures,nomoi"
if "GEMI_SLUGS_DIMOI" not in st.session_state:
    st.session_state["GEMI_SLUGS_DIMOI"] = "municipalities,dimoi,dhmoi,municipal_units"
if "GEMI_SLUGS_STATUS" not in st.session_state:
    st.session_state["GEMI_SLUGS_STATUS"] = "statuses,status,company_statuses"
if "GEMI_SLUGS_KAD" not in st.session_state:
    st.session_state["GEMI_SLUGS_KAD"] = "kad,kads,activity_codes,kad_codes,nace"
if "GEMI_SEARCH_PATHS" not in st.session_state:
    st.session_state["GEMI_SEARCH_PATHS"] = "api/companies/search,companies/search,search"

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
    gemi_key_input = st.text_input("GΕΜΗ API Key", type="password") if biz_source == "ΓΕΜΗ (OpenData API)" else None
    if gemi_key_input is not None:
        st.session_state["GEMI_API_KEY"] = gemi_key_input or ""

    with st.expander("API (ΓΕΜΗ) Ρυθμίσεις", expanded=(biz_source == "ΓΕΜΗ (OpenData API)")):
        st.caption("Αν τα παραμετρικά 404, το app γυρίζει σε fallback (χωρίς IDs). Για μεγαλύτερη συμβατότητα μπορείς να χρησιμοποιήσεις το publicity.")
        base_url = st.text_input("Base URL", value=st.session_state["GEMI_BASE_URL"])
        header_name = st.text_input("Header name (άστο κενό για publicity)", value=st.session_state["GEMI_HEADER_NAME"])
        search_paths = st.text_input("Paths: Search", value=st.session_state["GEMI_SEARCH_PATHS"],
                                     help="Δοκιμάζονται με τη σειρά (π.χ. api/companies/search,companies/search,search)")

        regions_slugs = st.text_input("Slugs: Περιφέρειες", value=st.session_state["GEMI_SLUGS_REGIONS"])
        regunits_slugs = st.text_input("Slugs: Περιφερειακές Ενότητες", value=st.session_state["GEMI_SLUGS_REGUNITS"])
        dimoi_slugs = st.text_input("Slugs: Δήμοι", value=st.session_state["GEMI_SLUGS_DIMOI"])
        status_slugs = st.text_input("Slugs: Καταστάσεις", value=st.session_state["GEMI_SLUGS_STATUS"])
        kad_slugs = st.text_input("Slugs: ΚΑΔ", value=st.session_state["GEMI_SLUGS_KAD"])

        # Save
        st.session_state["GEMI_BASE_URL"] = base_url.strip().rstrip("/")
        st.session_state["GEMI_HEADER_NAME"] = header_name.strip()
        st.session_state["GEMI_SEARCH_PATHS"] = search_paths.strip()
        st.session_state["GEMI_SLUGS_REGIONS"] = regions_slugs.strip()
        st.session_state["GEMI_SLUGS_REGUNITS"] = regunits_slugs.strip()
        st.session_state["GEMI_SLUGS_DIMOI"] = dimoi_slugs.strip()
        st.session_state["GEMI_SLUGS_STATUS"] = status_slugs.strip()
        st.session_state["GEMI_SLUGS_KAD"] = kad_slugs.strip()

        test_click = st.button("🔌 Test API (δοκίμασε παραμετρικά)")
        if test_click:
            tried = []
            ok = []

            def _try(url):
                tried.append(url)
                try:
                    r = requests.get(url, headers={"Accept": "application/json"}, timeout=15)
                    return r.ok
                except Exception:
                    return False

            bases = [st.session_state["GEMI_BASE_URL"]]
            if not bases[0].endswith("/opendata"):
                bases.append(bases[0] + "/opendata")
            else:
                bases.append(bases[0].rsplit("/opendata", 1)[0])

            def _slugs(s): return [x.strip() for x in s.split(",") if x.strip()]

            for b in bases:
                for slug in _slugs(regions_slugs):
                    if _try(f"{b}/params/{slug}"):
                        ok.append(f"{b}/params/{slug}")
                        break
                for slug in _slugs(regunits_slugs):
                    if _try(f"{b}/params/{slug}"):
                        ok.append(f"{b}/params/{slug}")
                        break
                for slug in _slugs(dimoi_slugs):
                    if _try(f"{b}/params/{slug}"):
                        ok.append(f"{b}/params/{slug}")
                        break
                for slug in _slugs(status_slugs):
                    if _try(f"{b}/params/{slug}"):
                        ok.append(f"{b}/params/{slug}")
                        break
                for slug in _slugs(kad_slugs):
                    if _try(f"{b}/params/{slug}"):
                        ok.append(f"{b}/params/{slug}")
                        break

            if ok:
                st.success("OK! Βρέθηκαν παραμετρικά.")
            else:
                st.warning("Δεν απάντησαν τα παραμετρικά. Θα χρησιμοποιηθεί fallback (χωρίς IDs).")
            with st.expander("🔎 URLs που δοκιμάστηκαν"):
                st.text("\n".join(tried))

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
        str(s)
        .lower()
        .replace("(", " ").replace(")", " ")
        .replace("[", " ").replace("]", " ")
        .replace(".", " ").replace(",", " ")
        .replace("ά","α").replace("έ","ε").replace("ή","η")
        .replace("ί","ι").replace("ό","ο").replace("ύ","υ").replace("ώ","ω")
        .strip()
    )

def _find_col(df: pd.DataFrame, patterns):
    cleaned = {c: _clean_col(c) for c in df.columns}
    for p in patterns:
        for orig, cl in cleaned.items():
            if p in cl:
                return orig
    return None

def normalize_ftth(df: pd.DataFrame) -> pd.DataFrame:
    lat_col = _find_col(df, ["latitude", "lat", "πλατος", "γεωγραφικο πλατος", "φ"])
    lon_col = _find_col(df, ["longitude", "lon", "long", "μηκος", "γεωγραφικο μηκος", "λ"])
    if not lat_col or not lon_col:
        raise ValueError("Δεν βρέθηκαν στήλες latitude/longitude (δοκιμάστηκαν και ελληνικά: Πλάτος/Μήκος).")
    out = df[[lat_col, lon_col]].rename(columns={lat_col: "latitude", lon_col: "longitude"}).copy()
    out["latitude"] = pd.to_numeric(out["latitude"].astype(str).str.replace(",", "."), errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"].astype(str).str.replace(",", "."), errors="coerce")
    return out.dropna(subset=["latitude", "longitude"])

def _first_key(d: dict, keys, default=""):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k]:
            return d[k]
    return default

def _to_excel_bytes(df: pd.DataFrame):
    output = io.BytesIO()
    if df is None or df.empty:
        df = pd.DataFrame([{"info": "no data"}])
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    for c in df.columns:
        df[c] = df[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
    with pd.ExcelWriter(output, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    output.seek(0)
    return output

# ---------- GEMI client ----------
def _gemi_bases():
    """Return a list of base URLs to try, always with sane defaults."""
    base = (st.session_state.get("GEMI_BASE_URL") or DEFAULT_GEMI_BASE).strip().rstrip("/")
    if base.endswith("/opendata"):
        return [base, base.rsplit("/opendata", 1)[0]]
    return [base, base + "/opendata"]

def _gemi_headers():
    """
    Headers for opendata/publicity.
    - If header name & key exist → add them (for opendata).
    - If we talk to publicity → act like a browser, no api_key required.
    """
    base = (st.session_state.get("GEMI_BASE_URL") or "").lower()
    name = (st.session_state.get("GEMI_HEADER_NAME") or "").strip()
    key  = (st.session_state.get("GEMI_API_KEY") or "").strip()

    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if name and key:
        h[name] = key

    if "publicity.businessportal.gr" in base:
        h["X-Requested-With"] = "XMLHttpRequest"
        h["Origin"] = "https://publicity.businessportal.gr"
        h["Referer"] = "https://publicity.businessportal.gr/"
    return h

def _slugs_to_list(s: str):
    return [x.strip() for x in (s or "").split(",") if x.strip()]

def _gemi_candidates(kind: str, *, parent_id=None):
    tried = []
    if kind == "regions":
        for slug in _slugs_to_list(st.session_state["GEMI_SLUGS_REGIONS"]):
            tried.append(f"/params/{slug}")
    elif kind == "regional_units":
        for slug in _slugs_to_list(st.session_state["GEMI_SLUGS_REGUNITS"]):
            if parent_id is None:
                tried.append(f"/params/{slug}")
            else:
                tried.append(f"/params/{slug}/{parent_id}")
                tried.append(f"/params/{slug}?regionId={parent_id}")
    elif kind == "dimoi":
        for slug in _slugs_to_list(st.session_state["GEMI_SLUGS_DIMOI"]):
            if parent_id is None:
                tried.append(f"/params/{slug}")
            else:
                tried.append(f"/params/{slug}/{parent_id}")
                tried.append(f"/params/{slug}?prefectureId={parent_id}")
                tried.append(f"/params/{slug}?regionalUnitId={parent_id}")
    elif kind == "statuses":
        for slug in _slugs_to_list(st.session_state["GEMI_SLUGS_STATUS"]):
            tried.append(f"/params/{slug}")
    elif kind == "kad":
        for slug in _slugs_to_list(st.session_state["GEMI_SLUGS_KAD"]):
            tried.append(f"/params/{slug}")
    return tried

def gemi_params(kind, *, parent_id=None, timeout=20):
    urls_tried = []
    last_err = None
    for base in _gemi_bases():
        for path in _gemi_candidates(kind, parent_id=parent_id):
            url = f"{base}{path}"
            urls_tried.append(url)
            try:
                r = requests.get(url, headers=_gemi_headers(), timeout=timeout)
                if r.status_code == 404:
                    last_err = f"404 on {url}"
                    continue
                r.raise_for_status()
                st.session_state["GEMI_LAST_TRIED"] = urls_tried
                return r.json()
            except requests.RequestException as e:
                last_err = str(e)
                continue
    st.session_state["GEMI_LAST_TRIED"] = urls_tried
    raise RuntimeError(f"ΓΕΜΗ: δεν βρέθηκε endpoint για '{kind}'. Τελευταίο σφάλμα: {last_err}")

def gemi_search(
    *,
    region_id=None,
    regunit_id=None,
    nomos_id=None,
    dimos_id=None,
    status_id=None,
    name_part=None,
    kad_list=None,
    date_from=None,
    date_to=None,
    page=1,
    page_size=200,
    timeout=60,
    soft_fail=False,
):
    """
    Αναζήτηση εταιρειών: δοκιμάζει πολλαπλά paths + payload variants.
    Αν soft_fail=True και όλα αποτύχουν, επιστρέφει {"items": []}.
    """
    headers = _gemi_headers()

    payload_variants_post = [
        {
            "page": page,
            "page_size": page_size,
            "region_id": region_id,
            "perifereia_id": region_id,
            "regional_unit_id": regunit_id or nomos_id,
            "perifereiaki_enotita_id": regunit_id or nomos_id,
            "prefecture_id": regunit_id or nomos_id,
            "nomos_id": nomos_id,
            "dimos_id": dimos_id,
            "status_id": status_id,
            "name_part": name_part,
            "kad": kad_list or [],
            "incorporation_date_from": date_from,
            "incorporation_date_to": date_to,
            "foundation_date_from": date_from,
            "foundation_date_to": date_to,
            "registration_date_from": date_from,
            "registration_date_to": date_to,
        },
        {
            "page": page,
            "per_page": page_size,
            "regionId": region_id,
            "regionalUnitId": regunit_id or nomos_id,
            "prefectureId": regunit_id or nomos_id,
            "nomosId": nomos_id,
            "dimosId": dimos_id,
            "statusId": status_id,
            "name": name_part,
            "kad": kad_list or [],
            "incorporationDateFrom": date_from,
            "incorporationDateTo": date_to,
            "foundationDateFrom": date_from,
            "foundationDateTo": date_to,
            "registrationDateFrom": date_from,
            "registrationDateTo": date_to,
        },
    ]

    payload_variants_get = [
        {"page": page, "per_page": page_size},
        {"page": page, "page_size": page_size},
        payload_variants_post[-1],
    ]

    raw = st.session_state.get("GEMI_SEARCH_PATHS", "api/companies/search,companies/search,search")
    paths = [("/" + p.strip().lstrip("/")) for p in raw.split(",") if p.strip()]

    tried = []
    last_err = None

    for base in _gemi_bases():
        for path in paths:
            url = f"{base}{path}"

          # POST
            for payload in payload_variants_post:
                tried.append(f"POST {url} keys={list(payload.keys())}")
                try:
                    r = requests.post(url, json=payload, headers=headers, timeout=timeout)
                    if not r.ok:
                        # γράψε status + έως 400 chars από το body
                        last_err = f"POST {r.status_code} on {url} :: {r.text[:400]}"
                        continue
                    st.session_state["GEMI_SEARCH_TRIED"] = tried + [f"OK {url}"]
                    return r.json()
                except requests.RequestException as e:
                    last_err = f"POST EXC {type(e).__name__}: {e}"

            # GET
            for params in payload_variants_get:
                tried.append(f"GET  {url} keys={list(params.keys())}")
                try:
                    r = requests.get(url, params=params, headers=headers, timeout=timeout)
                    if not r.ok:
                        last_err = f"GET {r.status_code} on {url} :: {r.text[:400]}"
                        continue
                    st.session_state["GEMI_SEARCH_TRIED"] = tried + [f"OK {url}"]
                    return r.json()
                except requests.RequestException as e:
                    last_err = f"GET EXC {type(e).__name__}: {e}"

    st.session_state["GEMI_SEARCH_TRIED"] = tried
    if soft_fail:
        return {"items": []}
    raise RuntimeError(f"ΓΕΜΗ: αναζήτηση απέτυχε. Τελευταίο σφάλμα: {last_err}")

def gemi_search_all(
    *,
    region_id=None,
    regunit_id=None,
    nomos_id=None,
    dimos_id=None,
    status_id=None,
    name_part=None,
    kad_list=None,
    date_from=None,
    date_to=None,
    page_size=200,
    max_pages=200,
    sleep_sec=0.3,
    soft_fail=False,
):
    all_items = []
    for page in range(1, max_pages + 1):
        data = gemi_search(
            region_id=region_id,
            regunit_id=regunit_id,
            nomos_id=nomos_id,
            dimos_id=dimos_id,
            status_id=status_id,
            name_part=name_part,
            kad_list=kad_list,
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
            soft_fail=soft_fail,
        )
        items = data.get("items", data if isinstance(data, list) else [])
        if not items:
            break
        all_items.extend(items)
        if len(items) < page_size:
            break
        time.sleep(sleep_sec)
    return all_items

def gemi_items_to_df(items):
    rows = []
    for it in items:
        raw_kads = it.get("kads") or it.get("kad") or it.get("activity_codes")
        if isinstance(raw_kads, list):
            def _x(x):
                if isinstance(x, dict):
                    return x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or ""
                return str(x)
            kad_join = ";".join([_x(x) for x in raw_kads if x])
        else:
            kad_join = str(raw_kads or "")
        rows.append({
            "name":  _first_key(it, ["name","company_name","commercial_name","registered_name"]),
            "afm":   _first_key(it, ["afm","vat_number","tin"]),
            "gemi":  _first_key(it, ["gemi_number","registry_number","commercial_registry_no"]),
            "legal_form": _first_key(it, ["legal_form","company_type","form"]),
            "status":     _first_key(it, ["status","company_status","status_name"]),
            "incorporation_date": _first_key(it, [
                "incorporation_date","foundation_date","establishment_date","founded_at","registration_date"
            ]),
            "address": _first_key(it, ["address","postal_address","registered_address","address_line"]),
            "city":    _first_key(it, ["municipality","dimos_name","city","town"]),
            "postal_code": _first_key(it, ["postal_code","zip","tk","postcode"]),
            "phone":   _first_key(it, ["phone","telephone","contact_phone","phone_number"]),
            "email":   _first_key(it, ["email","contact_email","email_address"]),
            "website": _first_key(it, ["website","site","url","homepage"]),
            "kad_codes": kad_join,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["incorporation_date"] = df["incorporation_date"].astype(str).str.strip()
        df = df.drop_duplicates().sort_values(["name","city","postal_code"], na_position="last").reset_index(drop=True)
    return df

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

# ---------- GEMI UI ----------
gemi_df = None
if biz_source == "ΓΕΜΗ (OpenData API)":
    # Κρατάμε το api key που έβαλες
    st.session_state["GEMI_API_KEY"] = st.session_state.get("GEMI_API_KEY", "")

    # Προσπάθεια φόρτωσης παραμετρικών για ιεραρχία
    param_ok = True
    try:
        regions = gemi_params("regions")
        region_names = [r.get("name") for r in regions] or []
        region_name_to_id = {r.get("name"): r.get("id") for r in regions if isinstance(r, dict)}
    except Exception as e:
        param_ok = False
        regions = []; region_names = []; region_name_to_id = {}
        st.warning(f"⚠️ Παραμετρικά μη διαθέσιμα (Περιφέρειες): {e}. Χρήση fallback (χωρίς IDs).")

    name_part = st.text_input("Κομμάτι επωνυμίας (προαιρετικό)", "")

    # ΚΑΔ
    sel_kads = []
    fallback_kads = []
    try:
        kad_params = gemi_params("kad") if param_ok else []
    except Exception:
        kad_params = []
    if kad_params:
        def _kad_label(x):
            if isinstance(x, dict):
                code = x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or ""
                desc = x.get("name") or x.get("title") or x.get("description") or ""
                return f"{code} — {desc}".strip(" —")
            return str(x)
        kad_options = [(_kad_label(k), (k.get("code") or k.get("kad") or k.get("id") or k.get("nace") or "").strip())
                       for k in kad_params if isinstance(k, dict)]
        kad_labels = [lbl for (lbl, code) in kad_options if code]
        kad_label_to_code = {lbl: code for (lbl, code) in kad_options if code}
        sel_kad_labels = st.multiselect("ΚΑΔ (πολλοί, προαιρετικό)", kad_labels, default=[])
        sel_kads = [kad_label_to_code[lbl] for lbl in sel_kad_labels]
    else:
        kads_str = st.text_input("ΚΑΔ (comma-sep, π.χ. 47.11,56.10)", "")
        fallback_kads = [x for x in kads_str.replace(" ", "").split(",") if x]

    c1, c2 = st.columns(2)
    with c1:
        date_from = st.text_input("Σύσταση από (YYYY-MM-DD)", "")
    with c2:
        date_to = st.text_input("Σύσταση έως (YYYY-MM-DD)", "")

    # Ιεραρχία περιοχής
    selected_region_ids = []
    selected_regunit_ids = []
    selected_dimos_ids = []

    if param_ok and region_names:
        ALL_REG = "— Όλες οι Περιφέρειες —"
        sel_regions = st.multiselect("Περιφέρειες", [ALL_REG] + region_names, default=[ALL_REG])
        if sel_regions and not (len(sel_regions) == 1 and ALL_REG in sel_regions):
            selected_region_ids = [region_name_to_id[x] for x in sel_regions if x in region_name_to_id]

        # Περιφερειακές Ενότητες
        regunits = []
        if selected_region_ids:
            for rid in selected_region_ids:
                try:
                    regunits.extend(gemi_params("regional_units", parent_id=rid) or [])
                except Exception:
                    pass
        else:
            try:
                regunits = gemi_params("regional_units")
            except Exception:
                regunits = []

        regunit_names = [x.get("name") for x in regunits if isinstance(x, dict)]
        regunit_name_to_id = {x.get("name"): x.get("id") for x in regunits if isinstance(x, dict)}
        ALL_RU = "— Όλες οι Π.Ε. —"
        sel_reg_units = st.multiselect("Περιφερειακές Ενότητες", [ALL_RU] + regunit_names, default=[ALL_RU])
        if sel_reg_units and not (len(sel_reg_units) == 1 and ALL_RU in sel_reg_units):
            selected_regunit_ids = [regunit_name_to_id[x] for x in sel_reg_units if x in regunit_name_to_id]

        # Δήμοι
        dimoi = []
        if selected_regunit_ids:
            for ruid in selected_regunit_ids:
                try:
                    dimoi.extend(gemi_params("dimoi", parent_id=ruid) or [])
                except Exception:
                    pass
        else:
            try:
                dimoi = gemi_params("dimoi")
            except Exception:
                dimoi = []

        dimos_names = [d.get("name") for d in dimoi if isinstance(d, dict)]
        dimos_name_to_id = {d.get("name"): d.get("id") for d in dimoi if isinstance(d, dict)}
        ALL_DM = "— Όλοι οι Δήμοι —"
        sel_dimoi = st.multiselect("Δήμοι (πολλαπλή επιλογή)", [ALL_DM] + dimos_names, default=[ALL_DM])
        if sel_dimoi and not (len(sel_dimoi) == 1 and ALL_DM in sel_dimoi):
            selected_dimos_ids = [dimos_name_to_id[x] for x in sel_dimoi if x in dimos_name_to_id]
    else:
        st.markdown("**Φίλτρα περιοχής (fallback, string contains):**")
        fallback_region = st.text_input("Περιφέρεια (λέξη/λέξεις, comma-sep)", "")
        fallback_regunit = st.text_input("Π.Ε. (λέξη/λέξεις, comma-sep)", "")
        fallback_dimos = st.text_input("Δήμος (λέξη/λέξεις, comma-sep)", "")

    # Καταστάσεις
    status_id = None
    try:
        statuses = gemi_params("statuses") if param_ok else []
    except Exception:
        statuses = []
    if statuses:
        status_names = [s.get("name") for s in statuses]
        default_status = next((i for i, s in enumerate(statuses) if "ενεργ" in (s.get("name", "").lower())), 0)
        sel_status = st.selectbox("Κατάσταση", status_names, index=default_status)
        status_id = next((s.get("id") for s in statuses if s.get("name") == sel_status), None)

    cA, cB = st.columns(2)
    with cA:
        do_search = st.button("🔎 Αναζήτηση ΓΕΜΗ")
    with cB:
        do_export_one = st.button("⬇️ Εξαγωγή Excel (ένα αρχείο, με φίλτρα)")

    def _apply_client_filters(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        # Ημερομηνίες
        if (date_from or date_to) and "incorporation_date" in df.columns:
            dser = pd.to_datetime(df["incorporation_date"], errors="coerce").dt.date
            if date_from:
                try:
                    df = df[dser >= pd.to_datetime(date_from, errors="coerce").date()]
                except Exception:
                    pass
            if date_to:
                try:
                    df = df[dser <= pd.to_datetime(date_to, errors="coerce").date()]
                except Exception:
                    pass
        # ΚΑΔ
        if sel_kads:
            patt = "|".join([re.escape(k) for k in sel_kads if k])
            if patt:
                df = df[df["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]
        if fallback_kads:
            patt = "|".join([re.escape(k) for k in fallback_kads if k])
            if patt:
                df = df[df["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]
        # Περιοχή (fallback)
        if not param_ok and "city" in df.columns:
            if 'fallback_region' in locals() and fallback_region:
                keys = [x.strip().lower() for x in fallback_region.split(",") if x.strip()]
                df = df[df["city"].astype(str).str.lower().str.contains("|".join([re.escape(k) for k in keys]), na=False)]
            if 'fallback_regunit' in locals() and fallback_regunit:
                keys = [x.strip().lower() for x in fallback_regunit.split(",") if x.strip()]
                df = df[df["city"].astype(str).str.lower().str.contains("|".join([re.escape(k) for k in keys]), na=False)]
            if 'fallback_dimos' in locals() and fallback_dimos:
                keys = [x.strip().lower() for x in fallback_dimos.split(",") if x.strip()]
                df = df[df["city"].astype(str).str.lower().str.contains("|".join([re.escape(k) for k in keys]), na=False)]
        return df

    def _search_scopes():
        # Δήμοι -> Π.Ε. -> Περιφέρειες -> χωρίς IDs
        if selected_dimos_ids:
            return [{"dimos_id": d_id} for d_id in selected_dimos_ids]
        if selected_regunit_ids:
            return [{"regunit_id": ru_id} for ru_id in selected_regunit_ids]
        if selected_region_ids:
            return [{"region_id": r_id} for r_id in selected_region_ids]
        return [dict()]

    if do_search:
        all_items = []
        for scope in _search_scopes():
            data = gemi_search(
                region_id=scope.get("region_id"),
                regunit_id=scope.get("regunit_id"),
                dimos_id=scope.get("dimos_id"),
                status_id=status_id,
                name_part=name_part or None,
                kad_list=sel_kads or None,
                date_from=(date_from or None),
                date_to=(date_to or None),
                page=1,
                page_size=200,
                soft_fail=not param_ok,
            )
            items = data.get("items", [])
            all_items.extend(items)

        gemi_df = gemi_items_to_df(all_items)
        gemi_df = _apply_client_filters(gemi_df)

        if gemi_df.empty:
            st.warning("Δεν βρέθηκαν εγγραφές με τα φίλτρα που έβαλες.")
        else:
            st.success(f"Βρέθηκαν {len(gemi_df)} εγγραφές.")
            st.dataframe(gemi_df, use_container_width=True)
            st.download_button(
                "⬇️ Κατέβασμα επιχειρήσεων ΓΕΜΗ (Excel)",
                _to_excel_bytes(gemi_df),
                file_name="gemi_businesses.xlsx",
            )

    if do_export_one:
        with st.spinner("Εξαγωγή…"):
            dfs = []
            for scope in _search_scopes():
                items = gemi_search_all(
                    region_id=scope.get("region_id"),
                    regunit_id=scope.get("regunit_id"),
                    dimos_id=scope.get("dimos_id"),
                    status_id=status_id,
                    name_part=name_part or None,
                    kad_list=sel_kads or None,
                    date_from=(date_from or None),
                    date_to=(date_to or None),
                    soft_fail=not param_ok,
                )
                df = gemi_items_to_df(items)
                if not df.empty:
                    dfs.append(df)
            export_df = pd.concat(dfs, ignore_index=True).drop_duplicates() if dfs else pd.DataFrame()
            export_df = _apply_client_filters(export_df) if not export_df.empty else export_df

            if export_df.empty:
                st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
            else:
                st.success(f"Έτοιμο: {len(export_df)} εγγραφές στο αρχείο.")
                st.dataframe(export_df.head(50), use_container_width=True)
                st.download_button(
                    "⬇️ Excel – Επιχειρήσεις (ένα αρχείο, με φίλτρα)",
                    _to_excel_bytes(export_df),
                    file_name="gemi_filtered.xlsx",
                )

    # Διαγνωστικά
    with st.expander("🔎 Διαγνωστικά (GEMH)"):
        st.write("Τελευταία params URLs:")
        for line in st.session_state.get("GEMI_LAST_TRIED", []):
            st.text(line)
        st.write("Τελευταίες δοκιμές search:")
        for line in st.session_state.get("GEMI_SEARCH_TRIED", []):
            st.text(line)

# Αν επιλεγεί ΓΕΜΗ, χρησιμοποίησε αυτά τα δεδομένα ως πηγή επιχειρήσεων
if biz_source == "ΓΕΜΗ (OpenData API)":
    biz_df = gemi_df

# ---------- Geocode cache ----------
if CACHE_OK:
    requests_cache.install_cache("geocode_cache", backend="sqlite", expire_after=60 * 60 * 24 * 14)

session = requests.Session()
session.headers.update({"User-Agent": "ftth-app/1.0 (+contact: user)"})

def geocode_nominatim(address, cc="gr", lang="el"):
    params = {"q": address, "format": "json", "limit": 1, "countrycodes": cc, "accept-language": lang}
    r = session.get("https://nominatim.openstreetmap.org/search", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return (float(data[0]["lat"]), float(data[0]["lon"])) if data else (None, None)

def geocode_google(address, api_key, lang="el"):
    params = {"address": address, "key": api_key, "language": lang}
    r = session.get("https://maps.googleapis.com/maps/api/geocode/json", params=params, timeout=15)
    r.raise_for_status()
    js = r.json()
    if js.get("status") == "OK" and js.get("results"):
        loc = js["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"])
    return (None, None)

def geocode_address(address, provider, api_key=None, cc="gr", lang="el", throttle_sec=1.0):
    if provider.startswith("Google") and api_key:
        lat, lon = geocode_google(address, api_key, lang=lang)
    else:
        lat, lon = geocode_nominatim(address, cc, lang)
        time.sleep(throttle_sec)
    if lat is None and "greece" not in address.lower() and "ελλάδα" not in address.lower():
        fallback = f"{address}, Greece"
        if provider.startswith("Google") and api_key:
            lat, lon = geocode_google(fallback, api_key, lang=lang)
        else:
            lat, lon = geocode_nominatim(fallback, cc, lang)
            time.sleep(throttle_sec)
    return lat, lon

# ========== Main ==========
start = st.button("🚀 Ξεκίνα geocoding & matching")

if start and biz_df is not None and ftth_df is not None:
    work = biz_df.copy()

    # Επιλογή πιθανών στηλών διεύθυνσης/πόλης για κάθε είδος upload
    addr_series = pick_first_series(work, ["address", "site.company_insights.address", "διεύθυνση", "οδός", "διευθυνση"])
    city_series = pick_first_series(work, ["city", "site.company_insights.city", "πόλη"])

    # Τελική διεύθυνση προς geocoding
    base_addr = addr_series.astype(str).str.strip()
    from_input_city = city_series.astype(str).str.strip()
    work["Address"] = (base_addr + (", " + from_input_city).where(from_input_city.ne(""), "")).str.replace(r"\s+", " ", regex=True)

    # Αφαίρεση εντελώς κενών διευθύνσεων
    work = work[work["Address"].str.len() > 3].copy()

    # Γεωκωδικοποίηση
    total = len(work)
    progress = st.progress(0, text=f"0 / {total}")
    errs = 0

    # cache από προηγούμενο αρχείο
    geo_map = {}
    prev_df = load_table(prev_geo_file) if prev_geo_file is not None else None
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

    work["Latitude"]  = pd.to_numeric(work["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
    work["Longitude"] = pd.to_numeric(work["Longitude"].astype(str).str.replace(",", "."), errors="coerce")

    merged = work.copy()

    # Matching
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
                    "Distance(m)": round(d, 2),
                })
                break

    result_df = pd.DataFrame(matches)
    if not result_df.empty and "Distance(m)" in result_df.columns:
        result_df = result_df.sort_values("Distance(m)").reset_index(drop=True)

    if result_df.empty:
        st.warning(f"⚠️ Δεν βρέθηκαν αντιστοιχίσεις εντός {distance_limit} m.")
    else:
        st.success(f"✅ Βρέθηκαν {len(result_df)} επιχειρήσεις εντός {distance_limit} m από FTTH.")
        st.dataframe(result_df, use_container_width=True)

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
        st.download_button("⬇️ Geocoded διευθύνσεις (γραμμή-γραμμή)",
                           to_excel_bytes(merged[["Address","Latitude","Longitude"]]),
                           file_name="geocoded_addresses.xlsx")
    with c2:
        st.download_button("⬇️ Αποτελέσματα Matching", to_excel_bytes(result_df),
                           file_name="ftth_matching_results.xlsx")
    with c3:
        st.download_button("⬇️ Όλα τα δεδομένα (merged)", to_excel_bytes(merged),
                           file_name="merged_with_geocoded.xlsx")

elif start and (biz_df is None or ftth_df is None):
    st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
else:
    st.info("📄 Ανέβασε FTTH, επίλεξε πηγή επιχειρήσεων (Upload ή ΓΕΜΗ), και πάτα «🚀 Ξεκίνα».")
