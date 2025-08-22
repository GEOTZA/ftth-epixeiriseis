# ftth_scraper_nova_streamlit.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io, time, re, math
from urllib.parse import urljoin

# ---------- Optional cache ----------
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

st.set_page_config(page_title="FTTH Geocoding & Matching (v8)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v8")

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

    # --- API (ΓΕΜΗ) Ρυθμίσεις ---
    with st.expander("🔌 API (ΓΕΜΗ) Ρυθμίσεις", expanded=(biz_source=="ΓΕΜΗ (OpenData API)")):
        default_base = "https://opendata-api.businessportal.gr/api/opendata/v1"
        gemi_base  = st.text_input("Base URL", value=st.session_state.get("gemi_base", default_base))
        gemi_hdr   = st.text_input("Header name", value=st.session_state.get("gemi_header", "api_key"))
        gemi_key   = st.text_input("GEMH API Key", type="password", value=st.session_state.get("gemi_key",""))
        st.caption("Rate limit του ΓΕΜΗ: ~8 req/min. Για 429, περίμενε μερικά δευτ. ή μείωσε κλήσεις.")

        # Αποθήκευση ρυθμίσεων στο session
        st.session_state.update(gemi_base=gemi_base, gemi_header=gemi_hdr, gemi_key=gemi_key)

        c1, c2 = st.columns(2)
        with c1:
            test_params = st.button("🧪 Test API (params)")
        with c2:
            load_lists = st.button("🔁 Φόρτωση λιστών (regions/units/muni/statuses/KAD)")

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
    return (str(s).lower()
            .replace("(", " ").replace(")", " ")
            .replace("[", " ").replace("]", " ")
            .replace(".", " ").replace(",", " ")
            .replace("ά","α").replace("έ","ε").replace("ή","η")
            .replace("ί","ι").replace("ό","ο").replace("ύ","υ").replace("ώ","ω")
            .strip())

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
    out["latitude"]  = pd.to_numeric(out["latitude"].astype(str).str.replace(",", "."), errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"].astype(str).str.replace(",", "."), errors="coerce")
    out = out.dropna(subset=["latitude","longitude"])
    return out

def _first_non_empty(d, keys, default=""):
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip() != "":
            return v
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
TIMEOUT = 40

def _headers(api_key: str, header_name: str):
    return {header_name: api_key, "Accept": "application/json"} if api_key else {"Accept":"application/json"}

def _fix_base(base: str) -> str:
    # ελληνικό ‘ο’ → λατινικό ‘o’, καθάρισμα
    base = (base or "").replace("οpendata","opendata").strip()
    return base.rstrip("/")

def _safe_get(url, headers, params=None, timeout=TIMEOUT, retries=3, base_delay=0.8):
    """
    GET με backoff για 429. Αν υπάρχει Retry-After (sec), το ακολουθεί.
    """
    last = None
    for i in range(retries+1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429:
                # Respect Retry-After
                retry_after = r.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait = max(0.5, float(retry_after))
                    except Exception:
                        wait = base_delay * (2 ** i)
                else:
                    wait = base_delay * (2 ** i)
                time.sleep(wait)
                if i < retries:
                    continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            if i < retries:
                time.sleep(base_delay * (2 ** i))
            else:
                raise last

@st.cache_data(ttl=3600, show_spinner=False)
def cached_params(api_key: str, base: str, header_name: str, what: str, region_id=None):
    return gemi_params(api_key, what, region_id=region_id, base=base, header_name=header_name)

def gemi_params(api_key: str, what: str, *, region_id=None, base=None, header_name="api_key"):
    """
    Παράμετροι (regions/nomoi, regional_units/perifereiakes_enotites, dimoi/municipalities, statuses/kad)
    Δοκιμάζει πολλαπλά slugs. Χωρίς αυτόματο retry storm (το κάνει _safe_get).
    """
    base = _fix_base(base or st.session_state.get("gemi_base",""))
    headers = _headers(api_key, header_name)
    if not base:
        raise RuntimeError("Δεν έχει οριστεί Base URL για ΓΕΜΗ.")

    def E(ep): return urljoin(base + "/", ep.lstrip("/"))

    endpoints = []
    if what == "regions":
        endpoints = ["params/regions","params/nomoi","params/perifereies","params/peripheries"]
    elif what in ("regional_units","perifereiakes_enotites"):
        if region_id:
            endpoints = [f"params/regional_units/{region_id}",
                        f"params/perifereiakes_enotites/{region_id}",
                        f"params/periferiakes_enotites/{region_id}",
                        f"params/prefectures/{region_id}"]
        else:
            endpoints = ["params/regional_units","params/perifereiakes_enotites","params/periferiakes_enotites","params/prefectures"]
    elif what in ("dimoi","municipalities"):
        if region_id:
            endpoints = [f"params/dimoi/{region_id}", f"params/municipalities/{region_id}"]
        else:
            endpoints = ["params/dimoi","params/municipalities"]
    elif what in ("statuses",):
        endpoints = ["params/statuses","params/status","params/company_statuses"]
    elif what in ("kad","kads"):
        endpoints = ["params/kad","params/kads","params/activity_codes","params/kad_codes","params/nace"]
    else:
        endpoints = [f"params/{what}"]

    last_err = ""
    tried = []
    for ep in endpoints:
        u = E(ep)
        tried.append(u)
        try:
            r = _safe_get(u, headers=headers)
            js = r.json()
            if isinstance(js, (list, dict)):
                return js
        except Exception as e:
            last_err = str(e)
            continue
    raise RuntimeError(f"ΓΕΜΗ: δεν βρέθηκε endpoint για '{what}'. Τελευταίο σφάλμα: {last_err}\nΔοκιμάστηκαν:\n" + "\n".join(tried[-6:]))

def gemi_companies_search(api_key: str, *,
                          page=1, per_page=100,
                          name_part=None,
                          region_id=None, regional_unit_id=None, municipality_id=None,
                          status_id=None, kad_list=None,
                          date_from=None, date_to=None,
                          base=None, header_name="api_key"):
    """
    Αναζήτηση επιχειρήσεων — GET {base}/companies
    """
    base = _fix_base(base or st.session_state.get("gemi_base",""))
    headers = _headers(api_key, header_name)
    if not base:
        raise RuntimeError("Δεν έχει οριστεί Base URL για ΓΕΜΗ.")

    def B(path): return urljoin(base + "/", path.lstrip("/"))

    params_variants = [
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

    url = B("companies")
    last_err, last_keys = "", []
    for q in params_variants:
        q = {k: v for k, v in q.items() if v not in (None, "", [])}
        try:
            r = _safe_get(url, headers=headers, params=q)
            js = r.json()
            return js
        except Exception as e:
            last_err = str(e)
            last_keys = list(q.keys())
            continue
    raise RuntimeError(f"ΓΕΜΗ: αναζήτηση απέτυχε. Τελευταίο σφάλμα: {last_err} (url={url}, keys={last_keys})")

def gemi_companies_all(api_key: str, *,
                       name_part=None,
                       region_id=None, regional_unit_id=None, municipality_id=None,
                       status_id=None, kad_list=None,
                       date_from=None, date_to=None,
                       per_page=200, max_pages=100,
                       base=None, header_name="api_key"):
    items = []
    for p in range(1, max_pages+1):
        js = gemi_companies_search(
            api_key,
            page=p, per_page=per_page,
            name_part=name_part,
            region_id=region_id, regional_unit_id=regional_unit_id, municipality_id=municipality_id,
            status_id=status_id, kad_list=kad_list,
            date_from=date_from, date_to=date_to,
            base=base, header_name=header_name
        )
        arr = js.get("items") or js.get("data") or js.get("results") or []
        items.extend(arr)
        total = js.get("total") or js.get("total_count")
        if total and len(items) >= int(total):
            break
        if not arr or len(arr) < per_page:
            break
        time.sleep(0.25)  # μικρή καθυστέρηση για να μη χτυπάμε rate limit
    return items

def companies_items_to_df(items):
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
            "region": _first_non_empty(it, ["region","perifereia","region_name"]),
            "regional_unit": _first_non_empty(it, ["regional_unit","perifereiaki_enotita","nomos_name","prefecture"]),
            "municipality": _first_non_empty(it, ["municipality","dimos_name","city","town"]),
            "name":  _first_non_empty(it, ["name","company_name","commercial_name","registered_name"]),
            "afm":   _first_non_empty(it, ["afm","vat_number","tin"]),
            "gemi":  _first_non_empty(it, ["gemi_number","registry_number","commercial_registry_no","ar_gemi","arGemi"]),
            "legal_form": _first_non_empty(it, ["legal_form","company_type","form"]),
            "status":     _first_non_empty(it, ["status","company_status","status_name"]),
            "incorporation_date": _first_non_empty(it, [
                "incorporation_date","foundation_date","establishment_date","founded_at","registration_date"
            ]),
            "address": _first_non_empty(it, ["address","postal_address","registered_address","address_line"]),
            "postal_code": _first_non_empty(it, ["postal_code","zip","tk","postcode"]),
            "phone":   _first_non_empty(it, ["phone","telephone","contact_phone","phone_number"]),
            "email":   _first_non_empty(it, ["email","contact_email","email_address"]),
            "website": _first_non_empty(it, ["website","site","url","homepage"]),
            "kad_codes": kad_join,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["incorporation_date"] = df["incorporation_date"].astype(str).str.strip()
        df = df.drop_duplicates().reset_index(drop=True)
    return df

# ---- GEMI UI ----
gemi_df = None
if biz_source == "ΓΕΜΗ (OpenData API)":
    if not st.session_state.get("gemi_key"):
        st.warning("🔑 Βάλε GΕΜΗ API Key για να ενεργοποιηθεί η αναζήτηση.")
    else:
        try:
            st.subheader("🔎 Αναζήτηση ΓΕΜΗ (GET /companies)")
            base = st.session_state["gemi_base"]; hdr = st.session_state["gemi_header"]; key = st.session_state["gemi_key"]

            # Test button
            if 'pressed_test' not in st.session_state:
                st.session_state['pressed_test'] = False
            if 'lists_loaded' not in st.session_state:
                st.session_state['lists_loaded'] = False

            if test_params:
                st.session_state['pressed_test'] = True
                try:
                    tried = []
                    for ep in ["params/regions","params/nomoi","params/perifereies","params/peripheries","params/statuses","params/status"]:
                        u = urljoin(_fix_base(base)+"/", ep)
                        u = u.replace("οpendata","opendata")
                        tried.append(u)
                        r = _safe_get(u, headers=_headers(key, hdr))
                        if r.status_code == 200:
                            st.success("OK: κάποιο params endpoint απάντησε.")
                            break
                    st.code("\n".join(tried[-10:]), language="text")
                except Exception as e:
                    st.error(f"Σφάλμα params: {e}")

            # Φόρτωση λιστών ΜΟΝΟ κατόπιν ενέργειας
            if load_lists:
                st.session_state['lists_loaded'] = True

            region_map = {}
            runit_map = {}
            muni_map = {}
            status_map = {}
            kad_label_to_code = {}

            if st.session_state['lists_loaded']:
                # Regions
                try:
                    regions = cached_params(key, base, hdr, "regions")
                except Exception:
                    regions = []
                if isinstance(regions, list):
                    for r in regions:
                        rid = r.get("id") or r.get("code") or r.get("region_id") or r.get("nomos_id")
                        rname = r.get("name") or r.get("title") or r.get("label")
                        if rid and rname:
                            region_map[rname] = rid

                sel_region_name = st.selectbox("Περιφέρεια", ["— Όλες —"] + sorted(region_map.keys())) if region_map else st.selectbox("Περιφέρεια", ["— Όλες —"])
                sel_region_id = region_map.get(sel_region_name)

                # Regional Units
                if sel_region_id:
                    try:
                        runits = cached_params(key, base, hdr, "regional_units", region_id=sel_region_id)
                    except Exception:
                        runits = []
                    if isinstance(runits, list):
                        for u in runits:
                            uid = u.get("id") or u.get("code") or u.get("regional_unit_id") or u.get("prefecture_id")
                            uname = u.get("name") or u.get("title") or u.get("label")
                            if uid and uname:
                                runit_map[uname] = uid

                sel_runit_name = st.selectbox("Περιφερειακή Ενότητα", ["— Όλες —"] + sorted(runit_map.keys())) if runit_map else st.selectbox("Περιφερειακή Ενότητα", ["— Όλες —"])
                sel_runit_id = runit_map.get(sel_runit_name)

                # Municipalities
                if sel_runit_id:
                    try:
                        munis = cached_params(key, base, hdr, "dimoi", region_id=sel_runit_id)
                    except Exception:
                        munis = []
                    if isinstance(munis, list):
                        for m in munis:
                            mid = m.get("id") or m.get("code") or m.get("municipality_id") or m.get("dimos_id")
                            mname = m.get("name") or m.get("title") or m.get("label")
                            if mid and mname:
                                muni_map[mname] = mid

                sel_muni_name = st.selectbox("Δήμος", ["— Όλοι —"] + sorted(muni_map.keys())) if muni_map else st.selectbox("Δήμος", ["— Όλοι —"])
                sel_muni_id = muni_map.get(sel_muni_name)

                # Statuses
                try:
                    statuses = cached_params(key, base, hdr, "statuses")
                except Exception:
                    statuses = []
                if isinstance(statuses, list):
                    for s in statuses:
                        sid = s.get("id") or s.get("code")
                        sname = s.get("name") or s.get("title")
                        if sid and sname:
                            status_map[sname] = sid
                status_names = ["— Όλες —"] + sorted(status_map.keys())
                default_status_idx = 0
                for i,nm in enumerate(status_names):
                    if "ενεργ" in nm.lower():
                        default_status_idx = i; break
                sel_status_name = st.selectbox("Κατάσταση", status_names, index=default_status_idx)
                sel_status_id = status_map.get(sel_status_name)

                # KAD
                try:
                    kad_params = cached_params(key, base, hdr, "kad")
                except Exception:
                    kad_params = []
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
                st.info("Για να εμφανιστούν επιλογές στα dropdowns, πάτησε «🔁 Φόρτωση λιστών».")
                sel_region_id = sel_runit_id = sel_muni_id = sel_status_id = None
                sel_kads = []

            # Ελεύθερα φίλτρα
            name_part = st.text_input("Επωνυμία περιέχει (προαιρετικό)", "")
            c1, c2 = st.columns(2)
            with c1:
                date_from = st.text_input("Σύσταση από (YYYY-MM-DD)", "")
            with c2:
                date_to = st.text_input("Σύσταση έως (YYYY-MM-DD)", "")

            cA, cB = st.columns(2)
            with cA:
                do_search = st.button("🔎 Αναζήτηση ΓΕΜΗ (τρέχοντα φίλτρα)")
            with cB:
                do_export_one = st.button("⬇️ Εξαγωγή Excel (ένα αρχείο με εφαρμοσμένα φίλτρα)")

            if do_search:
                js = gemi_companies_search(
                    key,
                    page=1, per_page=200,
                    name_part=(name_part or None),
                    region_id=sel_region_id, regional_unit_id=sel_runit_id, municipality_id=sel_muni_id,
                    status_id=sel_status_id, kad_list=sel_kads or None,
                    date_from=(date_from or None), date_to=(date_to or None),
                    base=base, header_name=hdr
                )
                items = js.get("items") or js.get("data") or js.get("results") or []
                gemi_df = companies_items_to_df(items)

                # client-side safety filters
                if not gemi_df.empty and (date_from or date_to):
                    dser = pd.to_datetime(gemi_df["incorporation_date"], errors="coerce").dt.date
                    if date_from:
                        try:
                            dmin = pd.to_datetime(date_from, errors="coerce").date()
                            gemi_df = gemi_df[dser >= dmin]
                        except Exception:
                            pass
                    if date_to:
                        try:
                            dmax = pd.to_datetime(date_to, errors="coerce").date()
                            gemi_df = gemi_df[dser <= dmax]
                        except Exception:
                            pass
                if not gemi_df.empty and st.session_state.get('lists_loaded') and sel_kads:
                    patt = "|".join([re.escape(k) for k in sel_kads])
                    gemi_df = gemi_df[gemi_df["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]

                if gemi_df.empty:
                    st.warning("Δεν βρέθηκαν εγγραφές από ΓΕΜΗ με τα φίλτρα.")
                else:
                    st.success(f"Βρέθηκαν {len(gemi_df)} εγγραφές.")
                    st.dataframe(gemi_df, use_container_width=True)
                    st.download_button(
                        "⬇️ Κατέβασμα επιχειρήσεων ΓΕΜΗ (Excel)",
                        _to_excel_bytes(gemi_df),
                        file_name="gemi_businesses.xlsx"
                    )

            if do_export_one:
                with st.spinner("Εξαγωγή…"):
                    items = gemi_companies_all(
                        key,
                        name_part=(name_part or None),
                        region_id=sel_region_id, regional_unit_id=sel_runit_id, municipality_id=sel_muni_id,
                        status_id=sel_status_id, kad_list=sel_kads or None,
                        date_from=(date_from or None), date_to=(date_to or None),
                        per_page=200, max_pages=200,
                        base=base, header_name=hdr
                    )
                    export_df = companies_items_to_df(items)

                    # safety filters
                    if not export_df.empty and (date_from or date_to):
                        dser = pd.to_datetime(export_df["incorporation_date"], errors="coerce").dt.date
                        if date_from:
                            try:
                                dmin = pd.to_datetime(date_from, errors="coerce").date()
                                export_df = export_df[dser >= dmin]
                            except Exception:
                                pass
                        if date_to:
                            try:
                                dmax = pd.to_datetime(date_to, errors="coerce").date()
                                export_df = export_df[dser <= dmax]
                            except Exception:
                                pass
                    if not export_df.empty and st.session_state.get('lists_loaded') and sel_kads:
                        patt = "|".join([re.escape(k) for k in sel_kads])
                        export_df = export_df[export_df["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]

                    if export_df.empty:
                        st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
                    else:
                        st.success(f"Έτοιμο: {len(export_df)} εγγραφές στο αρχείο.")
                        st.dataframe(export_df.head(50), use_container_width=True)
                        st.download_button(
                            "⬇️ Excel – Επιχειρήσεις (ένα αρχείο, με φίλτρα)",
                            _to_excel_bytes(export_df),
                            file_name="gemi_filtered.xlsx"
                        )

        except Exception as e:
            st.error(f"Σφάλμα ΓΕΜΗ: {e}")
            st.stop()

# Αν επιλεγεί ΓΕΜΗ, χρησιμοποίησε αυτά τα δεδομένα ως πηγή επιχειρήσεων
if biz_source == "ΓΕΜΗ (OpenData API)":
    biz_df = gemi_df
else:
    biz_df = load_table(biz_file) if biz_file else None

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
    if provider.startswith("Google") and api_key:
        lat, lon = geocode_google(address, api_key, lang=lang)
    else:
        lat, lon = geocode_nominatim(address, cc, lang)
        if not getattr(session, "cache_disabled", True):
            time.sleep(throttle_sec)
    if (lat is None) and ("greece" not in address.lower()) and ("ελλάδα" not in address.lower()):
        fallback = f"{address}, Greece"
        if provider.startswith("Google") and api_key:
            lat, lon = geocode_google(fallback, api_key, lang=lang)
        else:
            lat, lon = geocode_nominatim(fallback, cc, lang)
            if not getattr(session, "cache_disabled", True):
                time.sleep(throttle_sec)
    return lat, lon

# ========== Main (Geocoding & Matching) ==========
start = st.button("🚀 Ξεκίνα geocoding & matching")

if start and biz_df is not None and ftth_df is not None:
    work = biz_df.copy()

    addr_series = pick_first_series(work, ["address", "site.company_insights.address", "διεύθυνση", "οδός", "διευθυνση"])
    city_series = pick_first_series(work, ["city", "site.company_insights.city", "πόλη"])

    base_addr = addr_series.astype(str).str.strip()
    from_input_city = city_series.astype(str).str.strip()
    work["Address"] = (base_addr + (", " + from_input_city).where(from_input_city.ne(""), "")).str.replace(r"\s+", " ", regex=True)
    work = work[work["Address"].str.len() > 3].copy()

    total = len(work)
    progress = st.progress(0, text=f"0 / {total}")
    errs = 0

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
        st.download_button("⬇️ Geocoded διευθύνσεις (γραμμή-γραμμή)", to_excel_bytes(merged[["Address","Latitude","Longitude"]]), file_name="geocoded_addresses.xlsx")
    with c2:
        st.download_button("⬇️ Αποτελέσματα Matching", to_excel_bytes(result_df), file_name="ftth_matching_results.xlsx")
    with c3:
        st.download_button("⬇️ Όλα τα δεδομένα (merged)", to_excel_bytes(merged), file_name="merged_with_geocoded.xlsx")

elif start and (biz_df is None or ftth_df is None):
    st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
else:
    st.info("📄 Ανέβασε FTTH, επίλεξε πηγή επιχειρήσεων (Upload ή ΓΕΜΗ), πάτα «🔁 Φόρτωση λιστών» (αν είναι ΓΕΜΗ) και μετά «🔎 Αναζήτηση».")
