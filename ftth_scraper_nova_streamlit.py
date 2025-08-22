# ftth_scraper_nova_streamlit.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io
import time
import re
from urllib.parse import urljoin

# ---------- Optional cache ----------
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

st.set_page_config(page_title="FTTH Geocoding & ΓΕΜΗ (v7)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v7")

# =========================
# Sidebar – Ρυθμίσεις
# =========================
with st.sidebar:
    st.header("🗺️ Geocoding")
    geocoder = st.selectbox("Geocoder", ["Nominatim (δωρεάν)", "Google (API key)"])
    google_key = st.text_input("Google API key", type="password", help="Αν μείνει κενό, χρησιμοποιείται Nominatim.")
    country = st.text_input("Country code", "gr")
    lang = st.text_input("Language", "el")
    throttle = st.slider("Καθυστέρηση (sec) [Nominatim]", 0.5, 2.0, 1.0, 0.5)
    distance_limit = st.number_input("📏 Μέγιστη απόσταση (m)", min_value=1, max_value=500, value=150)

    st.markdown("---")
    st.header("🔌 ΓΕΜΗ API")
    # Προεπιλογή σωστού base (ΛΑΤΙΝΙΚΟ 'o' στο opendata)
    default_base = "https://opendata-api.businessportal.gr/api/opendata/v1"
    gemi_base = st.text_input("Base URL", value=st.session_state.get("gemi_base", default_base))
    gemi_header = st.text_input("Header name", value=st.session_state.get("gemi_header", "api_key"))
    gemi_key = st.text_input("GEMI API Key", type="password", value=st.session_state.get("gemi_key", ""))

    # Αποθήκευση για χρήση στα Tabs
    st.session_state.update(gemi_base=gemi_base, gemi_header=gemi_header, gemi_key=gemi_key)

    st.caption("Limit: 8 req/min → γίνονται λίγες κλήσεις με backoff (429).")
    if st.button("🧪 Test API (params/regions)"):
        try:
            def _fix_base(b): return (b or "").replace("οpendata","opendata").rstrip("/")
            test_url = urljoin(_fix_base(gemi_base) + "/", "params/regions")
            r = requests.get(test_url, headers={gemi_header: gemi_key} if gemi_key else {}, timeout=20)
            r.raise_for_status()
            st.success("OK: Το endpoint απάντησε.")
        except Exception as e:
            st.error(f"Σφάλμα: {e}")

# =========================
# Helpers (κοινά)
# =========================
TIMEOUT = 40

def _to_excel_bytes(df: pd.DataFrame, sheet="Sheet1"):
    out = io.BytesIO()
    safe = df.copy()
    if safe is None or safe.empty:
        safe = pd.DataFrame([{"info": "no data"}])
    safe.columns = [str(c) for c in safe.columns]
    for c in safe.columns:
        safe[c] = safe[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        safe.to_excel(w, index=False, sheet_name=sheet)
    out.seek(0)
    return out

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

def geocode_nominatim(session, address, cc="gr", lang="el"):
    params = {"q": address, "format": "json", "limit": 1, "countrycodes": cc, "accept-language": lang}
    r = session.get("https://nominatim.openstreetmap.org/search", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data:
        return float(data[0]["lat"]), float(data[0]["lon"])
    return None, None

def geocode_google(session, address, api_key, lang="el"):
    params = {"address": address, "key": api_key, "language": lang}
    r = session.get("https://maps.googleapis.com/maps/api/geocode/json", params=params, timeout=15)
    r.raise_for_status()
    js = r.json()
    if js.get("status") == "OK" and js.get("results"):
        loc = js["results"][0]["geometry"]["location"]
        return float(loc["lat"]), float(loc["lng"])
    return None, None

def geocode_address(session, address, provider, api_key=None, cc="gr", lang="el", throttle_sec=1.0):
    if provider.startswith("Google") and api_key:
        lat, lon = geocode_google(session, address, api_key, lang=lang)
    else:
        lat, lon = geocode_nominatim(session, address, cc, lang)
        # throttle μόνο σε πραγματικό network call (όχι cache)
        if not getattr(session, "cache_disabled", True):
            time.sleep(throttle_sec)
    if (lat is None) and ("greece" not in address.lower()) and ("ελλάδα" not in address.lower()):
        fallback = f"{address}, Greece"
        if provider.startswith("Google") and api_key:
            lat, lon = geocode_google(session, fallback, api_key, lang=lang)
        else:
            lat, lon = geocode_nominatim(session, fallback, cc, lang)
            if not getattr(session, "cache_disabled", True):
                time.sleep(throttle_sec)
    return lat, lon

# =========================
# ΓΕΜΗ – μινι API client
# =========================
def _fix_base(base: str) -> str:
    return (base or "").replace("οpendata", "opendata").rstrip("/")

def _headers(api_key: str, header_name: str):
    h = {"Accept": "application/json"}
    if api_key:
        h[header_name] = api_key
    return h

def _safe_get(url, headers, params=None, timeout=TIMEOUT, retries=3, base_delay=0.9):
    last = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra is not None:
                    try:
                        wait = max(0.5, float(ra))
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
def get_params_cached(api_key: str, base: str, header_name: str, what: str, region_id=None):
    return gemi_params(api_key, base, header_name, what, region_id=region_id)

def gemi_params(api_key: str, base: str, header_name: str, what: str, *, region_id=None):
    base = _fix_base(base)
    headers = _headers(api_key, header_name)
    def E(ep): return urljoin(base + "/", ep.lstrip("/"))

    endpoints = []
    if what == "regions":
        endpoints = ["params/regions", "params/perifereies", "params/peripheries", "params/nomoi"]
    elif what in ("regional_units","perifereiakes_enotites"):
        if region_id:
            endpoints = [f"params/regional_units/{region_id}",
                         f"params/perifereiakes_enotites/{region_id}",
                         f"params/periferiakes_enotites/{region_id}",
                         f"params/prefectures/{region_id}"]
        else:
            endpoints = ["params/regional_units", "params/perifereiakes_enotites",
                         "params/periferiakes_enotites", "params/prefectures"]
    elif what in ("dimoi","municipalities"):
        if region_id:
            endpoints = [f"params/dimoi/{region_id}", f"params/municipalities/{region_id}"]
        else:
            endpoints = ["params/dimoi", "params/municipalities"]
    elif what in ("statuses",):
        endpoints = ["params/statuses", "params/status", "params/company_statuses"]
    elif what in ("kad","kads"):
        endpoints = ["params/kad","params/kads","params/activity_codes","params/kad_codes","params/nace"]
    else:
        endpoints = [f"params/{what}"]

    last_err, tried = "", []
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
    raise RuntimeError(f"ΓΕΜΗ: δεν βρέθηκε endpoint για '{what}'. Τελ. σφάλμα: {last_err}\nΔοκιμάστηκαν:\n" + "\n".join(tried[-6:]))

def _variants_query(page, per_page, name_part,
                    region_id, regional_unit_id, municipality_id,
                    status_id, kad_list,
                    date_from, date_to):
    return [
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

def gemi_companies_search(api_key: str, base: str, header_name: str, *,
                          page=1, per_page=200,
                          name_part=None,
                          region_id=None, regional_unit_id=None, municipality_id=None,
                          status_id=None, kad_list=None,
                          date_from=None, date_to=None):
    """
    Δοκιμάζει:
    1) GET {base}/companies (με διάφορες παραμέτρους)
    2) POST {base}/companies/search (fallback)
    """
    base = _fix_base(base)
    headers = _headers(api_key, header_name)
    vlist = _variants_query(page, per_page, name_part,
                            region_id, regional_unit_id, municipality_id,
                            status_id, kad_list, date_from, date_to)

    # 1) GET /companies
    url_get = urljoin(base + "/", "companies")
    last_err, last_keys = "", []
    for q in vlist:
        q = {k: v for k, v in q.items() if v not in (None, "", [])}
        try:
            r = _safe_get(url_get, headers=headers, params=q)
            return r.json()
        except Exception as e:
            last_err = str(e); last_keys = list(q.keys())

    # 2) POST /companies/search
    url_post = urljoin(base + "/", "companies/search")
    for q in vlist:
        q = {k: v for k, v in q.items() if v not in (None, "", [])}
        try:
            r = requests.post(url_post, json=q, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                time.sleep(float(ra) if ra else 1.5)
                r = requests.post(url_post, json=q, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = str(e); last_keys = list(q.keys())

    raise RuntimeError(f"ΓΕΜΗ: αναζήτηση απέτυχε. Τελευταίο σφάλμα: {last_err} (url={url_get} → {url_post}, keys={last_keys})")

def gemi_companies_all(api_key: str, base: str, header_name: str, *,
                       name_part=None,
                       region_id=None, regional_unit_id=None, municipality_id=None,
                       status_id=None, kad_list=None,
                       date_from=None, date_to=None,
                       per_page=200, max_pages=120):
    items = []
    for p in range(1, max_pages + 1):
        js = gemi_companies_search(
            api_key, base, header_name,
            page=p, per_page=per_page,
            name_part=name_part,
            region_id=region_id, regional_unit_id=regional_unit_id, municipality_id=municipality_id,
            status_id=status_id, kad_list=kad_list,
            date_from=date_from, date_to=date_to,
        )
        arr = js.get("items") or js.get("data") or js.get("results") or []
        items.extend(arr)
        total = js.get("total") or js.get("total_count")
        if total and len(items) >= int(total):
            break
        if not arr or len(arr) < per_page:
            break
        time.sleep(0.9)  # μικρή παύση να αποφύγουμε 429
    return items

def companies_items_to_df(items: list[dict]) -> pd.DataFrame:
    def first(d, keys, default=""):
        for k in keys:
            v = d.get(k)
            if v is not None and str(v).strip() != "":
                return v
        return default

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
            "region": first(it, ["region","perifereia","region_name"]),
            "regional_unit": first(it, ["regional_unit","perifereiaki_enotita","nomos_name","prefecture"]),
            "municipality": first(it, ["municipality","dimos_name","city","town"]),
            "name":  first(it, ["name","company_name","commercial_name","registered_name"]),
            "afm":   first(it, ["afm","vat_number","tin"]),
            "gemi":  first(it, ["gemi_number","registry_number","commercial_registry_no","ar_gemi","arGemi"]),
            "legal_form": first(it, ["legal_form","company_type","form"]),
            "status":     first(it, ["status","company_status","status_name"]),
            "incorporation_date": first(it, [
                "incorporation_date","foundation_date","establishment_date","founded_at","registration_date"
            ]),
            "address": first(it, ["address","postal_address","registered_address","address_line"]),
            "postal_code": first(it, ["postal_code","zip","tk","postcode"]),
            "phone":   first(it, ["phone","telephone","contact_phone","phone_number"]),
            "email":   first(it, ["email","contact_email","email_address"]),
            "website": first(it, ["website","site","url","homepage"]),
            "kad_codes": kad_join,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["incorporation_date"] = df["incorporation_date"].astype(str).str.strip()
        df = df.drop_duplicates().reset_index(drop=True)
    return df

# =========================
# Tabs
# =========================
tab1, tab2 = st.tabs(["🏁 FTTH Geocoding & Matching", "🏷️ ΓΕΜΗ – Εξαγωγή Excel"])

# --------------------------------------------------------------------
# Tab 2: ΓΕΜΗ – Εξαγωγή (κρατάει αποτελέσματα σε session για χρήση στο Tab1)
# --------------------------------------------------------------------
with tab2:
    st.subheader("ΓΕΜΗ – Φίλτρα & Εξαγωγή")

    # On-demand κουμπιά για φόρτωση λιστών (λιγότερες κλήσεις)
    cbtn = st.columns(5)
    b_regions = cbtn[0].button("① Περιφέρειες")
    b_units   = cbtn[1].button("② Περιφ. Ενότητες")
    b_muni    = cbtn[2].button("③ Δήμοι")
    b_status  = cbtn[3].button("④ Καταστάσεις")
    b_kad     = cbtn[4].button("⑤ ΚΑΔ")

    # session stores
    for key in ["regions_map","runits_map","muni_map","status_map","kad_label_to_code"]:
        if key not in st.session_state:
            st.session_state[key] = {}

    try:
        if b_regions:
            r = get_params_cached(st.session_state["gemi_key"], st.session_state["gemi_base"], st.session_state["gemi_header"], "regions")
            mp = {}
            if isinstance(r, list):
                for x in r:
                    rid = x.get("id") or x.get("code") or x.get("region_id") or x.get("nomos_id")
                    rname = x.get("name") or x.get("title") or x.get("label")
                    if rid and rname:
                        mp[rname] = rid
            st.session_state["regions_map"] = mp
            st.success(f"Φορτώθηκαν Περιφέρειες: {len(mp)}")

        region_names = ["— Όλες —"] + sorted(st.session_state["regions_map"].keys()) if st.session_state["regions_map"] else ["— Όλες —"]
        sel_region_name = st.selectbox("Περιφέρεια", region_names, index=0)
        sel_region_id = st.session_state["regions_map"].get(sel_region_name)

        if b_units:
            if not sel_region_id:
                st.warning("Διάλεξε πρώτα Περιφέρεια.")
            else:
                u = get_params_cached(st.session_state["gemi_key"], st.session_state["gemi_base"], st.session_state["gemi_header"], "regional_units", region_id=sel_region_id)
                mp = {}
                if isinstance(u, list):
                    for x in u:
                        uid = x.get("id") or x.get("code") or x.get("regional_unit_id") or x.get("prefecture_id")
                        uname = x.get("name") or x.get("title") or x.get("label")
                        if uid and uname:
                            mp[uname] = uid
                st.session_state["runits_map"] = mp
                st.success(f"Φορτώθηκαν Περιφερειακές Ενότητες: {len(mp)}")

        runit_names = ["— Όλες —"] + sorted(st.session_state["runits_map"].keys()) if st.session_state["runits_map"] else ["— Όλες —"]
        sel_runit_name = st.selectbox("Περιφερειακή Ενότητα", runit_names, index=0)
        sel_runit_id = st.session_state["runits_map"].get(sel_runit_name)

        if b_muni:
            if not sel_runit_id:
                st.warning("Διάλεξε πρώτα Περιφερειακή Ενότητα.")
            else:
                m = get_params_cached(st.session_state["gemi_key"], st.session_state["gemi_base"], st.session_state["gemi_header"], "dimoi", region_id=sel_runit_id)
                mp = {}
                if isinstance(m, list):
                    for x in m:
                        mid = x.get("id") or x.get("code") or x.get("municipality_id") or x.get("dimos_id")
                        mname = x.get("name") or x.get("title") or x.get("label")
                        if mid and mname:
                            mp[mname] = mid
                st.session_state["muni_map"] = mp
                st.success(f"Φορτώθηκαν Δήμοι: {len(mp)}")

        muni_names = ["— Όλοι —"] + sorted(st.session_state["muni_map"].keys()) if st.session_state["muni_map"] else ["— Όλοι —"]
        sel_muni_name = st.selectbox("Δήμος", muni_names, index=0)
        sel_muni_id = st.session_state["muni_map"].get(sel_muni_name)

        if b_status:
            s = get_params_cached(st.session_state["gemi_key"], st.session_state["gemi_base"], st.session_state["gemi_header"], "statuses")
            mp = {}
            if isinstance(s, list):
                for x in s:
                    sid = x.get("id") or x.get("code")
                    sname = x.get("name") or x.get("title")
                    if sid and sname:
                        mp[sname] = sid
            st.session_state["status_map"] = mp
            st.success(f"Φορτώθηκαν καταστάσεις: {len(mp)}")

        status_names = ["— Όλες —"] + sorted(st.session_state["status_map"].keys()) if st.session_state["status_map"] else ["— Όλες —"]
        default_idx = 0
        for i, nm in enumerate(status_names):
            if "ενεργ" in nm.lower():
                default_idx = i; break
        sel_status_name = st.selectbox("Κατάσταση", status_names, index=default_idx)
        sel_status_id = st.session_state["status_map"].get(sel_status_name)

        if b_kad:
            k = get_params_cached(st.session_state["gemi_key"], st.session_state["gemi_base"], st.session_state["gemi_header"], "kad")
            lbl_to_code = {}
            if isinstance(k, list):
                def _lbl(x):
                    if isinstance(x, dict):
                        code = x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or ""
                        desc = x.get("name") or x.get("title") or x.get("description") or ""
                        return f"{code} — {desc}".strip(" —")
                    return str(x)
                for x in k:
                    if not isinstance(x, dict): 
                        continue
                    code = (x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or "").strip()
                    if code:
                        lbl_to_code[_lbl(x)] = code
            st.session_state["kad_label_to_code"] = lbl_to_code
            st.success(f"Φορτώθηκαν ΚΑΔ: {len(lbl_to_code)}")

        kad_labels = sorted(st.session_state["kad_label_to_code"].keys())
        sel_kad_labels = st.multiselect("ΚΑΔ (προαιρετικό)", kad_labels, default=[])
        sel_kads = [st.session_state["kad_label_to_code"][l] for l in sel_kad_labels]

    except Exception as e:
        st.error(f"Σφάλμα φόρτωσης λιστών: {e}")

    # Ελεύθερα φίλτρα
    name_part = st.text_input("Επωνυμία περιέχει (προαιρετικό)", "")
    c1, c2 = st.columns(2)
    with c1:
        date_from = st.text_input("Σύσταση από (YYYY-MM-DD)", "")
    with c2:
        date_to = st.text_input("Σύσταση έως (YYYY-MM-DD)", "")

    cA, cB = st.columns(2)
    with cA:
        do_preview = st.button("🔎 Προεπισκόπηση (μέχρι 200 εγγραφές)")
    with cB:
        do_export = st.button("⬇️ Εξαγωγή Excel (όλες οι σελίδες)")

    def _apply_safety_filters(df: pd.DataFrame):
        out = df.copy()
        if not out.empty and (date_from or date_to):
            dser = pd.to_datetime(out["incorporation_date"], errors="coerce").dt.date
            if date_from:
                try:
                    dmin = pd.to_datetime(date_from, errors="coerce").date()
                    out = out[dser >= dmin]
                except Exception:
                    pass
            if date_to:
                try:
                    dmax = pd.to_datetime(date_to, errors="coerce").date()
                    out = out[dser <= dmax]
                except Exception:
                    pass
        if not out.empty and sel_kads:
            patt = "|".join([re.escape(k) for k in sel_kads])
            out = out[out["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]
        return out

    if do_preview:
        try:
            js = gemi_companies_search(
                st.session_state["gemi_key"], st.session_state["gemi_base"], st.session_state["gemi_header"],
                page=1, per_page=200,
                name_part=(name_part or None),
                region_id=sel_region_id, regional_unit_id=sel_runit_id, municipality_id=sel_muni_id,
                status_id=sel_status_id, kad_list=sel_kads or None,
                date_from=(date_from or None), date_to=(date_to or None),
            )
            items = js.get("items") or js.get("data") or js.get("results") or []
            df = companies_items_to_df(items)
            df = _apply_safety_filters(df)
            if df.empty:
                st.warning("Δεν βρέθηκαν εγγραφές.")
            else:
                st.success(f"Βρέθηκαν {len(df)} εγγραφές (προεπισκόπηση).")
                st.dataframe(df, use_container_width=True)
                st.download_button("⬇️ Κατέβασμα προεπισκόπησης (Excel)", _to_excel_bytes(df, "companies"), file_name="gemi_preview.xlsx")
                # αποθήκευση για χρήση στο Tab1
                st.session_state["gemi_export_df"] = df.copy()
        except Exception as e:
            st.error(f"Σφάλμα αναζήτησης: {e}")

    if do_export:
        try:
            with st.spinner("Γίνεται εξαγωγή…"):
                all_items = gemi_companies_all(
                    st.session_state["gemi_key"], st.session_state["gemi_base"], st.session_state["gemi_header"],
                    name_part=(name_part or None),
                    region_id=sel_region_id, regional_unit_id=sel_runit_id, municipality_id=sel_muni_id,
                    status_id=sel_status_id, kad_list=sel_kads or None,
                    date_from=(date_from or None), date_to=(date_to or None),
                    per_page=200, max_pages=200
                )
                df = companies_items_to_df(all_items)
                df = _apply_safety_filters(df)
                if df.empty:
                    st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
                else:
                    st.success(f"Έτοιμο: {len(df)} εγγραφές.")
                    st.dataframe(df.head(50), use_container_width=True)
                    st.download_button("⬇️ Excel – Επιχειρήσεις (με φίλτρα)", _to_excel_bytes(df, "companies"), file_name="gemi_filtered.xlsx")
                    # αποθήκευση για χρήση στο Tab1
                    st.session_state["gemi_export_df"] = df.copy()
        except Exception as e:
            st.error(f"Σφάλμα εξαγωγής: {e}")

# --------------------------------------------------------------------
# Tab 1: FTTH Geocoding & Matching
# --------------------------------------------------------------------
with tab1:
    st.subheader("📥 Αρχεία")
    # Επιλογή πηγής επιχειρήσεων: Upload ή χρήση των τελευταίων αποτελεσμάτων ΓΕΜΗ
    has_gemi_results = "gemi_export_df" in st.session_state and isinstance(st.session_state["gemi_export_df"], pd.DataFrame) and not st.session_state["gemi_export_df"].empty
    src_opts = ["Upload Excel/CSV"] + (["Χρήση αποτελεσμάτων ΓΕΜΗ (Tab 2)"] if has_gemi_results else [])
    biz_source = st.radio("Πηγή Επιχειρήσεων", src_opts, index=0, horizontal=False)

    biz_file = None
    if biz_source == "Upload Excel/CSV":
        biz_file = st.file_uploader("Excel/CSV Επιχειρήσεων", type=["xlsx", "csv"])

    ftth_file = st.file_uploader("FTTH σημεία Nova (Excel/CSV) – υποστηρίζει ελληνικές στήλες λ/φ και πολλαπλά sheets", type=["xlsx", "csv"])
    prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά) – Excel/CSV με στήλες: Address, Latitude, Longitude", type=["xlsx", "csv"])

    # ---------- FTTH load ----------
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

    # ---------- Biz source resolve ----------
    biz_df = None
    if biz_source == "Upload Excel/CSV":
        biz_df = load_table(biz_file) if biz_file else None
    elif biz_source.startswith("Χρήση αποτελεσμάτων ΓΕΜΗ"):
        biz_df = st.session_state.get("gemi_export_df")

    # ---------- Geocode cache ----------
    if CACHE_OK:
        requests_cache.install_cache("geocode_cache", backend="sqlite", expire_after=60*60*24*14)

    session = requests.Session()
    session.headers.update({"User-Agent": "ftth-app/1.0 (+contact: user)"})

    # ========== Main ==========
    start = st.button("🚀 Ξεκίνα geocoding & matching")

    if start and biz_df is not None and ftth_df is not None:
        work = biz_df.copy()

        # Επιλογή πιθανών στηλών διεύθυνσης/πόλης
        addr_series = pick_first_series(work, ["address", "site.company_insights.address", "διεύθυνση", "οδός", "διευθυνση"])
        city_series = pick_first_series(work, ["city", "site.company_insights.city", "πόλη"])

        base_addr = addr_series.astype(str).str.strip()
        from_input_city = city_series.astype(str).str.strip()
        work["Address"] = (base_addr + (", " + from_input_city).where(from_input_city.ne(""), "")).str.replace(r"\s+", " ", regex=True)

        work = work[work["Address"].str.len() > 3].copy()

        total = len(work)
        progress = st.progress(0, text=f"0 / {total}")
        errs = 0

        # cache από prev_geo_file (αν δόθηκε)
        geo_map = {}
        prev_df = load_table(prev_geo_file) if prev_geo_file is not None else None
        if prev_df is not None:
            cols_lower = {c.lower(): c for c in prev_df.columns}
            if {"address","latitude","longitude"}.issubset(set(cols_lower.keys())):
                p = prev_df.rename(columns={
                    cols_lower.get("address"): "Address",
                    cols_lower.get("latitude"): "Latitude",
                    cols_lower.get("longitude"): "Longitude",
                })
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
                lat, lon = geocode_address(session, addr, geocoder, api_key=google_key, cc=country, lang=lang, throttle_sec=throttle)
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

        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button("⬇️ Geocoded διευθύνσεις (γραμμή-γραμμή)", _to_excel_bytes(merged[["Address","Latitude","Longitude"]], "geocoded"), file_name="geocoded_addresses.xlsx")
        with c2:
            st.download_button("⬇️ Αποτελέσματα Matching", _to_excel_bytes(result_df, "matching"), file_name="ftth_matching_results.xlsx")
        with c3:
            st.download_button("⬇️ Όλα τα δεδομένα (merged)", _to_excel_bytes(merged, "merged"), file_name="merged_with_geocoded.xlsx")

    elif start and (biz_df is None or ftth_df is None):
        st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
    else:
        st.info("📄 Ανέβασε FTTH, διάλεξε πηγή επιχειρήσεων (Upload ή από ΓΕΜΗ στο Tab 2), και πάτα «🚀 Ξεκίνα».")
