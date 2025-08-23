# ftth_scraper_nova_streamlit.py
# -*- coding: utf-8 -*-

import io
import time
import re
import requests
import pandas as pd
import streamlit as st
from geopy.distance import geodesic

# ---------- Optional caches / libs ----------
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

try:
    # προαιρετικό για email verification (syntax + DNS)
    from email_validator import validate_email, EmailNotValidError
    EMAIL_LIB_OK = True
except Exception:
    EMAIL_LIB_OK = False

# ---------- Page ----------
st.set_page_config(page_title="FTTH + ΓΕΜΗ Toolkit (v7)", layout="wide")
st.title("📡 FTTH Geocoding & Matching  •  📊 ΓΕΜΗ Downloader (v7)")

# ========== Sidebar (Settings) ==========
with st.sidebar:
    st.header("Ρυθμίσεις")

    # --- Geocoder settings (για FTTH tab) ---
    st.subheader("Geocoder")
    geocoder = st.selectbox("Πάροχος", ["Nominatim (δωρεάν)", "Google (API key)"])
    google_key = st.text_input("Google API key", type="password", help="Αν μείνει κενό, χρησιμοποιείται Nominatim.")
    country = st.text_input("Country code", "gr")
    lang = st.text_input("Language", "el")
    throttle = st.slider("Καθυστέρηση (sec) [Nominatim]", 0.5, 2.0, 1.0, 0.5)
    distance_limit = st.number_input("📏 Μέγιστη απόσταση (m)", min_value=1, max_value=500, value=150)

    st.divider()

    # --- Πηγή επιχειρήσεων για το FTTH tab ---
    biz_source = st.radio("Πηγή Επιχειρήσεων (για FTTH Finder)", ["Upload Excel/CSV", "ΓΕΜΗ (OpenData API)"], index=0)

    st.divider()

    # --- ΓΕΜΗ API Settings ---
    st.subheader("ΓΕΜΗ API")
    # Σύμφωνα με τα Swagger που έστειλες
    DEFAULT_GEMI_BASE = "https://opendata-api.businessportal.gr/api/opendata/v1"
    DEFAULT_HEADER = "api_key"

    gemi_base = st.text_input("Base URL", value=st.session_state.get("gemi_base", DEFAULT_GEMI_BASE),
                              help="Swagger basePath")
    gemi_key = st.text_input("API Key", type="password", value=st.session_state.get("gemi_key", ""))
    st.caption("Header name: api_key (όπως στο Swagger)")

    # Αποθήκευση στο session_state
    st.session_state.update(gemi_base=gemi_base, gemi_key=gemi_key)

    cta1, cta2 = st.columns(2)
    with cta1:
        if st.button("🧪 Test /health"):
            try:
                r = requests.get(f"{gemi_base.rstrip('/')}/health", headers={"api_key": gemi_key} if gemi_key else {}, timeout=20)
                r.raise_for_status()
                st.success("OK: /health")
            except Exception as e:
                st.error(f"Σφάλμα: {e}")
    with cta2:
        if st.button("🧪 Test /companies"):
            try:
                # API θέλει τουλάχιστον 1 κριτήριο: βάζουμε name=AAA (minLength 3), size=1
                params = {"name": "AAA", "resultsOffset": 0, "resultsSize": 1}
                r = requests.get(f"{gemi_base.rstrip('/')}/companies", params=params,
                                 headers={"api_key": gemi_key} if gemi_key else {}, timeout=40)
                r.raise_for_status()
                st.success("OK: Το endpoint απάντησε.")
            except Exception as e:
                st.error(f"Σφάλμα: {e}")

# ---------- Small HTTP helpers ----------
TIMEOUT = 40

def _hdr():
    if not gemi_key:
        return {}
    return {"api_key": gemi_key, "Accept": "application/json"}

def _base():
    return gemi_base.rstrip("/")

def _http_get(url, params=None, headers=None, timeout=TIMEOUT):
    h = {}
    if headers:
        h.update(headers)
    r = requests.get(url, params=params, headers=h, timeout=timeout)
    r.raise_for_status()
    return r

# ---------- GEMI: Metadata ----------
@st.cache_data(ttl=3600, show_spinner=False)
def gemi_metadata_prefectures():
    # /metadata/prefectures -> [{"id": "17","descr":"ΧΑΝΙΩΝ"}, ...]
    url = f"{_base()}/metadata/prefectures"
    js = _http_get(url, headers=_hdr()).json()
    return js if isinstance(js, list) else []

@st.cache_data(ttl=3600, show_spinner=False)
def gemi_metadata_municipalities():
    # /metadata/municipalities -> [{"id":"61324","prefectureId":"17","descr":"ΧΑΝΙΩΝ"}...]
    url = f"{_base()}/metadata/municipalities"
    js = _http_get(url, headers=_hdr()).json()
    return js if isinstance(js, list) else []

@st.cache_data(ttl=3600, show_spinner=False)
def gemi_metadata_statuses():
    # /metadata/companyStatuses -> [{"id":3,"descr":"Ενεργή", "isActive":true}, ...]
    url = f"{_base()}/metadata/companyStatuses"
    js = _http_get(url, headers=_hdr()).json()
    return js if isinstance(js, list) else []

@st.cache_data(ttl=3600, show_spinner=False)
def gemi_metadata_activities():
    # /metadata/activities -> [{"id":"47.91.21.02","descr":"..."}, ...]
    url = f"{_base()}/metadata/activities"
    js = _http_get(url, headers=_hdr()).json()
    return js if isinstance(js, list) else []

# ---------- GEMI: Companies search (GET /companies) ----------
def _join_vals(vals):
    if not vals:
        return None
    return ",".join([str(x) for x in vals if str(x).strip() != ""])

def gemi_companies_search(*, name=None, prefectures=None, municipalities=None,
                          statuses=None, is_active=None, activities=None,
                          offset=0, size=200, sort="+arGemi"):
    """
    Επιστρέφει ένα dict όπως του Swagger (searchMetadata + searchResults).
    """
    if size < 1: size = 1
    if size > 200: size = 200

    # API: απαιτείται τουλάχιστον ένα κριτήριο
    if not any([name, prefectures, municipalities, statuses, is_active, activities]):
        raise ValueError("Το API απαιτεί τουλάχιστον 1 κριτήριο (name, prefectures, municipalities, statuses, isActive, activities).")

    params = {
        "resultsOffset": int(offset),
        "resultsSize": int(size),
        "resultsSortBy": sort,
    }
    if name and len(str(name)) >= 3:
        params["name"] = name
    if prefectures:
        params["prefectures"] = _join_vals(prefectures)
    if municipalities:
        params["municipalities"] = _join_vals(municipalities)
    if statuses:
        params["statuses"] = _join_vals(statuses)
    if activities:
        params["activities"] = _join_vals(activities)
    if is_active is not None:
        params["isActive"] = bool(is_active)

    url = f"{_base()}/companies"
    r = _http_get(url, params=params, headers=_hdr(), timeout=TIMEOUT)
    return r.json()

def gemi_companies_all(*, name=None, prefectures=None, municipalities=None,
                       statuses=None, is_active=None, activities=None,
                       page_size=200, max_pages=200, progress_cb=None, sleep_sec=0.0):
    """
    Κατεβάζει με pagination ΟΛΑ τα αποτελέσματα (μέχρι max_pages).
    """
    items = []
    for p in range(max_pages):
        offset = p * page_size
        js = gemi_companies_search(
            name=name, prefectures=prefectures, municipalities=municipalities,
            statuses=statuses, is_active=is_active, activities=activities,
            offset=offset, size=page_size
        )
        meta = js.get("searchMetadata") or {}
        arr = js.get("searchResults") or []
        items.extend(arr)
        if progress_cb:
            tot = meta.get("totalCount")
            progress_cb(p+1, len(items), tot)

        # stop conditions
        if not arr:
            break
        sz = meta.get("resultsSize")
        if isinstance(sz, int) and sz < page_size:
            break
        tot = meta.get("totalCount")
        if isinstance(tot, int) and len(items) >= tot:
            break
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return items

# ---------- GEMI: Company documents ----------
def company_documents(ar_gemi):
    """Επιστρέφει ;-joined URLs εγγράφων για μια εταιρεία (arGemi)."""
    try:
        ar = int(str(ar_gemi).strip())
    except Exception:
        return ""
    url = f"{_base()}/companies/{ar}/documents"
    try:
        js = _http_get(url, headers=_hdr(), timeout=TIMEOUT).json()
    except Exception:
        return ""
    urls = []
    for pub in js.get("publication", []) or []:
        u = pub.get("url")
        if u: urls.append(u)
    for dec in js.get("decision", []) or []:
        u = dec.get("assemblyDecisionUrl")
        if u: urls.append(u)
    return ";".join(urls)

def _company_url_from_ar(ar_gemi: object, base: str) -> str:
    if ar_gemi is None:
        return ""
    try:
        ar = int(str(ar_gemi).strip())
        return f"{base.rstrip('/')}/companies/{ar}"
    except Exception:
        return ""

def _company_docs_url_from_ar(ar_gemi: object, base: str) -> str:
    if ar_gemi is None:
        return ""
    try:
        ar = int(str(ar_gemi).strip())
        return f"{base.rstrip('/')}/companies/{ar}/documents"
    except Exception:
        return ""

# ---------- Normalizers ----------
def companies_to_df(results: list[dict]) -> pd.DataFrame:
    rows = []
    for it in results or []:
        status = it.get("status") or {}
        pref   = it.get("prefecture") or {}
        muni   = it.get("municipality") or {}
        legal  = it.get("legalType") or {}
        acts   = it.get("activities") or []

        # ΚΑΔ ids + περιγραφές
        kad_ids = []
        kad_descs = []
        for a in acts:
            act = a.get("activity") or {}
            if act.get("id"): kad_ids.append(str(act.get("id")))
            if act.get("descr"): kad_descs.append(str(act.get("descr")))
        kad_join = ";".join(kad_ids)
        kad_desc_join = ";".join(kad_descs)

        rows.append({
            "arGemi": it.get("arGemi"),
            "afm": it.get("afm"),
            "coNameEl": it.get("coNameEl"),
            "incorporationDate": it.get("incorporationDate"),
            "status_id": status.get("id"),
            "status_descr": status.get("descr"),
            "isActive": it.get("autoRegistered", True),  # το API δίνει isActive στο status list. Εδώ κρατάμε autoRegistered ως ένδειξη πληρότητας.
            "prefecture_id": pref.get("id"),
            "prefecture_descr": pref.get("descr"),
            "municipality_id": muni.get("id"),
            "municipality_descr": muni.get("descr"),
            "city": it.get("city"),
            "street": it.get("street"),
            "streetNumber": it.get("streetNumber"),
            "zipCode": it.get("zipCode"),
            "email": it.get("email"),
            "url": it.get("url"),
            "legalType_id": legal.get("id"),
            "legalType_descr": legal.get("descr"),
            "kad_codes": kad_join,
            "kad_descriptions": kad_desc_join,
            # Συγκεντρωτική διεύθυνση (για FTTH)
            "name": it.get("coNameEl"),
            "address": " ".join([str(x) for x in [it.get("street"), it.get("streetNumber")] if x]).strip(),
            "postal_code": it.get("zipCode"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df

def _to_excel_bytes(df: pd.DataFrame):
    output = io.BytesIO()
    safe = df.copy()
    if safe is None or safe.empty:
        safe = pd.DataFrame([{"info": "no data"}])
    safe.columns = [str(c) for c in safe.columns]
    for c in safe.columns:
        safe[c] = safe[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
    with pd.ExcelWriter(output, engine="openpyxl") as w:
        safe.to_excel(w, index=False)
    output.seek(0)
    return output

# ---------- FTTH Helpers ----------
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
    lat_col = _find_col(df, ["latitude", "lat", "πλατος", "γεωγραφικο πλατος", "φ"])
    lon_col = _find_col(df, ["longitude", "lon", "long", "μηκος", "γεωγραφικο μηκος", "λ"])
    if not lat_col or not lon_col:
        raise ValueError("Δεν βρέθηκαν στήλες latitude/longitude (δοκιμάστηκαν και ελληνικά: Πλάτος/Μήκος).")
    out = df[[lat_col, lon_col]].rename(columns={lat_col: "latitude", lon_col: "longitude"}).copy()
    out["latitude"]  = pd.to_numeric(out["latitude"].astype(str).str.replace(",", "."), errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"].astype(str).str.replace(",", "."), errors="coerce")
    out = out.dropna(subset=["latitude","longitude"])
    return out

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

# ========== Tabs ==========
tab1, tab2 = st.tabs(["🗺️ FTTH Finder", "🏛️ ΓΕΜΗ Downloader"])

# ------------------------------------------------------------------------------------
# TAB 2: ΓΕΜΗ DOWNLOADER
# ------------------------------------------------------------------------------------
with tab2:
    st.subheader("Αναζήτηση & Εξαγωγή από ΓΕΜΗ")

    if not gemi_key:
        st.warning("🔑 Βάλε API Key στο sidebar (ΓΕΜΗ API).")
    else:
        # Φόρτωση metadata (με caching)
        meta_cols = st.columns([1,1,1,1])
        with meta_cols[0]:
            reload_meta = st.button("↻ Ανανέωση λιστών (metadata)")
        if reload_meta:
            gemi_metadata_prefectures.clear()
            gemi_metadata_municipalities.clear()
            gemi_metadata_statuses.clear()
            gemi_metadata_activities.clear()

        try:
            prefectures = gemi_metadata_prefectures()
            municipalities = gemi_metadata_municipalities()
            statuses = gemi_metadata_statuses()
            activities = gemi_metadata_activities()
        except Exception as e:
            st.error(f"Σφάλμα φόρτωσης λιστών: {e}")
            prefectures, municipalities, statuses, activities = [], [], [], []

        # maps
        pref_map = {f'{p.get("descr","")} ({p.get("id")})': str(p.get("id")) for p in prefectures if p.get("id")}
        muni_map = {f'{m.get("descr","")} ({m.get("id")}) [Νομός {m.get("prefectureId","?")}]': str(m.get("id")) for m in municipalities if m.get("id")}
        stat_map = {f'{s.get("descr","")} ({s.get("id")})': int(s.get("id")) for s in statuses if isinstance(s.get("id"), (int, float))}
        act_map  = {f'{a.get("descr","")} ({a.get("id")})': str(a.get("id")) for a in activities if a.get("id")}

        # Filters UI
        st.markdown("#### Φίλτρα")
        c1, c2, c3 = st.columns(3)
        with c1:
            sel_prefs = st.multiselect("Νομοί (prefectures)", options=list(pref_map.keys()))
        with c2:
            # Αν έχουν επιλεχθεί νομοί, φιλτράρουμε τους δήμους που ανήκουν εκεί
            if sel_prefs:
                chosen_ids = {pref_map[k] for k in sel_prefs}
                muni_options = [k for k, mid in muni_map.items() if any(p in k for p in chosen_ids)]
            else:
                muni_options = list(muni_map.keys())
            sel_munis = st.multiselect("Δήμοι (municipalities)", options=muni_options)
        with c3:
            sel_stats = st.multiselect("Καταστάσεις", options=list(stat_map.keys()))

        c4, c5, c6 = st.columns(3)
        with c4:
            sel_acts = st.multiselect("Δραστηριότητες (ΚΑΔ)", options=list(act_map.keys()))
        with c5:
            name_part = st.text_input("Λεκτικό στην επωνυμία (min 3 χαρακ.)", "")
        with c6:
            is_active = st.selectbox("Ενεργή;", ["—", "Ναι", "Όχι"], index=0)

        st.markdown("#### Επιλογές εξαγωγής")
        c7, c8, c9 = st.columns(3)
        with c7:
            page_sz = st.number_input("resultsSize (1–200)", 1, 200, 200)
        with c8:
            max_pages = st.number_input("Μέγιστες σελίδες", 1, 2000, 200)
        with c9:
            include_docs = st.checkbox("📎 Συμπερίλαβε URLs εγγράφων")

        c10, c11 = st.columns(2)
        with c10:
            verify_emails = st.checkbox("✅ Email verification (syntax+DNS)")
            if verify_emails and not EMAIL_LIB_OK:
                st.info("Για έλεγχο email, πρόσθεσε στο requirements: `email-validator`.")
        with c11:
            pass

        # Χαρτογράφηση επιλογών σε ids
        q_pref_ids = [pref_map[k] for k in sel_prefs] if sel_prefs else None
        q_muni_ids = [muni_map[k] for k in sel_munis] if sel_munis else None
        q_stat_ids = [stat_map[k] for k in sel_stats] if sel_stats else None
        q_act_ids  = [act_map[k] for k in sel_acts] if sel_acts else None
        q_name = name_part if (name_part and len(name_part) >= 3) else None
        q_active = True if is_active == "Ναι" else (False if is_active == "Όχι" else None)

        # Κουμπιά
        b1, b2 = st.columns(2)
        with b1:
            do_preview = st.button("🔎 Προεπισκόπηση (1 σελίδα)")
        with b2:
            do_export = st.button("⬇️ Εξαγωγή σε Excel (όλα με pagination)")

        gemi_df = None

        # --- Preview ---
        if do_preview:
            try:
                js = gemi_companies_search(
                    name=q_name, prefectures=q_pref_ids, municipalities=q_muni_ids,
                    statuses=q_stat_ids, is_active=q_active, activities=q_act_ids,
                    offset=0, size=page_sz
                )
                results = js.get("searchResults") or []
                gemi_df = companies_to_df(results)

                # links
                if not gemi_df.empty:
                    base = _base()
                    if "arGemi" in gemi_df.columns:
                        gemi_df["gemi_company_url"]   = gemi_df["arGemi"].apply(lambda v: _company_url_from_ar(v, base))
                        gemi_df["gemi_documents_url"] = gemi_df["arGemi"].apply(lambda v: _company_docs_url_from_ar(v, base))

                # email verification (προαιρετικό)
                if verify_emails and EMAIL_LIB_OK and not gemi_df.empty and "email" in gemi_df:
                    val_ok, val_norm = [], []
                    for x in gemi_df["email"].fillna(""):
                        try:
                            v = validate_email(str(x), check_deliverability=True)
                            val_ok.append(True);  val_norm.append(v.email)
                        except EmailNotValidError:
                            val_ok.append(False); val_norm.append("")
                    gemi_df["email_valid"] = val_ok
                    gemi_df["email_normalized"] = val_norm

                if gemi_df.empty:
                    st.warning("Δεν βρέθηκαν αποτελέσματα για τα φίλτρα.")
                else:
                    meta = js.get("searchMetadata") or {}
                    st.success(f"Βρέθηκαν {len(gemi_df)} εγγραφές (σελίδα). Σύνολο: {meta.get('totalCount','?')}.")
                    st.dataframe(gemi_df, use_container_width=True)
                    st.download_button("⬇️ Κατέβασμα προεπισκόπησης (Excel)", _to_excel_bytes(gemi_df), file_name="gemi_preview.xlsx")

            except ValueError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"Σφάλμα αναζήτησης: {e}")

        # --- Export (όλα) ---
        if do_export:
            try:
                prog = st.progress(0.0, text="Ξεκίνησε η λήψη…")
                def _cb(pages_done, got, tot):
                    txt = f"Σελίδες: {pages_done} • Συγκεντρωτικά: {got}"
                    if isinstance(tot, int):
                        txt += f" / {tot}"
                        frac = min(0.99, got / max(1, tot))
                    else:
                        frac = 0.0
                    prog.progress(frac, text=txt)

                items = gemi_companies_all(
                    name=q_name, prefectures=q_pref_ids, municipalities=q_muni_ids,
                    statuses=q_stat_ids, is_active=q_active, activities=q_act_ids,
                    page_size=page_sz, max_pages=max_pages, progress_cb=_cb, sleep_sec=0.0
                )
                df = companies_to_df(items)

                # links
                if not df.empty and "arGemi" in df:
                    base = _base()
                    df["gemi_company_url"]   = df["arGemi"].apply(lambda v: _company_url_from_ar(v, base))
                    df["gemi_documents_url"] = df["arGemi"].apply(lambda v: _company_docs_url_from_ar(v, base))

                # email verification (προαιρετικό)
                if verify_emails and EMAIL_LIB_OK and not df.empty and "email" in df:
                    st.info("Έλεγχος emails… (syntax + DNS)")
                    val_ok, val_norm = [], []
                    for x in df["email"].fillna(""):
                        try:
                            v = validate_email(str(x), check_deliverability=True)
                            val_ok.append(True);  val_norm.append(v.email)
                        except EmailNotValidError:
                            val_ok.append(False); val_norm.append("")
                    df["email_valid"] = val_ok
                    df["email_normalized"] = val_norm

                # documents enrichment (προαιρετικό)
                if include_docs and not df.empty and "arGemi" in df:
                    st.info("📎 Λήψη URLs εγγράφων για κάθε επιχείρηση (σεβασμός ορίου 8 req/min)…")
                    df["documents_urls"] = ""
                    # απλός rate limiter 8/λεπτό
                    calls = []
                    for i, ar in df["arGemi"].items():
                        now = time.time()
                        calls = [t for t in calls if now - t < 60]
                        if len(calls) >= 8:
                            to_sleep = 60 - (now - calls[0])
                            if to_sleep > 0:
                                time.sleep(to_sleep)
                        try:
                            df.at[i, "documents_urls"] = company_documents(ar)
                        except Exception:
                            df.at[i, "documents_urls"] = ""
                        calls.append(time.time())

                prog.progress(1.0, text="Ολοκληρώθηκε ✔")

                if df.empty:
                    st.warning("Δεν βρέθηκαν εγγραφές.")
                else:
                    st.success(f"Έτοιμο: {len(df)} εγγραφές.")
                    st.dataframe(df.head(50), use_container_width=True)
                    st.download_button("⬇️ Excel – Επιχειρήσεις (με φίλτρα)", _to_excel_bytes(df), file_name="gemi_filtered.xlsx")

                # Επιπλέον: μπορείς να χρησιμοποιήσεις το df ως πηγή για FTTH
                st.session_state["gemi_last_df"] = df.copy()

            except ValueError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"Σφάλμα αναζήτησης/εξαγωγής: {e}")

# ------------------------------------------------------------------------------------
# TAB 1: FTTH FINDER
# ------------------------------------------------------------------------------------
with tab1:
    st.subheader("Geocoding & Matching")

    # Αρχεία
    st.markdown("#### 📥 Αρχεία")
    if biz_source == "Upload Excel/CSV":
        biz_file = st.file_uploader("Excel/CSV Επιχειρήσεων", type=["xlsx", "csv"])
        biz_df = load_table(biz_file) if biz_file else None
    else:
        st.caption("Πηγή: δεδομένα από το tab «ΓΕΜΗ Downloader» (τελευταία λήψη)")
        biz_df = st.session_state.get("gemi_last_df")

    ftth_file = st.file_uploader("FTTH σημεία Nova (Excel/CSV)", type=["xlsx", "csv"])
    prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά) – στήλες: Address, Latitude, Longitude", type=["xlsx", "csv"])

    # Φορτώνουμε FTTH
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

    # Start
    start = st.button("🚀 Ξεκίνα geocoding & matching")

    if start and biz_df is not None and ftth_df is not None:
        work = biz_df.copy()

        # Επιλογή πιθανών στηλών διεύθυνσης/πόλης
        addr_series = pick_first_series(work, ["address", "site.company_insights.address", "διεύθυνση", "οδός", "διευθυνση"])
        city_series = pick_first_series(work, ["city", "site.company_insights.city", "πόλη"])

        # Συνθέτουμε Address για geocoding
        base_addr = addr_series.astype(str).str.strip()
        from_input_city = city_series.astype(str).str.strip()
        work["Address"] = (base_addr + (", " + from_input_city).where(from_input_city.ne(""), "")).str.replace(r"\s+", " ", regex=True)

        # Αφαίρεση εντελώς κενών
        work = work[work["Address"].str.len() > 3].copy()

        # cache από prev_df (αν δόθηκε)
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

        # Προετοιμασία στηλών
        work["Latitude"] = pd.NA
        work["Longitude"] = pd.NA

        total = len(work)
        progress = st.progress(0, text=f"0 / {total}")
        errs = 0

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

        # Καθάρισμα
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

        # Exports
        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button("⬇️ Geocoded διευθύνσεις", _to_excel_bytes(merged[["Address","Latitude","Longitude"]]), file_name="geocoded_addresses.xlsx")
        with c2:
            st.download_button("⬇️ Αποτελέσματα Matching", _to_excel_bytes(result_df), file_name="ftth_matching_results.xlsx")
        with c3:
            st.download_button("⬇️ Όλα τα δεδομένα (merged)", _to_excel_bytes(merged), file_name="merged_with_geocoded.xlsx")

    elif start and (biz_df is None or ftth_df is None):
        st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
    else:
        st.info("📄 Ανέβασε FTTH, διάλεξε πηγή επιχειρήσεων (Upload ή ΓΕΜΗ) και πάτα «🚀 Ξεκίνα».")
