# ftth_scraper_nova_streamlit.py
# -*- coding: utf-8 -*-

import io
import time
import re
import requests
import pandas as pd
import streamlit as st
from geopy.distance import geodesic

# ============== App setup ==============
st.set_page_config(page_title="FTTH + ΓΕΜΗ (v7)", layout="wide")
st.title("📡 FTTH + ΓΕΜΗ Open Data – v7")

# ---------- Optional cache ----------
try:
    import requests_cache
    requests_cache.install_cache("gemi_cache", backend="sqlite", expire_after=60 * 60 * 24)  # 1 day
    CACHE_OK = True
except Exception:
    CACHE_OK = False

# ============== Constants / Helpers ==============
GEMI_BASE = "https://opendata-api.businessportal.gr/api/opendata/v1"
GEMI_HEADER_NAME = "api_key"
TIMEOUT = 40

EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", re.IGNORECASE)

def _headers(api_key: str):
    return {GEMI_HEADER_NAME: api_key, "Accept": "application/json"}

def _sleep_for_rate(resp: requests.Response, fallback_sec: float = 8.0):
    # Respect Retry-After if present, else fallback ~8s (8 req/min)
    try:
        ra = resp.headers.get("Retry-After")
        if ra:
            sec = float(ra)
        else:
            sec = fallback_sec
    except Exception:
        sec = fallback_sec
    time.sleep(sec)

def _get_with_retry(url, params, headers, timeout=TIMEOUT, max_retries=3):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r
            # handle throttling / gateway busy
            if r.status_code in (429, 503):
                if attempt < max_retries:
                    _sleep_for_rate(r)
                    continue
            r.raise_for_status()
        except requests.RequestException as e:
            last_err = str(e)
            if attempt < max_retries:
                time.sleep(2.0)
                continue
            raise
    raise RuntimeError(last_err or "Unknown request error")

@st.cache_data(show_spinner=False)
def fetch_metadata(api_key: str, endpoint: str) -> list:
    """GET /metadata/<endpoint>"""
    url = f"{GEMI_BASE}/metadata/{endpoint}"
    r = _get_with_retry(url, params=None, headers=_headers(api_key))
    js = r.json()
    # all metadata responses are arrays
    if isinstance(js, list):
        return js
    return []

def _join_vals(vals):
    if not vals:
        return None
    return ",".join(str(v).strip() for v in vals if str(v).strip())

def _bool_str(val):
    return "true" if bool(val) else "false"

def _email_valid(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    return EMAIL_RX.match(s.strip()) is not None

def _to_excel_bytes(df: pd.DataFrame, sheet_name="Sheet1"):
    buf = io.BytesIO()
    if df is None or df.empty:
        df = pd.DataFrame([{"info": "no data"}])
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    for c in df.columns:
        df[c] = df[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf

def companies_to_df(items: list[dict]) -> pd.DataFrame:
    rows = []
    for it in items or []:
        # activity list → "id:descr;id:descr;..."
        acts = it.get("activities") or []
        act_join = ";".join(
            f"{(a.get('activity') or {}).get('id','')}:{(a.get('activity') or {}).get('descr','')}".strip(":")
            for a in acts if isinstance(a, dict)
        )

        pref = it.get("prefecture") or {}
        muni = it.get("municipality") or {}
        stat = it.get("status") or {}
        ltyp = it.get("legalType") or {}

        ar_gemi = it.get("arGemi")
        rows.append({
            "arGemi": ar_gemi,
            "afm": it.get("afm"),
            "name_el": it.get("coNameEl"),
            "status": stat.get("descr"),
            "legal_type": ltyp.get("descr"),
            "incorporationDate": it.get("incorporationDate"),
            "prefecture_id": pref.get("id"),
            "prefecture": pref.get("descr"),
            "municipality_id": muni.get("id"),
            "municipality": muni.get("descr"),
            # address / contact
            "city": it.get("city"),
            "street": it.get("street"),
            "streetNumber": it.get("streetNumber"),
            "zipCode": it.get("zipCode"),
            "email": it.get("email"),
            "email_valid": _email_valid(it.get("email")),
            "url": it.get("url"),
            # activities
            "activities": act_join,
            # links (API)
            "gemi_api_url": f"{GEMI_BASE}/companies/{ar_gemi}" if ar_gemi else "",
            "gemi_docs_url": f"{GEMI_BASE}/companies/{ar_gemi}/documents" if ar_gemi else "",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df

def companies_search_page(api_key: str, *,
                          name=None,
                          prefectures=None, municipalities=None,
                          statuses=None, activities=None,
                          is_active=None,
                          offset=0, size=50, sort="+arGemi"):
    if size < 1:
        size = 1
    if size > 200:
        size = 200
    # At least 1 criterion is required by API
    if not any([name, prefectures, municipalities, statuses, activities, is_active is not None]):
        raise ValueError("Το API απαιτεί τουλάχιστον 1 κριτήριο (name / prefectures / municipalities / statuses / activities / isActive).")

    params = {
        "resultsOffset": int(offset),
        "resultsSize": int(size),
        "resultsSortBy": sort,
    }
    if name and len(str(name)) >= 3:
        params["name"] = str(name)
    if prefectures:
        params["prefectures"] = _join_vals(prefectures)
    if municipalities:
        params["municipalities"] = _join_vals(municipalities)
    if statuses:
        params["statuses"] = _join_vals(statuses)
    if activities:
        params["activities"] = _join_vals(activities)
    if is_active is not None:
        params["isActive"] = _bool_str(is_active)

    url = f"{GEMI_BASE}/companies"
    r = _get_with_retry(url, params=params, headers=_headers(api_key))
    js = r.json() or {}
    meta = js.get("searchMetadata") or {}
    results = js.get("searchResults") or []
    return meta, results

def companies_search_all(api_key: str, *,
                         name=None,
                         prefectures=None, municipalities=None,
                         statuses=None, activities=None,
                         is_active=None,
                         size=200, throttle_sec=8.0, max_pages=9999):
    # First page: get totalCount
    meta, first = companies_search_page(
        api_key,
        name=name,
        prefectures=prefectures, municipalities=municipalities,
        statuses=statuses, activities=activities,
        is_active=is_active,
        offset=0, size=size
    )
    total = int(meta.get("totalCount") or 0)
    items = list(first)
    fetched = len(first)
    # Progress info
    progress = st.progress(0.0, text=f"0 / {total}…")
    if total == 0:
        progress.progress(1.0, text="ΟΚ (0)")
        return items
    # More pages
    page_no = 1
    while fetched < total and page_no < max_pages:
        offset = fetched
        # throttle for rate limit (8/min)
        time.sleep(throttle_sec)
        _, page_items = companies_search_page(
            api_key,
            name=name,
            prefectures=prefectures, municipalities=municipalities,
            statuses=statuses, activities=activities,
            is_active=is_active,
            offset=offset, size=size
        )
        if not page_items:
            break
        items.extend(page_items)
        fetched += len(page_items)
        page_no += 1
        progress.progress(min(1.0, fetched / max(1, total)), text=f"{fetched} / {total}…")
    progress.progress(1.0, text=f"{min(fetched,total)} / {total} έτοιμα")
    return items

# ============== Tabs ==============
tab_gemi, tab_ftth = st.tabs(["🏛️ ΓΕΜΗ Export", "🗺️ FTTH Matching"])

# ============== TAB 1: ΓΕΜΗ Export ==============
with tab_gemi:
    st.subheader("Ρυθμίσεις API")
    c1, c2, c3 = st.columns([2,1,1])
    with c1:
        gemi_key = st.text_input("API Key (header: api_key)", type="password")
    with c2:
        throttle_sec = st.number_input("Throttle (sec) για σελιδοποίηση", min_value=0.0, max_value=30.0, value=8.0, step=0.5)
    with c3:
        st.caption("Max 8 req/min")

    cA, cB, cC = st.columns(3)
    with cA:
        if st.button("🧪 Test /health"):
            try:
                r = _get_with_retry(f"{GEMI_BASE}/health", params=None, headers=_headers(gemi_key))
                st.success("OK: Το gateway λειτουργεί.")
                st.code(r.text[:400], language="json")
            except Exception as e:
                st.error(f"Health error: {e}")
    with cB:
        if st.button("🧪 Test /metadata/prefectures"):
            try:
                data = fetch_metadata(gemi_key, "prefectures")
                st.success(f"OK: {len(data)} prefectures")
                st.json(data[:3])
            except Exception as e:
                st.error(f"Prefectures error: {e}")
    with cC:
        if st.button("🧪 Test /companies (με name='ΑΕ')"):
            try:
                meta, res = companies_search_page(gemi_key, name="ΑΕ", size=5)
                st.success(f"OK: {len(res)} αποτελέσματα")
                st.json(meta)
            except Exception as e:
                st.error(f"Companies error: {e}")

    st.markdown("---")
    st.subheader("Φίλτρα αναζήτησης")

    # --- Load metadata (cached) ---
    pref_data = []
    muni_data = []
    stat_data = []
    kad_data = []
    if gemi_key:
        try:
            pref_data = fetch_metadata(gemi_key, "prefectures")  # [{id:str, descr:str}]
        except Exception as e:
            st.error(f"Σφάλμα φόρτωσης Prefectures: {e}")
        try:
            muni_data = fetch_metadata(gemi_key, "municipalities")  # [{id:str, prefectureId:str, descr:str}]
        except Exception as e:
            st.error(f"Σφάλμα φόρτωσης Municipalities: {e}")
        try:
            stat_data = fetch_metadata(gemi_key, "companyStatuses")  # [{id:int, descr:str, isActive:bool}]
        except Exception as e:
            st.error(f"Σφάλμα φόρτωσης Statuses: {e}")
        try:
            kad_data = fetch_metadata(gemi_key, "activities")  # [{id:str, descr:str}]
        except Exception as e:
            st.warning(f"Σφάλμα φόρτωσης ΚΑΔ (προαιρετικό): {e}")

    # --- Build maps ---
    pref_map = {p.get("descr"): p.get("id") for p in pref_data if p.get("id") and p.get("descr")}
    pref_names = sorted(pref_map.keys())

    # Municipality filtered by selected prefectures
    sel_pref_names = st.multiselect("Νομοί (Prefectures)", options=pref_names, default=[])
    sel_pref_ids = [pref_map[n] for n in sel_pref_names]

    # filter municipalities by prefectureId if we selected prefectures
    muni_all = [m for m in muni_data if m.get("id") and m.get("descr")]
    if sel_pref_ids:
        muni_all = [m for m in muni_all if str(m.get("prefectureId")) in set(map(str, sel_pref_ids))]
    muni_map = {m.get("descr"): m.get("id") for m in muni_all}
    muni_names = sorted(muni_map.keys())
    sel_muni_names = st.multiselect("Δήμοι (Municipalities)", options=muni_names, default=[])
    sel_muni_ids = [muni_map[n] for n in sel_muni_names]

    # Statuses
    stat_map = {f"{s.get('descr')} ({'active' if s.get('isActive') else 'inactive'})": s.get("id")
                for s in stat_data if s.get("id") is not None}
    stat_names = sorted(stat_map.keys())
    sel_stat_names = st.multiselect("Καταστάσεις", options=stat_names, default=[])
    sel_stat_ids = [stat_map[n] for n in sel_stat_names]

    # Activities (ΚΑΔ) – optional (can be huge)
    kad_map = {f"{k.get('id')} — {k.get('descr')}": k.get("id") for k in kad_data if k.get("id") and k.get("descr")}
    kad_names = sorted(kad_map.keys())[:4000]  # guard UI
    sel_kad_names = st.multiselect("Δραστηριότητες (ΚΑΔ) – προαιρετικό", options=kad_names, default=[])
    sel_kad_ids = [kad_map[n] for n in sel_kad_names]

    # Name / isActive
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        name_part = st.text_input("Όνομα/Επωνυμία περιέχει (>=3 χαρακτήρες για χρήση)", value="")
    with c2:
        is_active_only = st.checkbox("Μόνο ενεργές (isActive=true)", value=False)
    with c3:
        page_size = st.number_input("Προβολή (μέχρι 200)", min_value=1, max_value=200, value=50, step=1)

    colA, colB = st.columns(2)
    with colA:
        do_preview = st.button("🔎 Προβολή (πρώτη σελίδα)")
    with colB:
        do_export_all = st.button("⬇️ Εξαγωγή ΟΛΩΝ (Excel)")

    # --- Execute actions ---
    gemi_preview_df = pd.DataFrame()
    if do_preview:
        try:
            meta, results = companies_search_page(
                gemi_key,
                name=name_part or None,
                prefectures=sel_pref_ids or None,
                municipalities=sel_muni_ids or None,
                statuses=sel_stat_ids or None,
                activities=sel_kad_ids or None,
                is_active=True if is_active_only else None,
                offset=0, size=int(page_size)
            )
            st.caption(f"Σύνολο: {int(meta.get('totalCount') or 0)} • Offset: {meta.get('resultsOffset')} • Size: {meta.get('resultsSize')}")
            gemi_preview_df = companies_to_df(results)
            if gemi_preview_df.empty:
                st.warning("Δεν βρέθηκαν αποτελέσματα.")
            else:
                st.dataframe(gemi_preview_df, use_container_width=True, height=420)
                st.download_button(
                    "⬇️ Κατέβασμα (Excel - preview)",
                    _to_excel_bytes(gemi_preview_df, sheet_name="preview"),
                    file_name="gemi_preview.xlsx"
                )
        except Exception as e:
            st.error(f"Σφάλμα αναζήτησης: {e}")

    if do_export_all:
        try:
            with st.spinner("Εξαγωγή όλων των αποτελεσμάτων με σελιδοποίηση…"):
                all_items = companies_search_all(
                    gemi_key,
                    name=name_part or None,
                    prefectures=sel_pref_ids or None,
                    municipalities=sel_muni_ids or None,
                    statuses=sel_stat_ids or None,
                    activities=sel_kad_ids or None,
                    is_active=True if is_active_only else None,
                    size=200, throttle_sec=float(throttle_sec)
                )
                export_df = companies_to_df(all_items)
                if export_df.empty:
                    st.warning("Δεν βρέθηκαν αποτελέσματα για εξαγωγή.")
                else:
                    st.success(f"Έτοιμο ({len(export_df)} εγγραφές).")
                    st.dataframe(export_df.head(50), use_container_width=True, height=420)
                    st.download_button(
                        "⬇️ Excel – Επιχειρήσεις (όλα τα αποτελέσματα)",
                        _to_excel_bytes(export_df, sheet_name="companies"),
                        file_name="gemi_companies_all.xlsx"
                    )
        except Exception as e:
            st.error(f"Σφάλμα εξαγωγής: {e}")

# ============== TAB 2: FTTH Matching ==============
with tab_ftth:
    st.subheader("FTTH Geocoding & Matching")
    with st.sidebar:
        st.header("Ρυθμίσεις (FTTH)")
        geocoder = st.selectbox("Geocoder", ["Nominatim (δωρεάν)", "Google (API key)"])
        google_key = st.text_input("Google API key", type="password", help="Αν μείνει κενό, χρησιμοποιείται Nominatim.")
        country = st.text_input("Country code", "gr")
        lang = st.text_input("Language", "el")
        throttle = st.slider("Καθυστέρηση (sec) [Nominatim]", 0.5, 2.0, 1.0, 0.5)
        distance_limit = st.number_input("📏 Μέγιστη απόσταση (m)", min_value=1, max_value=500, value=150)

        st.subheader("Πηγή Επιχειρήσεων")
        biz_source = st.radio("Επιλογή", ["Upload Excel/CSV"], index=0)

    st.caption("Ανέβασε τα αρχεία σου παρακάτω")
    c1, c2 = st.columns(2)
    with c1:
        biz_file = st.file_uploader("Excel/CSV Επιχειρήσεων (στήλες: address, city)", type=["xlsx", "csv"], key="biz_up")
    with c2:
        ftth_file = st.file_uploader("FTTH σημεία Nova (Excel/CSV)", type=["xlsx", "csv"], key="ftth_up")
    prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά) – Excel/CSV με στήλες: Address, Latitude, Longitude", type=["xlsx", "csv"])

    def load_table(uploaded):
        if uploaded is None:
            return None
        name = uploaded.name.lower()
        if name.endswith(".csv"):
            return pd.read_csv(uploaded)
        return pd.read_excel(uploaded)

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

    # FTTH load
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

    # Biz source
    biz_df = None
    if biz_source == "Upload Excel/CSV":
        biz_df = load_table(biz_file) if biz_file else None

    # Geocode cache (separate)
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

    start = st.button("🚀 Ξεκίνα geocoding & matching")

    if start and biz_df is not None and ftth_df is not None:
        work = biz_df.copy()

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

        addr_series = pick_first_series(work, ["address", "site.company_insights.address", "διεύθυνση", "οδός", "διευθυνση"])
        city_series = pick_first_series(work, ["city", "site.company_insights.city", "πόλη"])

        base_addr = addr_series.astype(str).str.strip()
        from_input_city = city_series.astype(str).str.strip()
        work["Address"] = (base_addr + (", " + from_input_city).where(from_input_city.ne(""), "")).str.replace(r"\s+", " ", regex=True)

        work = work[work["Address"].str.len() > 3].copy()

        total = len(work)
        progress = st.progress(0, text=f"0 / {total}")
        errs = 0

        # prev geocoded cache
        geo_map = {}
        prev_df = None
        prev_geo_file_up = st.session_state.get("prev_geo_file_up")  # not used, placeholder
        if prev_geo_file is not None:
            prev_df = load_table(prev_geo_file)

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
                lat, lon = geocode_address(addr, geocoder, api_key=google_key, cc=country, lang=lang, throttle_sec=float(throttle))
                if lat is not None and lon is not None:
                    geo_map[addr] = (lat, lon)
                else:
                    errs += 1
                    lat, lon = (None, None)

            work.at[idx, "Latitude"]  = lat
            work.at[idx, "Longitude"] = lon
            progress.progress(i / max(1, total), text=f"{i} / {total} γεωκωδικοποιημένα...")

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
            st.dataframe(result_df, use_container_width=True, height=420)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.download_button("⬇️ Geocoded διευθύνσεις (γραμμή-γραμμή)",
                               _to_excel_bytes(merged[["Address","Latitude","Longitude"]], sheet_name="geocoded"),
                               file_name="geocoded_addresses.xlsx")
        with c2:
            st.download_button("⬇️ Αποτελέσματα Matching",
                               _to_excel_bytes(result_df, sheet_name="matching"),
                               file_name="ftth_matching_results.xlsx")
        with c3:
            st.download_button("⬇️ Όλα τα δεδομένα (merged)",
                               _to_excel_bytes(merged, sheet_name="merged"),
                               file_name="merged_with_geocoded.xlsx")

    elif start and (biz_df is None or ftth_df is None):
        st.error("❌ Ανέβασε και τα δύο αρχεία: Επιχειρήσεις & FTTH σημεία.")
    else:
        st.info("📄 Ανέβασε FTTH & Επιχειρήσεις και πάτα «🚀 Ξεκίνα».")