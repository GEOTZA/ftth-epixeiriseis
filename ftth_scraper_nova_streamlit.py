# ftth_scraper_nova_streamlit.py
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import requests
import time
import io
import re
from geopy.distance import geodesic

# -------- Optional cache for geocoding --------
try:
    import requests_cache
    CACHE_OK = True
except Exception:
    CACHE_OK = False

# =========================
# App setup
# =========================
st.set_page_config(page_title="FTTH + ΓΕΜΗ (2-σε-1)", layout="wide")
st.title("🧭 FTTH Matching + 📥 ΓΕΜΗ Downloader (σε 1 αρχείο)")

# -------------------------
# Global API settings (always visible)
# -------------------------
DEFAULT_BASE = "https://opendata-api.businessportal.gr/api/opendata/v1"
DEFAULT_HEADER = "api_key"

with st.expander("🔌 API Ρυθμίσεις (ΓΕΜΗ)", expanded=True):
    colA, colB, colC = st.columns([2,1,2])
    with colA:
        gemi_base = st.text_input("Base URL", value=st.session_state.get("gemi_base", DEFAULT_BASE),
                                  help="Swagger base: https://opendata-api.businessportal.gr/api/opendata/v1")
    with colB:
        gemi_header = st.text_input("Header name", value=st.session_state.get("gemi_header", DEFAULT_HEADER))
    with colC:
        gemi_key = st.text_input("API Key", type="password", value=st.session_state.get("gemi_key", ""))

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🔧 Χρήση προτεινόμενων (Swagger)"):
            gemi_base = DEFAULT_BASE
            gemi_header = DEFAULT_HEADER
    with c2:
        if st.button("🧪 Test /companies (1 αποτέλεσμα)"):
            try:
                r = requests.get(
                    f"{gemi_base.rstrip('/')}/companies",
                    headers={gemi_header: gemi_key, "Accept": "application/json"},
                    params={"resultsSize": 1, "resultsOffset": 0},
                    timeout=40,
                )
                if r.status_code == 429:
                    st.error("429 Too Many Requests · όριο 8 req/min – δοκίμασε ξανά σε λίγο.")
                else:
                    r.raise_for_status()
                    st.success("OK: Το endpoint απάντησε.")
            except Exception as e:
                st.error(f"Σφάλμα Test /companies: {e}")

    st.session_state.update(gemi_base=gemi_base, gemi_header=gemi_header, gemi_key=gemi_key)

# Helpers for API config
def _hdr():
    return {st.session_state.get("gemi_header", DEFAULT_HEADER): st.session_state.get("gemi_key", ""),
            "Accept": "application/json"}

def _base():
    # Προσοχή: μερικές φορές γίνεται μπέρδεμα με ελληνικό 'ο' -> αντικατάσταση αν χρειαστεί
    return st.session_state.get("gemi_base", DEFAULT_BASE).replace("οpendata", "opendata")

# Generic GET with small backoff for 429
def _http_get(url, *, headers, params=None, timeout=40, max_retries=2):
    last_err = None
    for i in range(max_retries + 1):
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code == 429:
            last_err = "429 Too Many Requests (όριο 8 req/min)"
            if i < max_retries:
                time.sleep(10)
                continue
            raise RuntimeError(last_err)
        if r.status_code >= 400:
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            r.raise_for_status()
        return r
    raise RuntimeError(last_err or "Αποτυχία κλήσης")

# =========================
# Metadata (Swagger /metadata/*), cached
# =========================
@st.cache_data(show_spinner=False, ttl=60*30)
def md_prefectures():
    r = _http_get(f"{_base().rstrip('/')}/metadata/prefectures", headers=_hdr())
    return r.json()

@st.cache_data(show_spinner=False, ttl=60*30)
def md_municipalities():
    r = _http_get(f"{_base().rstrip('/')}/metadata/municipalities", headers=_hdr())
    return r.json()

@st.cache_data(show_spinner=False, ttl=60*30)
def md_statuses():
    r = _http_get(f"{_base().rstrip('/')}/metadata/companyStatuses", headers=_hdr())
    return r.json()

@st.cache_data(show_spinner=False, ttl=60*30)
def md_activities():
    r = _http_get(f"{_base().rstrip('/')}/metadata/activities", headers=_hdr())
    return r.json()

# =========================
# Companies search (/companies)
# =========================
def companies_search(*, name=None, prefectures=None, municipalities=None,
                     statuses=None, activities=None, is_active=None,
                     offset=0, size=200, sort_by="+arGemi"):
    url = f"{_base().rstrip('/')}/companies"
    params = {
        "resultsOffset": int(offset),
        "resultsSize": max(1, min(200, int(size))),
        "resultsSortBy": sort_by,
    }
    if name and len(name.strip()) >= 3:
        params["name"] = name.strip()
    if prefectures:
        params["prefectures"] = ",".join([str(x) for x in prefectures])
    if municipalities:
        params["municipalities"] = ",".join([str(x) for x in municipalities])
    if statuses:
        params["statuses"] = ",".join([str(x) for x in statuses])
    if activities:
        params["activities"] = ",".join([str(x) for x in activities])
    if is_active in ("true", "false"):
        params["isActive"] = is_active

    params = {k: v for k, v in params.items() if v not in (None, "", [])}

    r = _http_get(url, headers=_hdr(), params=params, timeout=40)
    js = r.json()
    results = js.get("searchResults") or []
    meta = js.get("searchMetadata") or {}
    total = meta.get("totalCount")
    try:
        total = int(total) if total is not None else None
    except Exception:
        total = None
    return results, total

def companies_all(*, name=None, prefectures=None, municipalities=None,
                  statuses=None, activities=None, is_active=None,
                  size=200, max_pages=100):
    all_rows = []
    for page in range(max_pages):
        offset = page * size
        rows, total = companies_search(
            name=name, prefectures=prefectures, municipalities=municipalities,
            statuses=statuses, activities=activities, is_active=is_active,
            offset=offset, size=size
        )
        all_rows.extend(rows)
        # respect rate limit a bit
        if page % 6 == 5:
            time.sleep(8)
        if not rows or (total is not None and len(all_rows) >= total):
            break
    return all_rows
import re

EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", re.IGNORECASE)

def _email_valid(email):
    if email is None:
        return False
    return bool(EMAIL_RX.match(str(email).strip()))
# =========================
# Normalize Company -> DataFrame
# =========================
def companies_to_df(items: list[dict]) -> pd.DataFrame:
    rows = []
    for it in items or []:
        acts = it.get("activities") or []

        kad_codes = []
        kad_descrs = []
        kad_full = []

        for a in acts:
            act = a.get("activity") or {}
            act_id = str(act.get("id") or "").strip()
            act_descr = str(act.get("descr") or "").strip()

            if act_id:
                kad_codes.append(act_id)
            if act_descr:
                kad_descrs.append(act_descr)
            if act_id or act_descr:
                kad_full.append(f"{act_id} - {act_descr}".strip(" -"))

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
            "city": it.get("city"),
            "street": it.get("street"),
            "streetNumber": it.get("streetNumber"),
            "zipCode": it.get("zipCode"),
            "email": it.get("email"),
            "email_valid": _email_valid(it.get("email")),
            "url": it.get("url"),

            # ΚΑΔ
            "kad_codes": "; ".join(kad_codes),
            "kad_descriptions": "; ".join(kad_descrs),
            "kad_full": " | ".join(kad_full),

            # συγκεντρωτική διεύθυνση για FTTH
            "name": it.get("coNameEl"),
            "address": " ".join([str(x) for x in [it.get("street"), it.get("streetNumber")] if x]).strip(),
            "postal_code": it.get("zipCode"),

            # links
            "gemi_api_url": f"{GEMI_BASE}/companies/{ar_gemi}" if ar_gemi else "",
            "gemi_docs_url": f"{GEMI_BASE}/companies/{ar_gemi}/documents" if ar_gemi else "",
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df

def to_excel_bytes(df: pd.DataFrame, sheet_name="Sheet1") -> bytes:
    output = io.BytesIO()
    safe = df.copy()
    if safe is None or safe.empty:
        safe = pd.DataFrame([{"info": "no data"}])
    safe.columns = [str(c) for c in safe.columns]
    for c in safe.columns:
        safe[c] = safe[c].apply(lambda x: x if pd.api.types.is_scalar(x) else str(x))
    with pd.ExcelWriter(output, engine="openpyxl") as w:
        safe.to_excel(w, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output

# =========================
# Tabs: FTTH | ΓΕΜΗ
# =========================
tab_ftth, tab_gemi = st.tabs(["📡 FTTH Matching", "📥 ΓΕΜΗ Downloader"])

# ------------------------------------------------------
# TAB 2: ΓΕΜΗ Downloader (δεξιά)
# ------------------------------------------------------
with tab_gemi:
    st.subheader("📥 Λήψη Επιχειρήσεων από ΓΕΜΗ (με φίλτρα)")
    if not st.session_state.get("gemi_key"):
        st.warning("Βάλε API Key στο πάνω πλαίσιο για να ενεργοποιηθούν τα φίλτρα.")
    else:
        # Load metadata
        try:
            PREFS = md_prefectures()
            MUNIS = md_municipalities()
            STAT  = md_statuses()
        except Exception as e:
            st.error(f"Σφάλμα φόρτωσης metadata: {e}")
            PREFS, MUNIS, STAT = [], [], []

        pref_map = {str(p.get("descr","")): p.get("id") for p in PREFS}
        pref_label = st.selectbox("Νομός", ["— Όλοι —"] + sorted(pref_map.keys()))
        pref_id = pref_map.get(pref_label)

        munis_of_pref = [m for m in MUNIS if (not pref_id) or str(m.get("prefectureId")) == str(pref_id)]
        muni_map = {str(m.get("descr","")): m.get("id") for m in munis_of_pref}
        muni_label = st.selectbox("Δήμος", ["— Όλοι —"] + sorted(muni_map.keys()))
        muni_id = muni_map.get(muni_label)

        status_map = {str(s.get("descr","")): s.get("id") for s in STAT}
        sel_statuses = st.multiselect("Καταστάσεις", sorted(status_map.keys()))
        status_ids = [status_map[x] for x in sel_statuses if x in status_map]

        ia_label = st.selectbox("Ενεργή;", ["—", "Ναι", "Όχι"])
        ia_value = {"—": None, "Ναι": "true", "Όχι": "false"}[ia_label]

        name_part = st.text_input("Επωνυμία περιέχει (>=3 χαρακτήρες, προαιρετικό)", "")

        cA, cB, cC = st.columns([1,1,1])
        with cA:
            do_preview = st.button("🔎 Προεπισκόπηση (μέχρι 200)")
        with cB:
            do_export = st.button("⬇️ Εξαγωγή σε Excel (όλα με pagination)")
        with cC:
            set_src = st.button("📌 Χρήση αυτών ως Πηγή για FTTH (αριστερά)")

        if do_preview:
            try:
                results, total = companies_search(
                    name=name_part or None,
                    prefectures=[pref_id] if pref_id else None,
                    municipalities=[muni_id] if muni_id else None,
                    statuses=status_ids or None,
                    is_active=ia_value,
                    size=200,
                )
                df = companies_to_df(results)
                if df.empty:
                    st.warning("Δεν βρέθηκαν επιχειρήσεις με τα κριτήρια.")
                else:
                    st.success(f"Ήρθαν {len(df)} / σύνολο: {total if total is not None else '—'}")
                    st.dataframe(df, use_container_width=True)
                    st.download_button("⬇️ Λήψη Excel (προεπισκόπηση)",
                                       to_excel_bytes(df, "preview"),
                                       file_name="gemi_preview.xlsx")
                    st.session_state["last_gemi_df"] = df
            except Exception as e:
                st.error(f"Σφάλμα αναζήτησης: {e}")

        if do_export:
            with st.spinner("Γίνεται λήψη όλων των σελίδων…"):
                try:
                    items = companies_all(
                        name=name_part or None,
                        prefectures=[pref_id] if pref_id else None,
                        municipalities=[muni_id] if muni_id else None,
                        statuses=status_ids or None,
                        is_active=ia_value,
                        size=200, max_pages=200
                    )
                    df = companies_to_df(items)
                    if df.empty:
                        st.warning("Δεν βρέθηκαν επιχειρήσεις για εξαγωγή.")
                    else:
                        st.success(f"Έτοιμο: {len(df)} εγγραφές.")
                        st.dataframe(df.head(50), use_container_width=True)
                        st.download_button("⬇️ Excel – Επιχειρήσεις (φίλτρα εφαρμοσμένα)",
                                           to_excel_bytes(df, "export"),
                                           file_name="gemi_export.xlsx")
                        st.session_state["last_gemi_df"] = df
                except Exception as e:
                    st.error(f"Σφάλμα αναζήτησης/εξαγωγής: {e}")

        if set_src:
            if "last_gemi_df" in st.session_state and not st.session_state["last_gemi_df"].empty:
                st.success("Ορίστηκε: Θα χρησιμοποιηθούν τα τελευταία αποτελέσματα ΓΕΜΗ ως πηγή στο FTTH.")
            else:
                st.warning("Δεν υπάρχει αποτέλεσμα από ΓΕΜΗ ακόμη (τρέξε Προεπισκόπηση ή Εξαγωγή).")

# ------------------------------------------------------
# TAB 1: FTTH Matching (αριστερά)
# ------------------------------------------------------
with tab_ftth:
    st.subheader("📡 FTTH Geocoding & Matching")

    # Sidebar-like settings inside the tab
    with st.expander("⚙️ Ρυθμίσεις γεωκωδικοποίησης & απόστασης", expanded=True):
        geocoder = st.selectbox("Geocoder", ["Nominatim (δωρεάν)", "Google (API key)"])
        google_key = st.text_input("Google API key", type="password", help="Αν είναι κενό, χρησιμοποιείται Nominatim.")
        country = st.text_input("Country code", "gr")
        lang = st.text_input("Language", "el")
        throttle = st.slider("Καθυστέρηση (sec) [Nominatim]", 0.5, 2.0, 1.0, 0.5)
        distance_limit = st.number_input("📏 Μέγιστη απόσταση (m)", min_value=1, max_value=500, value=150)

    # Επιλογή πηγής επιχειρήσεων
    source = st.radio("Πηγή Επιχειρήσεων", ["Upload Excel/CSV", "Από ΓΕΜΗ (τελευταίο αποτέλεσμα δεξιά)"], index=0, horizontal=True)

    # Uploads
    c1, c2 = st.columns(2)
    with c1:
        if source == "Upload Excel/CSV":
            biz_file = st.file_uploader("📥 Επιχειρήσεις (Excel/CSV)", type=["xlsx", "csv"])
        else:
            biz_file = None
    with c2:
        ftth_file = st.file_uploader("📥 FTTH σημεία Nova (Excel/CSV)", type=["xlsx", "csv"])

    prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά)", type=["xlsx", "csv"])

    # Helpers (columns detection)
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
        return out.dropna(subset=["latitude","longitude"])

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

    # Business source
    if source == "Upload Excel/CSV":
        biz_df = load_table(biz_file) if biz_file else None
    else:
        biz_df = st.session_state.get("last_gemi_df")

    # Geocode cache
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
            # throttle μόνο σε πραγματικό network call (όχι cache)
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

    # Main run
    start = st.button("🚀 Ξεκίνα geocoding & matching")
    if start and biz_df is not None and ftth_df is not None:
        work = biz_df.copy()

        # pick likely address/city columns
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

        addr_series = pick_first_series(work, ["address", "διεύθυνση", "οδός", "street", "site.company_insights.address"])
        city_series = pick_first_series(work, ["city", "πόλη", "town", "site.company_insights.city"])

        base_addr = addr_series.astype(str).str.strip()
        from_input_city = city_series.astype(str).str.strip()
        work["Address"] = (base_addr + (", " + from_input_city).where(from_input_city.ne(""), "")).str.replace(r"\s+", " ", regex=True)
        work = work[work["Address"].str.len() > 3].copy()

        total = len(work)
        progress = st.progress(0, text=f"0 / {total}")
        errs = 0

        # Optional previous geocoded cache
        geo_map = {}
        prev_df = None
        if prev_geo_file is not None:
            if prev_geo_file.name.lower().endswith(".csv"):
                prev_df = pd.read_csv(prev_geo_file)
            else:
                prev_df = pd.read_excel(prev_geo_file)
        if prev_df is not None:
            cols = {c.lower(): c for c in prev_df.columns}
            if {"address","latitude","longitude"}.issubset(set(cols.keys())):
                p = prev_df.rename(columns={
                    cols.get("address"): "Address",
                    cols.get("latitude"): "Latitude",
                    cols.get("longitude"): "Longitude"
                })
                p["Latitude"]  = pd.to_numeric(p["Latitude"], errors="coerce")
                p["Longitude"] = pd.to_numeric(p["Longitude"], errors="coerce")
                p = p.dropna(subset=["Latitude","Longitude"])
                for _, r in p.iterrows():
                    geo_map[str(r["Address"]).strip()] = (float(r["Latitude"]), float(r["Longitude"]))

        work["Latitude"] = pd.NA
        work["Longitude"] = pd.NA

        for i, (idx, row) in enumerate(work.iterrows(), start=1):
            addr = str(row["Address"]).strip()
            if addr in geo_map:
                lat, lon = geo_map[addr]
            else:
                lat, lon = geocode_address(addr, geocoder, api_key=google_key, cc=country, lang=lang, throttle_sec=throttle)
                if (lat is not None) and (lon is not None):
                    geo_map[addr] = (lat, lon)
                else:
                    errs += 1
                    lat, lon = (None, None)
            work.at[idx, "Latitude"]  = lat
            work.at[idx, "Longitude"] = lon
            progress.progress(i/max(1,total), text=f"{i} / {total} γεωκωδικοποιημένα…")

        work["Latitude"]  = pd.to_numeric(work["Latitude"], errors="coerce")
        work["Longitude"] = pd.to_numeric(work["Longitude"], errors="coerce")

        merged = work.copy()

        # Matching
        ftth_points = ftth_df[["latitude","longitude"]].dropna().to_numpy()
        matches = []
        for _, row in merged.dropna(subset=["Latitude","Longitude"]).iterrows():
            try:
                biz_lat = float(row["Latitude"])
                biz_lon = float(row["Longitude"])
            except Exception:
                continue
            biz_coords = (biz_lat, biz_lon)
            found = False
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
                    found = True
                    break
            if not found:
                pass

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
            st.download_button("⬇️ Geocoded διευθύνσεις", to_excel_bytes(merged[["Address","Latitude","Longitude"]]), file_name="geocoded_addresses.xlsx")
        with c2:
            st.download_button("⬇️ Αποτελέσματα Matching", to_excel_bytes(result_df), file_name="ftth_matching_results.xlsx")
        with c3:
            st.download_button("⬇️ Όλα τα δεδομένα (merged)", to_excel_bytes(merged), file_name="merged_with_geocoded.xlsx")

    if start and (biz_df is None or ftth_df is None):
        st.error("❌ Χρειάζονται ΚΑΙ Πηγή Επιχειρήσεων ΚΑΙ FTTH σημεία.")
