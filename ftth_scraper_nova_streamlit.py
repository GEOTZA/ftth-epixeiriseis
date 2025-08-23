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

st.set_page_config(page_title="FTTH Geocoding & Matching (v7)", layout="wide")
st.title("📡 FTTH Geocoding & Matching – v7")

# ================= Sidebar =================
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

    st.caption("⚠️ Rate limit ΓΕΜΗ: 8 req/min (429 αν ξεπεραστεί).")

# ================= Uploads & Inputs =================
st.subheader("📥 Αρχεία")
biz_file = st.file_uploader("Excel/CSV Επιχειρήσεων", type=["xlsx", "csv"]) if biz_source == "Upload Excel/CSV" else None
ftth_file = st.file_uploader("FTTH σημεία Nova (Excel/CSV) – υποστηρίζει ελληνικές στήλες λ/φ και πολλαπλά sheets", type=["xlsx", "csv"])
prev_geo_file = st.file_uploader("🧠 Προηγούμενα geocoded (προαιρετικά) – Excel/CSV με στήλες: Address, Latitude, Longitude", type=["xlsx", "csv"])

# ================= Helpers =================
def load_table(uploaded):
    if uploaded is None:
        return None
    name = uploaded.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded)
    return pd.read_excel(uploaded)

def pick_first_series(df, candidates):
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

# ============= GEMI OpenData (σύμφωνα με Swagger) =============
GEMI_BASE = "https://opendata-api.businessportal.gr/api/opendata/v1"
GEMI_HEADER = "api_key"
TIMEOUT = 40

def _hdr(api_key: str):
    return {GEMI_HEADER: api_key, "Accept": "application/json"}

@st.cache_data(ttl=3600, show_spinner=False)
def gemi_metadata(api_key: str):
    """
    Φέρνει λίστες: prefectures, municipalities, companyStatuses, activities.
    Επιστρέφει dict με keys: 'prefectures','municipalities','statuses','activities'
    """
    s = requests.Session()
    s.headers.update(_hdr(api_key))
    def _get(ep):
        url = f"{GEMI_BASE}/{ep.lstrip('/')}"
        r = s.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()

    data = {}
    data["prefectures"]   = _get("metadata/prefectures")
    data["municipalities"] = _get("metadata/municipalities")
    data["statuses"]      = _get("metadata/companyStatuses")
    # ΚΑΔ μπορεί να είναι πολλά – αλλά το ζητήσατε: dropdown. Αν «βαρύ», αλλάζουμε σε text input.
    data["activities"]    = _get("metadata/activities")
    return data

def _safe(v, *keys):
    cur = v
    for k in keys:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(k, "")
    return cur if cur is not None else ""

def companies_to_df(items):
    rows = []
    for it in items:
        # ονόματα
        name = it.get("coNameEl") or _safe(it, "coTitlesEl") or _safe(it, "coTitlesEn") or ""
        # διεύθυνση
        street = it.get("street") or ""
        street_no = it.get("streetNumber") or ""
        address = f"{street} {street_no}".strip()
        # ΚΑΔ (activities)
        act_list = it.get("activities") or []
        kad_codes = []
        kad_descrs = []
        for a in act_list:
            act = a.get("activity") or {}
            if isinstance(act, dict):
                if act.get("id"):
                    kad_codes.append(str(act.get("id")))
                if act.get("descr"):
                    kad_descrs.append(str(act.get("descr")))
        rows.append({
            "prefecture_id": _safe(it, "prefecture", "id"),
            "prefecture": _safe(it, "prefecture", "descr"),
            "municipality_id": _safe(it, "municipality", "id"),
            "municipality": _safe(it, "municipality", "descr"),
            "city": it.get("city") or "",
            "address": address,
            "zip": it.get("zipCode") or "",
            "email": it.get("email") or "",
            "url": it.get("url") or "",
            "arGemi": it.get("arGemi") or "",
            "afm": it.get("afm") or "",
            "legal_type": _safe(it, "legalType", "descr"),
            "status": _safe(it, "status", "descr"),
            "incorporationDate": it.get("incorporationDate") or "",
            "kad_codes": ";".join(kad_codes),
            "kad_descr": ";".join(kad_descrs),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df

def companies_search(api_key: str, *, name=None, prefectures=None, municipalities=None,
                     statuses=None, activities=None, is_active=None,
                     offset=0, size=200, sort_by="+arGemi"):
    """
    Καλεί GET /companies σύμφωνα με Swagger.
    - arrays: comma-separated strings (π.χ. '1,2,3')
    """
    s = requests.Session()
    s.headers.update(_hdr(api_key))
    params = {"resultsOffset": offset, "resultsSize": size, "resultsSortBy": sort_by}

    if name and len(name.strip()) >= 3:
        params["name"] = name.strip()

    def _join(x):
        return ",".join([str(i) for i in x]) if x else None

    if prefectures:
        params["prefectures"] = _join(prefectures)
    if municipalities:
        params["municipalities"] = _join(municipalities)
    if statuses:
        params["statuses"] = _join(statuses)
    if activities:
        params["activities"] = _join(activities)
    if is_active is not None:
        params["isActive"] = bool(is_active)

    # καθάρισμα None
    params = {k: v for k, v in params.items() if v not in (None, "", [])}

    url = f"{GEMI_BASE}/companies"
    r = s.get(url, params=params, timeout=TIMEOUT)
    if r.status_code == 429:
        raise RuntimeError("429 Too Many Requests (υπέρβαση 8 req/min). Δοκίμασε πάλι μετά από μερικά δευτερόλεπτα.")
    r.raise_for_status()
    js = r.json()
    results = js.get("searchResults") or []
    meta = js.get("searchMetadata") or {}
    total = meta.get("totalCount")
    return results, int(total) if isinstance(total, int) or (isinstance(total, str) and total.isdigit()) else None

def companies_export_all(api_key: str, **kw):
    """
    Πολλαπλές σελίδες με σεβασμό στο 8 req/min:
    - size=200
    - 1ο call ⇒ παίρνουμε totalCount
    - έπειτα loop με offset += 200 και sleep 8s
    """
    size = kw.pop("size", 200)
    size = max(1, min(200, int(size)))
    out = []

    first, total = companies_search(api_key, size=size, **kw)
    out.extend(first)
    if total is None:
        return out
    if len(out) >= total:
        return out

    offset = size
    while offset < total:
        time.sleep(8.2)  # rate limit guard
        page, _ = companies_search(api_key, offset=offset, size=size, **kw)
        if not page:
            break
        out.extend(page)
        offset += size
    return out

# ============= ΓΕΜΗ – UI =============
gemi_df = None
if biz_source == "ΓΕΜΗ (OpenData API)":
    if not gemi_key:
        st.warning("🔑 Βάλε GΕΜΗ API Key για να ενεργοποιηθεί η αναζήτηση.")
    else:
        st.subheader("🏷️ ΓΕΜΗ – Εξαγωγή / Προεπισκόπηση")
        # Φόρτωση metadata με caching
        md_pref, md_muni, md_status, md_act = [], [], [], []
        try:
            meta = gemi_metadata(gemi_key)
            md_pref = meta.get("prefectures") or []
            md_muni = meta.get("municipalities") or []
            md_status = meta.get("statuses") or []
            md_act = meta.get("activities") or []
        except Exception as e:
            st.error(f"Σφάλμα φόρτωσης metadata: {e}")
            st.info("Δοκίμασε ξανά σε ~60s (πιθανό rate limit 429).")

        # Prefectures (Νομοί)
        pref_label_to_id = {}
        if isinstance(md_pref, list):
            for p in md_pref:
                pid = str(p.get("id") or "").strip()
                pdescr = str(p.get("descr") or "").strip()
                if pid and pdescr:
                    pref_label_to_id[pdescr] = pid
        sel_pref = st.multiselect("Νομός", sorted(pref_label_to_id.keys()), default=[])
        sel_pref_ids = [pref_label_to_id[x] for x in sel_pref]

        # Municipalities (Δήμοι) – φιλτράρονται από Νομούς
        muni_label_to_id = {}
        if isinstance(md_muni, list):
            for m in md_muni:
                mid = str(m.get("id") or "").strip()
                mdescr = str(m.get("descr") or "").strip()
                m_pref_id = str(m.get("prefectureId") or "").strip()
                if sel_pref_ids and (m_pref_id not in sel_pref_ids):
                    continue
                if mid and mdescr:
                    muni_label_to_id[f"{mdescr} (#{mid})"] = mid
        sel_muni = st.multiselect("Δήμος", sorted(muni_label_to_id.keys()), default=[])
        sel_muni_ids = [muni_label_to_id[x] for x in sel_muni]

        # Statuses
        status_label_to_id = {}
        if isinstance(md_status, list):
            for s in md_status:
                sid = s.get("id")
                sdescr = s.get("descr")
                if sid is not None and sdescr:
                    status_label_to_id[f"{sdescr} (#{sid})"] = sid
        sel_status = st.multiselect("Κατάσταση", sorted(status_label_to_id.keys()), default=[])
        sel_status_ids = [status_label_to_id[x] for x in sel_status]

        # ΚΑΔ (Activities)
        # Σημ: είναι πολλά – αλλά με αναζήτηση στο multiselect βρίσκεις εύκολα
        act_label_to_id = {}
        if isinstance(md_act, list):
            for a in md_act:
                aid = str(a.get("id") or "").strip()
                adesc = str(a.get("descr") or "").strip()
                if aid:
                    act_label_to_id[f"{aid} — {adesc}"] = aid
        sel_acts = st.multiselect("ΚΑΔ (δραστηριότητες)", sorted(act_label_to_id.keys()), default=[])
        sel_act_ids = [act_label_to_id[x] for x in sel_acts]

        # Λεκτικό ονόματος, ενεργές μόνο, μέγεθος σελίδας
        name_part = st.text_input("Επωνυμία περιέχει (>=3 χαρακτήρες για χρήση στο API)", "")
        is_active = st.selectbox("Ενεργές μόνο;", ["—", "Ναι", "Όχι"], index=0)
        is_active_val = None if is_active == "—" else (True if is_active == "Ναι" else False)
        page_size = st.slider("Μέγεθος σελίδας (Preview/Export)", 10, 200, 200, 10)

        # Client-side φίλτρα ημερομηνίας (δεν υπάρχουν στο API)
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.text_input("Σύσταση από (YYYY-MM-DD) – client-side", "")
        with c2:
            date_to = st.text_input("Σύσταση έως (YYYY-MM-DD) – client-side", "")

        cA, cB = st.columns(2)
        with cA:
            do_preview = st.button("🔎 Προεπισκόπηση (<=200)")
        with cB:
            do_export = st.button("⬇️ Εξαγωγή Excel (όλες οι σελίδες)")

        if do_preview:
            try:
                items, total = companies_search(
                    gemi_key,
                    name=name_part or None,
                    prefectures=sel_pref_ids or None,
                    municipalities=sel_muni_ids or None,
                    statuses=sel_status_ids or None,
                    activities=sel_act_ids or None,
                    is_active=is_active_val,
                    offset=0, size=page_size
                )
                df = companies_to_df(items)
                # client-side date filter
                if not df.empty and (date_from or date_to):
                    dser = pd.to_datetime(df["incorporationDate"], errors="coerce")
                    if date_from:
                        try:
                            df = df[dser >= pd.to_datetime(date_from)]
                        except Exception:
                            pass
                    if date_to:
                        try:
                            df = df[dser <= pd.to_datetime(date_to)]
                        except Exception:
                            pass
                gemi_df = df
                if df.empty:
                    st.warning("Δεν βρέθηκαν εγγραφές.")
                else:
                    st.success(f"OK: {len(df)} εγγραφές (totalCount: {total if total is not None else '—'})")
                    st.dataframe(df, use_container_width=True)
                    st.download_button("⬇️ Excel (προεπισκόπηση)", _to_excel_bytes(df), file_name="gemi_preview.xlsx")
            except Exception as e:
                st.error(f"Σφάλμα αναζήτησης: {e}")

        if do_export:
            try:
                with st.spinner("Κατέβασμα σελίδων… (τηρείται 8 req/min)"):
                    items = companies_export_all(
                        gemi_key,
                        name=name_part or None,
                        prefectures=sel_pref_ids or None,
                        municipalities=sel_muni_ids or None,
                        statuses=sel_status_ids or None,
                        activities=sel_act_ids or None,
                        is_active=is_active_val,
                        size=page_size
                    )
                df = companies_to_df(items)
                # client-side date filter
                if not df.empty and (date_from or date_to):
                    dser = pd.to_datetime(df["incorporationDate"], errors="coerce")
                    if date_from:
                        try:
                            df = df[dser >= pd.to_datetime(date_from)]
                        except Exception:
                            pass
                    if date_to:
                        try:
                            df = df[dser <= pd.to_datetime(date_to)]
                        except Exception:
                            pass
                if df.empty:
                    st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
                else:
                    st.success(f"Έτοιμο: {len(df)} εγγραφές.")
                    st.dataframe(df.head(50), use_container_width=True)
                    st.download_button("⬇️ Excel – Επιχειρήσεις (με φίλτρα)", _to_excel_bytes(df), file_name="gemi_filtered.xlsx")
                gemi_df = df
            except Exception as e:
                st.error(f"Σφάλμα εξαγωγής: {e}")

# Αν επιλεγεί ΓΕΜΗ, χρησιμοποίησε αυτά τα δεδομένα ως πηγή επιχειρήσεων
biz_df = None
if biz_source == "Upload Excel/CSV":
    biz_df = load_table(biz_file) if biz_file else None
elif biz_source == "ΓΕΜΗ (OpenData API)":
    biz_df = gemi_df

# ============= Geocode cache =============
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

# ============= Main: Geocoding & Matching =============
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
    st.info("📄 Ανέβασε FTTH, διάλεξε πηγή επιχειρήσεων (Upload ή ΓΕΜΗ), και πάτα «🚀 Ξεκίνα».")
