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

def _find_col(df: pd.DataFrame, patterns):
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

def _first_key(d: dict, keys, default=""):
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
    """Φορτώνει παραμετρικά από ΓΕΜΗ με fallbacks (slugs)."""
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
    """Αναζήτηση εταιρειών: δοκιμάζει εναλλακτικά paths & payloads (snake/camel)."""
    headers = _gemi_headers(api_key)
    payload_variants = [
        {  # variant A
            "page": page, "page_size": page_size,
            "nomos_id": nomos_id, "dimos_id": dimos_id,
            "status_id": status_id, "name_part": name_part, "kad": kad_list or [],
            "incorporation_date_from": date_from, "incorporation_date_to": date_to,
            "foundation_date_from": date_from, "foundation_date_to": date_to,
            "registration_date_from": date_from, "registration_date_to": date_to,
        },
        {  # variant B
            "page": page, "per_page": page_size,
            "nomosId": nomos_id, "dimosId": dimos_id,
            "statusId": status_id, "name": name_part, "kad": kad_list or [],
            "incorporationDateFrom": date_from, "incorporationDateTo": date_to,
            "foundationDateFrom": date_from, "foundationDateTo": date_to,
            "registrationDateFrom": date_from, "registrationDateTo": date_to,
        },
    ]
    paths = ["/search", "/companies/search"]
    last_err = None
    for path in paths:
        url = f"{GEMI_BASE}{path}"
        # POST (κυρίως)
        for payload in payload_variants:
            try:
                r = requests.post(url, json=payload, headers=headers, timeout=60)
                if r.status_code in (400, 404, 415):
                    last_err = f"{r.status_code} on {url} payload={list(payload.keys())}"
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                last_err = str(e)
        # GET fallback
        try:
            r = requests.get(url, params=payload_variants[-1], headers=headers, timeout=60)
            if r.ok:
                return r.json()
        except requests.RequestException as e:
            last_err = str(e)
    raise RuntimeError(f"ΓΕΜΗ: αναζήτηση απέτυχε. Τελευταίο σφάλμα: {last_err}")

def gemi_search_all(api_key, *, nomos_id=None, dimos_id=None, status_id=None,
                    name_part=None, kad_list=None, date_from=None, date_to=None,
                    page_size=200, max_pages=200, sleep_sec=0.3):
    """Πλήρης εξαγωγή (pagination) για τα τρέχοντα φίλτρα."""
    all_items = []
    for page in range(1, max_pages + 1):
        data = gemi_search(
            api_key,
            nomos_id=nomos_id, dimos_id=dimos_id, status_id=status_id,
            name_part=name_part, kad_list=kad_list,
            date_from=date_from, date_to=date_to,
            page=page, page_size=page_size
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
    """Κανονικοποίηση αντικειμένων ΓΕΜΗ → DataFrame με επικοινωνία, σύσταση & ΚΑΔ."""
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
    if not gemi_key:
        st.warning("🔑 Βάλε GΕΜΗ API Key για να ενεργοποιηθεί η αναζήτηση.")
    else:
        try:
            nomoi = gemi_params(gemi_key, "nomoi")
            statuses = gemi_params(gemi_key, "statuses")

            nomos_names = [n.get("name") for n in nomoi]
            sel_nomos = st.selectbox("Νομός", nomos_names, index=0)
            nomos_id = next(n["id"] for n in nomoi if n.get("name") == sel_nomos)

            dimoi = gemi_params(gemi_key, "dimoi", nomos_id=nomos_id)
            dimos_names = [d.get("name") for d in dimoi]
            ALL_DM = "— Όλοι οι Δήμοι —"
            dimos_label_to_id = {d.get("name"): d.get("id") for d in dimoi}
            sel_dimoi = st.multiselect("Δήμοι (πολλαπλή επιλογή)", [ALL_DM] + dimos_names, default=[ALL_DM])

            status_names = [s.get("name") for s in statuses]
            default_status = next((i for i, s in enumerate(statuses) if "ενεργ" in s.get("name","").lower()), 0)
            sel_status = st.selectbox("Κατάσταση", status_names, index=default_status)
            status_id = next(s["id"] for s in statuses if s.get("name") == sel_status)

            name_part = st.text_input("Κομμάτι επωνυμίας (προαιρετικό)", "")

            # ΚΑΔ (multi)
            try:
                kad_params = gemi_params(gemi_key, "kad")
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

            # Ημερομηνία σύστασης
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
                rows = []
                target_dimoi = None
                if sel_dimoi and not (len(sel_dimoi) == 1 and ALL_DM in sel_dimoi):
                    target_dimoi = [dimos_label_to_id[x] for x in sel_dimoi if x in dimos_label_to_id]

                if target_dimoi:
                    all_items = []
                    for d_id in target_dimoi:
                        data = gemi_search(
                            gemi_key,
                            nomos_id=nomos_id, dimos_id=d_id, status_id=status_id,
                            name_part=name_part, kad_list=sel_kads,
                            date_from=(date_from or None), date_to=(date_to or None),
                            page=1, page_size=200
                        )
                        items = data.get("items", [])
                        for it in items:
                            it["__region_dimos"] = next((nm for nm, _id in dimos_label_to_id.items() if _id == d_id), "")
                        all_items.extend(items)
                else:
                    data = gemi_search(
                        gemi_key,
                        nomos_id=nomos_id, dimos_id=None, status_id=status_id,
                        name_part=name_part, kad_list=sel_kads,
                        date_from=(date_from or None), date_to=(date_to or None),
                        page=1, page_size=200
                    )
                    all_items = data.get("items", [])

                for it in all_items:
                    name  = _first_key(it, ["name", "company_name"])
                    addr  = _first_key(it, ["address", "postal_address", "registered_address"])
                    city  = _first_key(it, ["municipality", "dimos_name", "city"])
                    afm   = _first_key(it, ["afm", "vat_number", "tin"])
                    gemi  = _first_key(it, ["gemi_number", "registry_number", "commercial_registry_no"])
                    phone = _first_key(it, ["phone", "telephone", "contact_phone", "phone_number"])
                    email = _first_key(it, ["email", "contact_email", "email_address"])
                    website = _first_key(it, ["website","site","url","homepage"])
                    inc_date = _first_key(it, ["incorporation_date","foundation_date","establishment_date","founded_at","registration_date"])
                    kad_codes = it.get("kad_codes") or it.get("kads") or it.get("kad") or ""
                    rows.append({
                        "region_nomos": sel_nomos,
                        "region_dimos": it.get("__region_dimos",""),
                        "name": name, "address": addr, "city": city,
                        "afm": afm, "gemi": gemi, "phone": phone, "email": email,
                        "website": website, "incorporation_date": inc_date,
                        "kad_codes": kad_codes,
                    })
                gemi_df = pd.DataFrame(rows)

                # client-side date filter (safety)
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

                # client-side KAD filter (safety)
                if not gemi_df.empty and sel_kads:
                    patt = "|".join([re.escape(k) for k in sel_kads])
                    gemi_df = gemi_df[gemi_df["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]

                if gemi_df.empty:
                    st.warning("Δεν βρέθηκαν εγγραφές από ΓΕΜΗ με τα φίλτρα που έβαλες.")
                else:
                    st.success(f"Βρέθηκαν {len(gemi_df)} εγγραφές από ΓΕΜΗ.")
                    st.dataframe(gemi_df, use_container_width=True)
                    st.download_button(
                        "⬇️ Κατέβασμα επιχειρήσεων ΓΕΜΗ (Excel)",
                        _to_excel_bytes(gemi_df),
                        file_name="gemi_businesses.xlsx"
                    )

            if do_export_one:
                with st.spinner("Εξαγωγή… μπορεί να πάρει λίγο αν υπάρχουν πολλές σελίδες."):
                    dfs = []
                    target_dimoi = None
                    if sel_dimoi and not (len(sel_dimoi) == 1 and ALL_DM in sel_dimoi):
                        target_dimoi = [dimos_label_to_id[x] for x in sel_dimoi if x in dimos_label_to_id]

                    def _fetch_df(d_id, dimos_label):
                        items = gemi_search_all(
                            gemi_key,
                            nomos_id=nomos_id, dimos_id=d_id, status_id=status_id,
                            name_part=name_part or None,
                            kad_list=sel_kads or None,
                            date_from=(date_from or None), date_to=(date_to or None),
                            page_size=200
                        )
                        df = gemi_items_to_df(items)
                        if not df.empty:
                            df.insert(0, "region_nomos", sel_nomos)
                            df.insert(1, "region_dimos", dimos_label or "")
                        return df

                    if target_dimoi:
                        for d_id in target_dimoi:
                            dimos_label = next((nm for nm, _id in dimos_label_to_id.items() if _id == d_id), "")
                            dfp = _fetch_df(d_id, dimos_label)
                            if dfp is not None and not dfp.empty:
                                dfs.append(dfp)
                    else:
                        dfp = _fetch_df(None, "")
                        if dfp is not None and not dfp.empty:
                            dfs.append(dfp)

                    if not dfs:
                        st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
                    else:
                        export_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
                        # safety date filter
                        if (date_from or date_to) and "incorporation_date" in export_df:
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

                        st.success(f"Έτοιμο: {len(export_df)} εγγραφές στο αρχείο.")
                        st.dataframe(export_df.head(50), use_container_width=True)
                        st.download_button(
                            "⬇️ Excel – Επιχειρήσεις (ένα αρχείο, με φίλτρα)",
                            _to_excel_bytes(export_df),
                            file_name=f"gemi_{sel_nomos}_filtered.xlsx"
                        )
        except Exception as e:
            st.error(f"Σφάλμα ΓΕΜΗ: {e}")
            st.stop()

# Αν επιλεγεί ΓΕΜΗ, χρησιμοποίησε αυτά τα δεδομένα ως πηγή επιχειρήσεων
if biz_source == "ΓΕΜΗ (OpenData API)":
    biz_df = gemi_df

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
        time.sleep(throttle_sec)  # ευγενικό throttle
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

    # Γραμμή-γραμμή geocoding
    total = len(work)
    progress = st.progress(0, text=f"0 / {total}")
    errs = 0

    # cache από prev_df (αν δόθηκε)
    geo_map = {}
    if prev_geo_file is not None:
        prev_df = load_table(prev_geo_file)
    else:
        prev_df = None

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

    # Κόμμα/τελεία -> float & καθάρισμα
    work["Latitude"]  = pd.to_numeric(work["Latitude"].astype(str).str.replace(",", "."), errors="coerce")
    work["Longitude"] = pd.to_numeric(work["Longitude"].astype(str).str.replace(",", "."), errors="coerce")

    # merged = όλα τα geocoded rows
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

    # ----- Robust Excel export -----
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
    st.info("📄 Ανέβασε FTTH, επίλεξε πηγή επιχειρήσεων (Upload ή ΓΕΜΗ), και πάτα «🚀 Ξεκίνα».")
