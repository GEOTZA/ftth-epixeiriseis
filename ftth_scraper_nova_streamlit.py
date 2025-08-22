import re

@@
 GEMI_BASE = "https://opendata-api.businessportal.gr/opendata"
 GEMI_HEADER_NAME = "api_key"
@@
 def gemi_params(api_key, what, *, nomos_id=None):
@@
     if what == "statuses":
         candidates += [
             f"{GEMI_BASE}/params/status",
             f"{GEMI_BASE}/params/company_statuses",
         ]
+    if what in ("kad", "kads"):
+        # ΚΑΔ fallbacks (διαφορετικά slugs σε διαφορετικές εκδόσεις)
+        candidates += [
+            f"{GEMI_BASE}/params/kad",
+            f"{GEMI_BASE}/params/kads",
+            f"{GEMI_BASE}/params/activity_codes",
+            f"{GEMI_BASE}/params/kad_codes",
+            f"{GEMI_BASE}/params/nace",
+        ]
@@
-def gemi_search(api_key, *, nomos_id=None, dimos_id=None, status_id=None,
-                name_part=None, kad_list=None, page=1, page_size=200):
+def gemi_search(api_key, *, nomos_id=None, dimos_id=None, status_id=None,
+                name_part=None, kad_list=None, date_from=None, date_to=None,
+                page=1, page_size=200):
@@
-    payload_variants = [
+    payload_variants = [
         {  # variant A
             "page": page, "page_size": page_size,
             "nomos_id": nomos_id, "dimos_id": dimos_id,
-            "status_id": status_id, "name_part": name_part, "kad": kad_list or []
+            "status_id": status_id, "name_part": name_part, "kad": kad_list or [],
+            # ημερομηνίες σύστασης (πιθανά κλειδιά)
+            "incorporation_date_from": date_from, "incorporation_date_to": date_to,
+            "foundation_date_from": date_from, "foundation_date_to": date_to,
+            "registration_date_from": date_from, "registration_date_to": date_to,
         },
         {  # variant B
             "page": page, "per_page": page_size,
             "nomosId": nomos_id, "dimosId": dimos_id,
-            "statusId": status_id, "name": name_part, "kad": kad_list or []
+            "statusId": status_id, "name": name_part, "kad": kad_list or [],
+            "incorporationDateFrom": date_from, "incorporationDateTo": date_to,
+            "foundationDateFrom": date_from, "foundationDateTo": date_to,
+            "registrationDateFrom": date_from, "registrationDateTo": date_to,
         },
     ]
@@
-def gemi_search_all(api_key, *, nomos_id=None, dimos_id=None, status_id=None,
-                    name_part=None, kad_list=None, page_size=200, max_pages=200, sleep_sec=0.3):
+def gemi_search_all(api_key, *, nomos_id=None, dimos_id=None, status_id=None,
+                    name_part=None, kad_list=None, date_from=None, date_to=None,
+                    page_size=200, max_pages=200, sleep_sec=0.3):
@@
-        data = gemi_search(
+        data = gemi_search(
             api_key,
             nomos_id=nomos_id, dimos_id=dimos_id, status_id=status_id,
-            name_part=name_part, kad_list=kad_list, page=page, page_size=page_size
+            name_part=name_part, kad_list=kad_list,
+            date_from=date_from, date_to=date_to,
+            page=page, page_size=page_size
         )
@@
 def gemi_items_to_df(items: list[dict]) -> pd.DataFrame:
     """Κανονικοποίηση αντικειμένων ΓΕΜΗ → DataFrame με επικοινωνία & σύσταση."""
     rows = []
     for it in items:
+        # Συλλογή ΚΑΔ (λίστα ή string)
+        raw_kads = it.get("kads") or it.get("kad") or it.get("activity_codes")
+        if isinstance(raw_kads, list):
+            def _x(x):
+                if isinstance(x, dict):
+                    return x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or ""
+                return str(x)
+            kad_join = ";".join([_x(x) for x in raw_kads if x])
+        else:
+            kad_join = str(raw_kads or "")
         rows.append({
             "name":  _first_non_empty(it, ["name","company_name","commercial_name","registered_name"]),
             "afm":   _first_non_empty(it, ["afm","vat_number","tin"]),
             "gemi":  _first_non_empty(it, ["gemi_number","registry_number","commercial_registry_no"]),
             "legal_form": _first_non_empty(it, ["legal_form","company_type","form"]),
             "status":     _first_non_empty(it, ["status","company_status","status_name"]),
             # Ημερομηνία σύστασης / εγγραφής
             "incorporation_date": _first_non_empty(it, [
                 "incorporation_date","foundation_date","establishment_date","founded_at","registration_date"
             ]),
             # Επικοινωνία
             "address": _first_non_empty(it, ["address","postal_address","registered_address","address_line"]),
             "city":    _first_non_empty(it, ["municipality","dimos_name","city","town"]),
             "postal_code": _first_non_empty(it, ["postal_code","zip","tk","postcode"]),
             "phone":   _first_non_empty(it, ["phone","telephone","contact_phone","phone_number"]),
             "email":   _first_non_empty(it, ["email","contact_email","email_address"]),
             "website": _first_non_empty(it, ["website","site","url","homepage"]),
+            "kad_codes": kad_join,
         })
     df = pd.DataFrame(rows)
     if not df.empty:
         # Προαιρετικό καθάρισμα ημερομηνίας (μένει ως string αν δεν είναι ISO)
         for c in ["incorporation_date"]:
             df[c] = df[c].astype(str).str.strip()
         df = df.drop_duplicates().sort_values(["name","city","postal_code"], na_position="last").reset_index(drop=True)
     return df
@@
 if biz_source == "ΓΕΜΗ (OpenData API)":
@@
-            dimoi = gemi_params(gemi_key, "dimoi", nomos_id=nomos_id)
-            dimos_names = [d["name"] for d in dimoi]
-            sel_dimos = st.selectbox("Δήμος", dimos_names, index=0)
-            dimos_id = next(d["id"] for d in dimoi if d["name"] == sel_dimos)
+            dimoi = gemi_params(gemi_key, "dimoi", nomos_id=nomos_id)
+            dimos_names = [d["name"] for d in dimoi]
+            # Multi-select Δήμων με επιλογή "Όλοι"
+            ALL_DM = "— Όλοι οι Δήμοι —"
+            dimos_label_to_id = {d["name"]: d.get("id") for d in dimoi}
+            sel_dimoi = st.multiselect("Δήμοι (πολλαπλή επιλογή)", [ALL_DM] + dimos_names, default=[ALL_DM])
@@
-            name_part = st.text_input("Κομμάτι επωνυμίας (προαιρετικό)", "")
+            name_part = st.text_input("Κομμάτι επωνυμίας (προαιρετικό)", "")
+
+            # --- ΚΑΔ (multi-select) ---
+            try:
+                kad_params = gemi_params(gemi_key, "kad")
+            except Exception:
+                kad_params = []
+            def _kad_label(x):
+                if isinstance(x, dict):
+                    code = x.get("code") or x.get("kad") or x.get("id") or x.get("nace") or ""
+                    desc = x.get("name") or x.get("title") or x.get("description") or ""
+                    return f"{code} — {desc}".strip(" —")
+                return str(x)
+            kad_options = [(_kad_label(k), (k.get("code") or k.get("kad") or k.get("id") or k.get("nace") or "").strip())
+                           for k in kad_params if isinstance(k, dict)]
+            kad_labels = [lbl for (lbl, code) in kad_options if code]
+            kad_label_to_code = {lbl: code for (lbl, code) in kad_options if code}
+            sel_kad_labels = st.multiselect("ΚΑΔ (πολλοί, προαιρετικό)", kad_labels, default=[])
+            sel_kads = [kad_label_to_code[lbl] for lbl in sel_kad_labels]
+
+            # --- Ημερομηνία σύστασης (from–to) ---
+            c1, c2 = st.columns(2)
+            with c1:
+                date_from = st.text_input("Σύσταση από (YYYY-MM-DD)", "")
+            with c2:
+                date_to = st.text_input("Σύσταση έως (YYYY-MM-DD)", "")
@@
-            cA, cB = st.columns(2)
+            cA, cB = st.columns(2)
             with cA:
                 do_search = st.button("🔎 Αναζήτηση ΓΕΜΗ (τρεχόντας φίλτρα)")
             with cB:
-                do_export_nomos = st.button(f"⬇️ Εξαγωγή Excel για όλο τον Νομό «{sel_nomos}»")
+                do_export_one = st.button("⬇️ Εξαγωγή Excel (ένα αρχείο με εφαρμοσμένα φίλτρα)")
@@
-            if do_search:
-                data = gemi_search(gemi_key, nomos_id=nomos_id, dimos_id=dimos_id, status_id=status_id, name_part=name_part)
+            if do_search:
+                # Αν επιλεγεί "Όλοι", κάνε μία κλήση στο Νομό. Αλλιώς, τρέξε για κάθε επιλεγμένο Δήμο.
+                rows = []
+                target_dimoi = None
+                if sel_dimoi and not (len(sel_dimoi) == 1 and ALL_DM in sel_dimoi):
+                    target_dimoi = [dimos_label_to_id[x] for x in sel_dimoi if x in dimos_label_to_id]
+
+                if target_dimoi:
+                    all_items = []
+                    for d_id in target_dimoi:
+                        data = gemi_search(gemi_key,
+                                           nomos_id=nomos_id, dimos_id=d_id, status_id=status_id,
+                                           name_part=name_part, kad_list=sel_kads,
+                                           date_from=(date_from or None), date_to=(date_to or None),
+                                           page=1, page_size=200)
+                        items = data.get("items", [])
+                        for it in items:
+                            it["__region_dimos"] = next((nm for nm, _id in dimos_label_to_id.items() if _id == d_id), "")
+                        all_items.extend(items)
+                else:
+                    data = gemi_search(gemi_key,
+                                       nomos_id=nomos_id, dimos_id=None, status_id=status_id,
+                                       name_part=name_part, kad_list=sel_kads,
+                                       date_from=(date_from or None), date_to=(date_to or None),
+                                       page=1, page_size=200)
+                    all_items = data.get("items", [])
+
-                rows = []
-                for it in data.get("items", []):
+                for it in all_items:
                     name  = _first_key(it, ["name", "company_name"])
                     addr  = _first_key(it, ["address", "postal_address", "registered_address"])
                     city  = _first_key(it, ["municipality", "dimos_name", "city"])
                     afm   = _first_key(it, ["afm", "vat_number", "tin"])
                     gemi  = _first_key(it, ["gemi_number", "registry_number", "commercial_registry_no"])
                     phone = _first_key(it, ["phone", "telephone", "contact_phone", "phone_number"])
                     email = _first_key(it, ["email", "contact_email", "email_address"])
                     website = _first_key(it, ["website","site","url","homepage"])
                     inc_date = _first_key(it, ["incorporation_date","foundation_date","establishment_date","founded_at","registration_date"])
+                    kad_codes = it.get("kad_codes") or it.get("kads") or it.get("kad") or ""
                     rows.append({
-                        "name": name, "address": addr, "city": city,
+                        "region_nomos": sel_nomos,
+                        "region_dimos": it.get("__region_dimos",""),
+                        "name": name, "address": addr, "city": city,
                         "afm": afm, "gemi": gemi, "phone": phone, "email": email,
-                        "website": website, "incorporation_date": inc_date
+                        "website": website, "incorporation_date": inc_date,
+                        "kad_codes": kad_codes,
                     })
                 gemi_df = pd.DataFrame(rows)
+                # client-side date filter (σε περίπτωση που ο server αγνόησε τα from/to)
+                if not gemi_df.empty and (date_from or date_to):
+                    dser = pd.to_datetime(gemi_df["incorporation_date"], errors="coerce").dt.date
+                    if date_from:
+                        try:
+                            dmin = pd.to_datetime(date_from, errors="coerce").date()
+                            gemi_df = gemi_df[dser >= dmin]
+                        except Exception:
+                            pass
+                    if date_to:
+                        try:
+                            dmax = pd.to_datetime(date_to, errors="coerce").date()
+                            gemi_df = gemi_df[dser <= dmax]
+                        except Exception:
+                            pass
+                # client-side kad filter (αν χρειαστεί)
+                if not gemi_df.empty and sel_kads:
+                    
+                    patt = "|".join([re.escape(k) for k in sel_kads])
+                    gemi_df = gemi_df[gemi_df["kad_codes"].astype(str).str.contains(patt, na=False, regex=True)]
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
 
-            # ---- ΠΛΗΡΗΣ ΕΞΑΓΩΓΗ ΑΝΑ ΝΟΜΟ ----
-            if do_export_nomos:
-                with st.spinner(f"Εξαγωγή όλων των επιχειρήσεων για τον Νομό «{sel_nomos}»..."):
-                    all_items = gemi_search_all(
-                        gemi_key,
-                        nomos_id=nomos_id, dimos_id=None,  # όλος ο Νομός
-                        status_id=status_id,
-                        name_part=name_part or None,
-                        page_size=200
-                    )
-                    export_df = gemi_items_to_df(all_items)
-                    if export_df.empty:
-                        st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
-                    else:
-                        st.success(f"Έτοιμο: {len(export_df)} εγγραφές για τον Νομό «{sel_nomos}».")
-                        st.dataframe(export_df.head(50), use_container_width=True)
-                        st.download_button(
-                            f"⬇️ Excel – Επιχειρήσεις Νομού «{sel_nomos}»",
-                            _to_excel_bytes(export_df),
-                            file_name=f"gemi_{sel_nomos}_businesses.xlsx"
-                        )
+            # ---- ΕΝΑ ΑΡΧΕΙΟ ΜΕ ΟΛΑ ΤΑ ΦΙΛΤΡΑ ----
+            if do_export_one:
+                with st.spinner("Εξαγωγή… αυτό μπορεί να πάρει λίγο αν υπάρχουν πολλές σελίδες."):
+                    dfs = []
+                    target_dimoi = None
+                    if sel_dimoi and not (len(sel_dimoi) == 1 and ALL_DM in sel_dimoi):
+                        target_dimoi = [dimos_label_to_id[x] for x in sel_dimoi if x in dimos_label_to_id]
+
+                    def _fetch_df(d_id, dimos_label):
+                        items = gemi_search_all(
+                            gemi_key,
+                            nomos_id=nomos_id, dimos_id=d_id, status_id=status_id,
+                            name_part=name_part or None,
+                            kad_list=sel_kads or None,
+                            date_from=(date_from or None), date_to=(date_to or None),
+                            page_size=200
+                        )
+                        df = gemi_items_to_df(items)
+                        if not df.empty:
+                            df.insert(0, "region_nomos", sel_nomos)
+                            df.insert(1, "region_dimos", dimos_label or "")
+                        return df
+
+                    if target_dimoi:
+                        for d_id in target_dimoi:
+                            dimos_label = next((nm for nm, _id in dimos_label_to_id.items() if _id == d_id), "")
+                            dfp = _fetch_df(d_id, dimos_label)
+                            if dfp is not None and not dfp.empty:
+                                dfs.append(dfp)
+                    else:
+                        # Όλοι οι Δήμοι με μία κλήση στο Νομό
+                        dfp = _fetch_df(None, "")
+                        if dfp is not None and not dfp.empty:
+                            dfs.append(dfp)
+
+                    if not dfs:
+                        st.warning("Δεν βρέθηκαν εγγραφές για εξαγωγή.")
+                    else:
+                        export_df = pd.concat(dfs, ignore_index=True).drop_duplicates()
+                        # Client-side date filter (safety)
+                        if (date_from or date_to) and "incorporation_date" in export_df:
+                            dser = pd.to_datetime(export_df["incorporation_date"], errors="coerce").dt.date
+                            if date_from:
+                                try:
+                                    dmin = pd.to_datetime(date_from, errors="coerce").date()
+                                    export_df = export_df[dser >= dmin]
+                                except Exception:
+                                    pass
+                            if date_to:
+                                try:
+                                    dmax = pd.to_datetime(date_to, errors="coerce").date()
+                                    export_df = export_df[dser <= dmax]
+                                except Exception:
+                                    pass
+
+                        st.success(f"Έτοιμο: {len(export_df)} εγγραφές στο αρχείο.")
+                        st.dataframe(export_df.head(50), use_container_width=True)
+                        st.download_button(
+                            "⬇️ Excel – Επιχειρήσεις (ένα αρχείο, με φίλτρα)",
+                            _to_excel_bytes(export_df),
+                            file_name=f"gemi_{sel_nomos}_filtered.xlsx"
+                        )
