import streamlit as st
import pandas as pd
from geopy.distance import geodesic
import io

st.set_page_config(page_title="FTTH Επιχειρήσεις Matching", layout="centered")
st.title("📡 FTTH Επιχειρησιακό Matching Tool")

st.markdown("### 📥 1. Ανέβασε το Excel με τα σημεία FTTH Nova (πρέπει να έχει στήλες `latitude`, `longitude`)")
ftth_file = st.file_uploader("FTTH Nova Excel", type=["xlsx"], key="ftth")

st.markdown("### 📥 2. Ανέβασε το Excel με τις επιχειρήσεις (πρέπει να έχει στήλες `name`, `latitude`, `longitude`)")
biz_file = st.file_uploader("Excel Επιχειρήσεων", type=["xlsx"], key="biz")

distance_limit = st.number_input("📏 Απόσταση εντοπισμού (σε μέτρα)", min_value=1, max_value=1000, value=50, step=5)

if ftth_file and biz_file:
    ftth_df = pd.read_excel(ftth_file)
    biz_df = pd.read_excel(biz_file)

    if not {"latitude", "longitude"}.issubset(ftth_df.columns) or not {"latitude", "longitude", "name"}.issubset(biz_df.columns):
        st.error("❌ Τα αρχεία πρέπει να περιέχουν τις στήλες: latitude, longitude (και name για επιχειρήσεις).")
    else:
        matches = []

        for _, biz in biz_df.iterrows():
            biz_location = (biz["latitude"], biz["longitude"])
            for _, ftth in ftth_df.iterrows():
                ftth_location = (ftth["latitude"], ftth["longitude"])
                if geodesic(biz_location, ftth_location).meters <= distance_limit:
                    matches.append({
                        "Επιχείρηση": biz["name"],
                        "Επιχείρηση_lat": biz["latitude"],
                        "Επιχείρηση_lon": biz["longitude"],
                        "FTTH_lat": ftth["latitude"],
                        "FTTH_lon": ftth["longitude"],
                        "Απόσταση (m)": round(geodesic(biz_location, ftth_location).meters, 2)
                    })
                    break

        if matches:
            result_df = pd.DataFrame(matches)
            st.success(f"✅ Βρέθηκαν {len(result_df)} επιχειρήσεις εντός {distance_limit} μέτρων από FTTH σημεία.")
            st.dataframe(result_df)

            output = io.BytesIO()
            result_df.to_excel(output, index=False, engine='openpyxl')
            output.seek(0)

            st.download_button(
                label="⬇️ Κατέβασε τα αποτελέσματα σε Excel",
                data=output,
                file_name="ftth_matched_businesses.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("⚠️ Δεν βρέθηκαν επιχειρήσεις εντός του ορίου απόστασης.")

