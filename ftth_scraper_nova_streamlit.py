
import streamlit as st
import pandas as pd
from geopy.distance import geodesic
import io

st.set_page_config(page_title="FTTH Matching από CSV", layout="centered")
st.title("📡 FTTH Επιχειρησιακό Matching Tool (CSV μόνο)")

# Upload FTTH CSV file
st.markdown("### 📥 1. Ανέβασε CSV αρχείο FTTH (στήλες `latitude`, `longitude`)")
ftth_file = st.file_uploader("Αρχείο FTTH (.csv)", type=["csv"], key="ftth_csv")

# Upload businesses Excel
st.markdown("### 📥 2. Ανέβασε Excel επιχειρήσεων (στήλες `name`, `latitude`, `longitude`)")
biz_file = st.file_uploader("Αρχείο Επιχειρήσεων (.xlsx)", type=["xlsx"], key="biz_excel")

distance_limit = st.number_input("📏 Μέγιστη απόσταση σε μέτρα", min_value=5, max_value=500, value=50, step=5)

if ftth_file and biz_file:
    try:
        ftth_df = pd.read_csv(ftth_file)
        if not {"latitude", "longitude"}.issubset(ftth_df.columns):
            st.error("❌ Το FTTH αρχείο πρέπει να έχει στήλες: latitude, longitude.")
            st.stop()
    except Exception as e:
        st.error(f"❌ Σφάλμα στο FTTH αρχείο: {e}")
        st.stop()

    try:
        biz_df = pd.read_excel(biz_file)
        if not {"name", "latitude", "longitude"}.issubset(biz_df.columns):
            st.error("❌ Το αρχείο επιχειρήσεων πρέπει να έχει στήλες: name, latitude, longitude.")
            st.stop()
    except Exception as e:
        st.error(f"❌ Σφάλμα στο αρχείο επιχειρήσεων: {e}")
        st.stop()

    matches = []
    for _, biz in biz_df.iterrows():
        biz_point = (biz["latitude"], biz["longitude"])
        for _, ftth in ftth_df.iterrows():
            ftth_point = (ftth["latitude"], ftth["longitude"])
            dist = geodesic(biz_point, ftth_point).meters
            if dist <= distance_limit:
                matches.append({
                    "Επιχείρηση": biz["name"],
                    "Επιχείρηση_lat": biz["latitude"],
                    "Επιχείρηση_lon": biz["longitude"],
                    "FTTH_lat": ftth["latitude"],
                    "FTTH_lon": ftth["longitude"],
                    "Απόσταση (m)": round(dist, 2)
                })
                break  # Stop if one match is found

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
            file_name="ftth_matches_from_csv.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("⚠️ Δεν βρέθηκαν επιχειρήσεις εντός του επιλεγμένου ορίου.")
