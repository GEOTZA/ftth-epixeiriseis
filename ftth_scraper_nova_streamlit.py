
import streamlit as st
import pandas as pd
import geopandas as gpd
from geopy.distance import geodesic
import io

st.set_page_config(page_title="FTTH Matching από GeoJSON", layout="centered")
st.title("📡 FTTH Επιχειρησιακό Matching Tool (GeoJSON/CSV)")

# Upload FTTH file
st.markdown("### 📥 1. Ανέβασε αρχείο FTTH (.geojson ή .csv με `latitude`, `longitude`)")
ftth_file = st.file_uploader("Αρχείο FTTH", type=["geojson", "csv"], key="ftth")

# Upload business file
st.markdown("### 📥 2. Ανέβασε αρχείο Excel Επιχειρήσεων (με `name`, `latitude`, `longitude`)")
biz_file = st.file_uploader("Excel Επιχειρήσεων", type=["xlsx"], key="biz")

distance_limit = st.number_input("📏 Απόσταση εντοπισμού (σε μέτρα)", min_value=5, max_value=500, value=50, step=5)

if ftth_file and biz_file:
    # Διαβάζουμε τα FTTH σημεία
    if ftth_file.name.endswith(".geojson"):
        ftth_gdf = gpd.read_file(ftth_file)
        if ftth_gdf.geometry.geom_type.isin(["Point"]).all():
            ftth_df = pd.DataFrame({
                "latitude": ftth_gdf.geometry.y,
                "longitude": ftth_gdf.geometry.x
            })
        else:
            st.error("❌ Το GeoJSON πρέπει να περιέχει μόνο σημεία (Point features).")
            st.stop()
    elif ftth_file.name.endswith(".csv"):
        ftth_df = pd.read_csv(ftth_file)
        if not {"latitude", "longitude"}.issubset(ftth_df.columns):
            st.error("❌ Το CSV πρέπει να περιέχει στήλες: latitude, longitude.")
            st.stop()
    else:
        st.error("❌ Μη υποστηριζόμενο format αρχείου.")
        st.stop()

    # Διαβάζουμε τις επιχειρήσεις
    biz_df = pd.read_excel(biz_file)
    if not {"name", "latitude", "longitude"}.issubset(biz_df.columns):
        st.error("❌ Το Excel πρέπει να περιέχει στήλες: name, latitude, longitude.")
        st.stop()

    # Υπολογισμός αποστάσεων
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
                break  # Αν βρεθεί κοντινό σημείο, δεν ελέγχει άλλα
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
            file_name="ftth_matched_businesses_geojson.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("⚠️ Δεν βρέθηκαν επιχειρήσεις εντός του ορίου απόστασης.")
