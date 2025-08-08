
import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic
import io
import time

st.set_page_config(page_title="FTTH Geocoding & Matching", layout="centered")
st.title("📍 Geocoding & Έλεγχος Επιχειρήσεων σε FTTH Κάλυψη")

# -----------------------------
# Geocoding function using OpenStreetMap Nominatim
# -----------------------------
def geocode_address(address):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "countrycodes": "gr",
        "accept-language": "el"
    }
    try:
        response = requests.get(url, params=params, headers={"User-Agent": "ftth-app"})
        response.raise_for_status()
        data = response.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        st.warning(f"⚠️ Σφάλμα στη διεύθυνση: {address} ({e})")
    return None, None

# -----------------------------
# Upload files
# -----------------------------
st.markdown("### 📥 1. Ανέβασε Excel Επιχειρήσεων (στήλες: name, address, city)")
biz_file = st.file_uploader("Excel με Διευθύνσεις", type=["xlsx"])

st.markdown("### 📥 2. Ανέβασε CSV ή Excel με FTTH σημεία (στήλες: latitude, longitude)")
ftth_file = st.file_uploader("FTTH Nova", type=["csv", "xlsx"])

distance_limit = st.number_input("📏 Μέγιστη απόσταση (σε μέτρα)", min_value=5, max_value=500, value=50, step=5)

if biz_file and ftth_file:
    biz_df = pd.read_excel(biz_file)
    if not {"name", "site.company_insights.address", "site.company_insights.city"}.issubset(biz_df.columns):
        st.error("❌ Το αρχείο επιχειρήσεων πρέπει να έχει τις στήλες: name, site.company_insights.address, site.company_insights.city")
        st.stop()

    if ftth_file.name.endswith(".csv"):
        ftth_df = pd.read_csv(ftth_file)
    else:
        ftth_df = pd.read_excel(ftth_file)

    if not {"latitude", "longitude"}.issubset(ftth_df.columns):
        st.error("❌ Το αρχείο FTTH πρέπει να έχει στήλες: latitude, longitude")
        st.stop()

    st.info("🔄 Γίνεται geocoding στις διευθύνσεις...")
    geocoded = []
    for i, row in biz_df.iterrows():
        full_address = f"{row['site.company_insights.address']}, {row['site.company_insights.city']}"
        lat, lon = geocode_address(full_address)
        time.sleep(1)  # Respect rate limits of Nominatim
        if lat and lon:
            geocoded.append({
                "Επιχείρηση": row["name"],
                "Διεύθυνση": full_address,
                "Latitude": lat,
                "Longitude": lon
            })

    if not geocoded:
        st.warning("⚠️ Δεν βρέθηκαν έγκυρες γεωγραφικές τοποθεσίες.")
        st.stop()

    geo_df = pd.DataFrame(geocoded)

    # Matching with FTTH points
    st.info("📡 Έλεγχος επιχειρήσεων εντός FTTH κάλυψης...")
    matches = []

    for _, biz in geo_df.iterrows():
        biz_coords = (biz["Latitude"], biz["Longitude"])
        for _, ftth in ftth_df.iterrows():
            ftth_coords = (ftth["latitude"], ftth["longitude"])
            dist = geodesic(biz_coords, ftth_coords).meters
            if dist <= distance_limit:
                matches.append({
                    "Επιχείρηση": biz["Επιχείρηση"],
                    "Διεύθυνση": biz["Διεύθυνση"],
                    "Latitude": biz["Latitude"],
                    "Longitude": biz["Longitude"],
                    "Απόσταση από FTTH (m)": round(dist, 2)
                })
                break

    if matches:
        result_df = pd.DataFrame(matches)
        st.success(f"✅ Βρέθηκαν {len(result_df)} επιχειρήσεις εντός κάλυψης FTTH.")
        st.dataframe(result_df)

        output = io.BytesIO()
        result_df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)

        st.download_button(
            label="⬇️ Κατέβασε τα αποτελέσματα σε Excel",
            data=output,
            file_name="ftth_matched_geocoded.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("⚠️ Δεν βρέθηκαν επιχειρήσεις εντός FTTH κάλυψης.")
