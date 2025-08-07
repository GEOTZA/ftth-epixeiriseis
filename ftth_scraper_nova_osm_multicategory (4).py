
import streamlit as st
import pandas as pd
import requests
from geopy.distance import geodesic

# ---------------------------------------------
# Helper: Get businesses using Nominatim (OpenStreetMap)
# ---------------------------------------------
def get_businesses(city, categories, max_results=30):
    st.info(f"Αναζήτηση για {', '.join(categories)} στην περιοχή '{city}' μέσω OpenStreetMap...")
    url = "https://nominatim.openstreetmap.org/search"
    results = []

    for category in categories:
        params = {
            "q": f"{category}, {city}, Greece",
            "format": "json",
            "limit": max_results,
            "addressdetails": 1
        }
        response = requests.get(url, params=params, headers={"User-Agent": "ftth-streamlit-app"})
        if response.status_code != 200:
            continue
        data = response.json()
        for item in data:
            results.append({
                "name": item.get("display_name", "N/A"),
                "latitude": float(item["lat"]),
                "longitude": float(item["lon"]),
                "category": category,
                "address": item.get("address", {}).get("road", "N/A")
            })
    return pd.DataFrame(results)

# ---------------------------------------------
# Main App
# ---------------------------------------------
st.title("FTTH Επιχειρησιακό Scraper (Nova) - OpenStreetMap έκδοση")

# Step 1: Load FTTH Nova Coordinates File
ftth_file = st.file_uploader("🔼 Ανέβασε το αρχείο Excel με τα σημεία FTTH Nova", type=['xlsx'])
if ftth_file:
    ftth_df = pd.read_excel(ftth_file)

    if "latitude" not in ftth_df.columns or "longitude" not in ftth_df.columns:
        st.error("Το αρχείο πρέπει να περιέχει τις στήλες 'latitude' και 'longitude'")
        st.stop()

    ftth_df = ftth_df.dropna(subset=["latitude", "longitude"])

    # Step 2: Input City & Business Types
    city = st.text_input("Πόλη", value="Ηράκλειο")
    category_input = st.text_input("Κατηγορίες Επιχειρήσεων (χώρισε με κόμμα, π.χ. καφέ, φαρμακείο)", value="καφέ, φαρμακείο")
    categories = [cat.strip() for cat in category_input.split(",") if cat.strip()]
    radius = st.slider("Απόσταση ελέγχου (μέτρα)", min_value=1, max_value=100, value=5)

    if st.button("Ξεκίνα Αναζήτηση"):
        business_df = get_businesses(city, categories)
        matched_rows = []

        for _, biz in business_df.iterrows():
            biz_coords = (biz['latitude'], biz['longitude'])
            for _, row in ftth_df.iterrows():
                ftth_coords = (row['latitude'], row['longitude'])
                distance = geodesic(biz_coords, ftth_coords).meters
                if distance <= radius:
                    matched_rows.append({
                        'Επιχείρηση': biz['name'],
                        'Διεύθυνση': biz['address'],
                        'Κατηγορία': biz['category'],
                        'Latitude': biz['latitude'],
                        'Longitude': biz['longitude'],
                        'Απόσταση (μ.)': round(distance, 2),
                        'FTTH διαθέσιμο': row.get('availability', 'N/A'),
                        'Πάροχος': 'Nova'
                    })
                    break

        result_df = pd.DataFrame(matched_rows)
        st.success(f"Βρέθηκαν {len(result_df)} επιχειρήσεις εντός κάλυψης FTTH Nova.")
        st.dataframe(result_df)

        if not result_df.empty:
            st.download_button("⬇️ Κατέβασε τα αποτελέσματα σε Excel", result_df.to_excel(index=False), file_name="matched_businesses_ftth_nova.xlsx")

else:
    st.warning("Ανέβασε πρώτα το Excel αρχείο FTTH Nova.")

st.markdown("---")
st.caption("🔍 Χρήση OpenStreetMap Nominatim API - Υποστήριξη πολλαπλών κατηγοριών")
