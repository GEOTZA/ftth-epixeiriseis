
import streamlit as st
import pandas as pd
import math
import time
from geopy.distance import geodesic
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

# ---------------------------------------------
# Helper: Get Google Maps search results
# ---------------------------------------------
def scrape_businesses(city, query, max_results=10):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=options)

    st.info(f"Αναζήτηση για '{query}' στην περιοχή '{city}' στο Google Maps...")

    search_url = f"https://www.google.com/maps/search/{query}+{city}"
    driver.get(search_url)
    time.sleep(5)

    results = []

    for _ in range(max_results):
        time.sleep(2)
        elements = driver.find_elements(By.CSS_SELECTOR, 'div.Nv2PK')
        for el in elements:
            try:
                name = el.find_element(By.CSS_SELECTOR, 'a.hfpxzc').text
                address = el.find_element(By.CLASS_NAME, 'W4Efsd').text
                href = el.find_element(By.CSS_SELECTOR, 'a.hfpxzc').get_attribute("href")
                if '/place/' in href and '@' in href:
                    coords_part = href.split('@')[1].split(',')[:2]
                    lat, lon = float(coords_part[0]), float(coords_part[1])
                    results.append({
                        'name': name,
                        'address': address,
                        'latitude': lat,
                        'longitude': lon
                    })
            except:
                continue
        break  # Stop after the first page for demo purposes

    driver.quit()
    return pd.DataFrame(results)

# ---------------------------------------------
# Main App
# ---------------------------------------------
st.title("FTTH Επιχειρησιακό Scraper (Nova)")

# Step 1: Load FTTH Nova Coordinates File
ftth_file = st.file_uploader("🔼 Ανέβασε το αρχείο Excel με τα σημεία FTTH Nova", type=['xlsx'])
if ftth_file:
    ftth_df = pd.read_excel(ftth_file)
    ftth_df = ftth_df.dropna(subset=["latitude", "longitude"])

    # Step 2: Input City & Business Type
    city = st.text_input("Πόλη", value="Ηράκλειο")
    category = st.text_input("Κατηγορία Επιχειρήσεων (π.χ. καφέ, φαρμακείο)", value="καφέ")
    radius = st.slider("Απόσταση ελέγχου (μέτρα)", min_value=1, max_value=100, value=5)

    if st.button("Ξεκίνα Αναζήτηση"):
        business_df = scrape_businesses(city, category)
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
                        'Latitude': biz['latitude'],
                        'Longitude': biz['longitude'],
                        'Απόσταση (μ.)': round(distance, 2),
                        'FTTH διαθέσιμο': row.get('availability', 'N/A'),
                        'Πάροχος': 'Nova'
                    })
                    break  # Stop after first match

        result_df = pd.DataFrame(matched_rows)
        st.success(f"Βρέθηκαν {len(result_df)} επιχειρήσεις εντός κάλυψης FTTH Nova.")
        st.dataframe(result_df)

        if not result_df.empty:
            st.download_button("⬇️ Κατέβασε τα αποτελέσματα σε Excel", result_df.to_excel(index=False), file_name="matched_businesses_ftth_nova.xlsx")
