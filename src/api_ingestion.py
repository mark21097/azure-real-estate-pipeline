import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv
import logging

# Set up Logging to catch issues during API calls
log_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'pipeline.log')
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

load_dotenv()
api_key = os.getenv("API_KEY")

def fetch_loopnet_data():
    
    logging.info("Fetching listing IDs from the map search...")
    
    # 1. First Endpoint: Get the IDs
    search_url = "https://loopnet-api.p.rapidapi.com/loopnet/sale/searchByCity"
    search_payload = {"cityId": "11854", "page": 1}
    headers = {
        "content-type": "application/json",
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "loopnet-api.p.rapidapi.com"
    }

    try:
        response = requests.post(search_url, json=search_payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        listings = data.get('data', data.get('results', data))
        
        # We grab just the top 5 so we don't hit the free-tier API rate limit
        top_listings = listings[:50]
        logging.info(f"✅ Found {len(listings)} listings. Extracting details for the top {len(top_listings)}...")

        enriched_data = []
        
        # 2. Second Endpoint: Get the Details for each ID
        details_url = "https://loopnet-api.p.rapidapi.com/loopnet/property/SaleDetails"

        for item in top_listings:
            listing_id = str(item.get('listingId'))
            coords = str(item.get('coordinations', ''))
            
            logging.info(f"  -> Fetching details for Listing ID: {listing_id}")
            
            detail_payload = {"listingId": listing_id}
            detail_response = requests.post(details_url, json=detail_payload, headers=headers)
            
            if detail_response.status_code == 200:
                detail_data = detail_response.json()
                
                # Unwrap the data
                if isinstance(detail_data, dict):
                    if 'data' in detail_data:
                        detail_data = detail_data['data']
                    elif 'results' in detail_data:
                        detail_data = detail_data['results']
                        
                if isinstance(detail_data, list):
                    prop = detail_data[0] if len(detail_data) > 0 else {}
                else:
                    prop = detail_data
                    
                if not isinstance(prop, dict):
                    prop = {}
                    
                # --- THE PERFECT JSON MAPPING ---
                enriched_data.append({
                    'ListingID': listing_id,
                    'Address': str(prop.get('title', 'Unknown'))[:250],
                    'PropertyType': str(prop.get('category', 'Unknown'))[:100],
                    'Price': prop.get('price', 0),
                    'SquareFootage': prop.get('propertyFacts', {}).get('buildingSize', 0),
                    'Coordinates': coords
                })
            
            time.sleep(1)

        df = pd.DataFrame(enriched_data)
        
        if not df.empty:
            # Clean prices and sqft
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0)
            df['SquareFootage'] = df['SquareFootage'].astype(str).str.replace(',', '').str.replace(' SF', '')
            df['SquareFootage'] = pd.to_numeric(df['SquareFootage'], errors='coerce').fillna(0)
            
            # --- NEW: LAT/LONG PARSING ---
            # Remove the brackets [[ ]]
            # --- NEW: BULLETPROOF LAT/LONG PARSING ---
            # Remove the brackets [[ ]]
            df['Coordinates'] = df['Coordinates'].astype(str).str.replace('[', '', regex=False).str.replace(']', '', regex=False)
            
            # Split the string by the comma into a temporary dataframe
            # This handles polygons by letting Pandas make as many columns as it needs
            coords_split = df['Coordinates'].str.split(',', expand=True)
            
            # Safely grab ONLY the first two columns (Index 0 = Longitude, Index 1 = Latitude)
            df['Longitude'] = pd.to_numeric(coords_split[0], errors='coerce')
            
            # Use an 'if' statement just in case a property has completely empty coordinates
            if 1 in coords_split.columns:
                df['Latitude'] = pd.to_numeric(coords_split[1], errors='coerce')
            else:
                df['Latitude'] = None
            # -----------------------------
            
            # --- NEW: SAVE TO LOCAL STAGING ---
            # Ensure the data directory exists
            os.makedirs('../data', exist_ok=True)
            
            # Save as CSV (you can also use .to_parquet('.../raw_listings.parquet'))
            file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw_listings.csv')
            df.to_csv(file_path, index=False)
            logging.info(f"📁 Data staged locally at: {file_path}")
            # ----------------------------------
            
            logging.info(f"✅ Pipeline complete! Enriched {len(df)} commercial listings.")
            return df
        else:
            return None

    except Exception as e:
        logging.error(f"❌ Error fetching data: {e}")
        return None

if __name__ == "__main__":
    property_df = fetch_loopnet_data()
    if property_df is not None:
        print(property_df)