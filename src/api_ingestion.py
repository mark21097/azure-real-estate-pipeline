import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load credentials from the .env file
load_dotenv()
api_key = os.getenv("API_KEY")

def fetch_loopnet_data():
    print("Connecting to LoopNet API...")
    
    # Updated URL based on your RapidAPI snippet
    url = "https://loopnet-api.p.rapidapi.com/loopnet/sale/searchByCity"
    
    # Updated payload based on your RapidAPI snippet
    payload = {
        "cityId": "11854", 
        "page": 1
    }

    headers = {
        "content-type": "application/json",
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "loopnet-api.p.rapidapi.com"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Extract the list of properties. (We default to 'data' or 'results' depending on LoopNet's exact JSON structure)
        # If it fails, the script will print the raw JSON so we can see what key we need to use.
        if 'data' in data:
            listings = data['data']
        elif 'results' in data:
            listings = data['results']
        else:
            listings = data # Fallback
            
        df = pd.DataFrame(listings)
        
        if not df.empty:
            print(f"✅ Successfully extracted {len(df)} commercial listings.")
            return df
        else:
            print("No listings found. Here is the raw response from the API to debug:")
            print(data)
            return None

    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

if __name__ == "__main__":
    property_df = fetch_loopnet_data()
    if property_df is not None:
        # Print the first 5 rows to ensure it worked
        print(property_df.head())