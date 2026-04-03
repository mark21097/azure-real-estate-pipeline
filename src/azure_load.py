import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from api_ingestion import fetch_loopnet_data

# Load environment variables
load_dotenv()

# Build the connection string for Azure SQL
server = os.getenv("AZURE_SERVER")
database = os.getenv("AZURE_DB")
username = os.getenv("AZURE_USER")
password = os.getenv("AZURE_PWD")
driver = '{SQL Server}' # Standard Azure SQL driver

def load_data_to_azure(df):
    if df is None or df.empty:
        print("No data to load.")
        return

    print("Connecting to Azure SQL Database...")
    try:
        # Create the connection
        conn_str = f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected successfully!")

        # Create a simple table if it doesn't exist yet
        # We are keeping it simple based on the data you currently have
        # Drop the table if it already exists so we can recreate it with the right column size
        create_table_query = """
        IF EXISTS (SELECT * FROM sysobjects WHERE name='CommercialListings' AND xtype='U')
            DROP TABLE CommercialListings;

        CREATE TABLE CommercialListings (
            ListingID VARCHAR(50) PRIMARY KEY,
            Coordinates VARCHAR(MAX) -- Changed from 255 to MAX to hold huge arrays
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert the data into the table
        print("Inserting records...")
        for index, row in df.iterrows():
            # Convert list of coordinates to a string so it fits in SQL
            coord_str = str(row['coordinations']) 
            
            # Use T-SQL MERGE or simple IF NOT EXISTS to avoid duplicate entries
            insert_query = """
            IF NOT EXISTS (SELECT 1 FROM CommercialListings WHERE ListingID = ?)
            BEGIN
                INSERT INTO CommercialListings (ListingID, Coordinates)
                VALUES (?, ?)
            END
            """
            cursor.execute(insert_query, row['listingId'], row['listingId'], coord_str)
        
        conn.commit()
        print("✅ All records inserted successfully!")

    except Exception as e:
        print(f"❌ Database error: {e}")
    finally:
        # Always close the connection
        if 'conn' in locals():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # 1. Extract
    df = fetch_loopnet_data()
    
    # 2. Load
    load_data_to_azure(df)