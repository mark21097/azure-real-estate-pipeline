import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from api_ingestion import fetch_loopnet_data

load_dotenv()

server = os.getenv("AZURE_SERVER")
database = os.getenv("AZURE_DB")
username = os.getenv("AZURE_USER")
password = os.getenv("AZURE_PWD")
driver = '{SQL Server}' 

def load_data_to_azure(df):
    if df is None or df.empty:
        print("No data to load.")
        return

    print("Connecting to Azure SQL Database...")
    try:
        conn_str = f"DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✅ Connected successfully!")

        # Recreate the table with our new analytical columns
        # Recreate the table with Lat/Long
        create_table_query = """
        IF EXISTS (SELECT * FROM sysobjects WHERE name='CommercialListings' AND xtype='U')
            DROP TABLE CommercialListings;

        CREATE TABLE CommercialListings (
            ListingID VARCHAR(50) PRIMARY KEY,
            Address VARCHAR(255),
            PropertyType VARCHAR(100),
            Price FLOAT,
            SquareFootage FLOAT,
            Coordinates VARCHAR(MAX),
            Latitude FLOAT,
            Longitude FLOAT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        print("Inserting enriched records...")
        for index, row in df.iterrows():
            insert_query = """
            INSERT INTO CommercialListings (ListingID, Address, PropertyType, Price, SquareFootage, Coordinates, Latitude, Longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_query, 
                           row['ListingID'], 
                           row['Address'], 
                           row['PropertyType'], 
                           row['Price'], 
                           row['SquareFootage'], 
                           row['Coordinates'],
                           row['Latitude'],
                           row['Longitude'])
        
        conn.commit()
        print("✅ All records inserted successfully!")

    except Exception as e:
        print(f"❌ Database error: {e}")
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    df = fetch_loopnet_data()
    load_data_to_azure(df)