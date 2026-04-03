# azure-real-estate-pipeline

## Project Overview
This project is an automated, end-to-end data pipeline designed to extract 3rd-party real estate market data, transform it for analytical consistency, and load it into a cloud data warehouse to drive actionable business insights. 

The goal of this project is to demonstrate modern data engineering practices by connecting external data sources to enterprise-grade Microsoft infrastructure (Azure SQL) and visualizing the resulting data models in Power BI.

## Technology Stack
* **Data Ingestion:** Python, REST APIs (Requests library)
* **Data Transformation:** Python (Pandas)
* **Data Storage:** Azure SQL Database (T-SQL)
* **Analytics & Visualization:** Power BI Desktop, DAX
* **Security:** Environment variables (`python-dotenv`) for credential management

## Architecture Flow
1.  **Extract:** A Python script connects to a 3rd-party REST API to pull live property and market data in JSON format.
2.  **Transform:** The data is parsed, cleaned, and structured using Pandas to ensure data integrity and model consistency.
3.  **Load:** Using `pyodbc`, the cleaned data is securely loaded into an Azure SQL database.
4.  **Analyze:** Power BI connects directly to the Azure SQL endpoint, utilizing custom DAX measures to aggregate data and display trends in an interactive dashboard.

## Key Features & DAX Measures
* **Automated Data Connectivity:** Removes the need for manual CSV exports by programmatically pulling from external APIs.
* **Cloud Infrastructure:** Leverages Azure SQL for scalable, secure data storage.
* **Dynamic DAX Aggregations:** * *Average Price per Square Foot*
    * *Rolling Market Valuations*
    * *Property Type Distribution*

## Setup & Installation

1. Clone the repository:
   ```bash
   git clone [https://github.com/yourusername/azure-real-estate-pipeline.git](https://github.com/yourusername/azure-real-estate-pipeline.git)

2. 
   ```bash
   cd azure-real-estate-pipeline
    install -r requirements.txt

3. ```bash
   API_KEY=your_rapidapi_key
   AZURE_SERVER=your_server.database.windows.net
   AZURE_DB=your_database_name
   AZURE_USER=your_admin_username
   AZURE_PWD=your_secure_password

4. ```bash
   python src/api_ingestion.py