# Daily-Stock-Price-ETL-Pipeline-with-Apache-Airflow-using-API
Project Objective
The goal of this project is to build an ETL (Extract, Transform, Load) pipeline that:
- Extract daily stock price data using the Financial Modeling Prep (FMP) API.
- Transforms the data using pandas to calculate moving averages.
- Loads the processed data into an Amazon S3 bucket.

  Project Steps
1. Extract
Retrieve 200 days of historical daily closing prices (e.g., for stocks like AAPL or MSFT) using
the FMP API.
2. Transform
Use the pandas library to compute the 50-day and 200-day simple moving averages (SMA)
for the extracted stock data.
3. Load
Save the transformed data as a CSV file and upload it to an Amazon S3 bucket.
Scheduling
Automate the ETL pipeline to run daily using a Directed Acyclic Graph (DAG) in Apache
Airflow.

The Financial Modeling Prep (FMP) API is chosen for this project due to its free tier
(offering 250 requests per day) and focus on stock data.
API Documentation: https://financialmodelingprep.com/developer/docs/
