# Configuration
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.ho oks.s3 import S3Hook
from airflow.decorators import dag, task
import requests
import pandas as pd
import numpy as np
import json
import io
import logging
import boto3

API_KEY = "0YM4jri63wcYjrWU6ayQTJJlGje2mkZP"
stock_symbol = "AAPL"  # Add more stocks as needed
S3_BUCKET = "stock-etl-fmp"
S3_PREFIX = "stock_data/processed"
    

default_args={
     'owner':'nyima',
     'depends_on_past':False,
     'start_date':datetime(2024,3,1),
     'retries':3,
     'retry_delay':timedelta(minutes=5),
     'email_on_failure':False,
     'email_on_retry':False,
}

@dag(
    dag_id='stock_etl_process_by_nyima',
    default_args=default_args,
    description='daily_stock price for ETL pipeline',
    schedule='@daily',
    catchup=False,
)
def stock_etl_process_dag():
    @task    
    def extract_stock_data(stock_symbol,API_KEY):
        """
    Extract historical stock data (last 200 days) from FMP API.
    """
        print("Extracting stock data...")
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{stock_symbol}?serietype=line&apikey={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        historical_data = data.get('historical', [])
        
        if not historical_data:
            raise Exception("No historical data found for symbol: " + str(stock_symbol)) #to safely handle both string and list types

        df = pd.DataFrame(historical_data)
        df = df[['date', 'close']]
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Keep only the latest 200 days
        df = df.tail(200).reset_index(drop=True)
        
        # Fix: convert 'date' column to string format
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        return df.to_dict()
    
    @task
    def transform_stock_data(df_dict):
        print("Transforming stock data...")
        df = pd.DataFrame.from_dict(df_dict)
        df['SMA_50'] = df['close'].rolling(window=50).mean()
        df['SMA_200'] = df['close'].rolling(window=200).mean()
        df=df.replace({np.nan:None}) ## Replaces NaNs with None, which becomes null in JSON
        json_data=df.to_json()
        # df=df.where(pd.notnull(df),None)
        return df.to_dict()
 
    @task
    def load_stock_data(transformed_df_dict):
        print("Loading stock data to CSV...")

        # Convert dictionary back to DataFrame
        df = pd.DataFrame.from_dict(transformed_df_dict)

        # Save to CSV
        df.to_csv('/opt/airflow/dags/transformed_stock_data.csv', index=False)

        print("Stock data successfully written to CSV.")
        
    
        # S3 upload config
        bucket_name = 'stock-etl-fmp'
        s3_file_key = 'stock-data/stock_data.csv'
        aws_access_key = 'use your access key'
        aws_secret_key = 'use your secret key'
        csv_path='/opt/airflow/dags/transformed_stock_data.csv'
        
        print("Uploading to S3...")
        s3_client = boto3.client(
            's3',
            aws_access_key_id='use your access key',
            aws_secret_access_key='use your secret key'
        )
        s3_client.upload_file(csv_path, bucket_name, s3_file_key)
        print(f"File uploaded successfully to s3://{bucket_name}/{s3_file_key}")

 # Step 1: Extract
    extracted= extract_stock_data(stock_symbol, API_KEY)
    
    # Step 2: Transform
    df_transformed = transform_stock_data(extracted)
    
    
    # Step 3: Load to S3
    load_stock_data(df_transformed)


dag = stock_etl_process_dag()
