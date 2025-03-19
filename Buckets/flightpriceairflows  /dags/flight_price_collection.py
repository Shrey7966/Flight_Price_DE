from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3
import os

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    "flight_price_collection",
    default_args=default_args,
    description="DAG to collect flight price data daily",
    schedule="0 0 * * *",  # Runs daily at midnight UTC
    catchup=False,
)

# Function to call API and save response
API_KEY = "fdcf5105b0mshb60125cb25ee57ep1565acjsndd740fc93e14"
def fetch_flight_prices():
    url = "https://flights-sky.p.rapidapi.com/flights/search-one-way"
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "flights-sky.p.rapidapi.com"
    }
    params = {
        "fromEntityId": "BLR",
        "toEntityId": "JFK",
        "departDate": datetime.today().strftime('%Y-%m-%d'),
        "currency": "INR",
        "cabinClass": "economy"
    }
    
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    
    # Save JSON response to S3
    s3_bucket = "flightpricedataanalysis"
    s3_key = f"flight_prices/{datetime.today().strftime('%Y-%m-%d')}/flight_data.json"
    
    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=json.dumps(data))
    print(f"Saved data to s3://{s3_bucket}/{s3_key}")

# Define Airflow Task
fetch_data_task = PythonOperator(
    task_id="fetch_flight_prices",
    python_callable=fetch_flight_prices,
    dag=dag,
)

fetch_data_task  # Run the task
