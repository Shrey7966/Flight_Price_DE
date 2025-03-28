import os
import boto3
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sqlalchemy import create_engine

# Load environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
S3_BUCKET = "flight-price-etl-github"
S3_PREFIX = "flight_prices/"
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Initialize Boto3 S3 client
s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def fetch_flight_prices():
    url = "https://flights-sky.p.rapidapi.com/flights/search-one-way"
querystring = {"fromEntityId":"BLR","toEntityId":"DFW","departDate":"2025-05-31","market":"IN","locale":"en-US","currency":"INR","stops":"direct,1stop,2stops","cabinClass":"economy"}

headers = {
	"x-rapidapi-key": "fdcf5105b0mshb60125cb25ee57ep1565acjsndd740fc93e14",
	"x-rapidapi-host": "flights-sky.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch flight prices", response.text)
        return None

def save_to_s3(data, filename):
    file_path = f"/tmp/{filename}"
    pd.DataFrame(data).to_csv(file_path, index=False)
    s3.upload_file(file_path, S3_BUCKET, f"{S3_PREFIX}{filename}")
    print(f"Uploaded {filename} to S3")

def process_data():
    spark = SparkSession.builder.appName("FlightPriceProcessing").getOrCreate()
    s3_path = f"s3a://{S3_BUCKET}/{S3_PREFIX}"
    df = spark.read.option("header", "true").csv(s3_path)
    df = df.withColumn("price", col("price").cast("float"))
    df = df.filter(col("price").isNotNull())
    return df.toPandas()

def save_to_db(df):
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
    df.to_sql("flight_prices", engine, if_exists="replace", index=False)
    print("Data saved to PostgreSQL")

if __name__ == "__main__":
    data = fetch_flight_prices()
    if data:
        save_to_s3(data, "flight_prices.csv")
        processed_df = process_data()
        save_to_db(processed_df)
