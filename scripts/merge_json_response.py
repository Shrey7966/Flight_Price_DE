import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from functools import reduce


# Initialize Boto3 S3 client
s3 = boto3.client("s3")

# S3 bucket and prefix
s3_bucket = "flight-price-etl-github"
s3_prefix = f"flight_prices_latest/{fetch_date}/{depart_date}.json"  # --> Folder containing all fetch dates

# List all fetch dates available in S3
response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix, Delimiter='/')

# Extract available fetch dates
fetch_dates = [obj['Prefix'].split('/')[-2] for obj in response.get('CommonPrefixes', [])]
print(" Available fetch dates:", fetch_dates)

# Initialize Spark session (if not already initialized in Databricks)
spark = SparkSession.builder.appName("FlightPriceAnalysis").getOrCreate()

# Configure Spark to access S3
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# Read data for each fetch date dynamically
dfs = []
for fetch_date in fetch_dates:
    s3_path = f"s3a://{s3_bucket}/{s3_prefix}{fetch_date}/*.json"  # Load all departure dates under each fetch date
    print(f"Reading data from: {s3_path}")
    
    try:
        df = spark.read.json(s3_path)
        if df.head(1):  # Ensure DataFrame is not empty
            df = df.withColumn("fetch_date", lit(fetch_date))  # Add fetch_date as a column
            dfs.append(df)
        else:
            print(f" No data found at {s3_path}")
    except Exception as e:
        print(f" Error reading data for {fetch_date}: {str(e)}")

# Merge all DataFrames if any data was read
if dfs:
    flight_data = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
    flight_data.show()  # Show sample data
else:
    print(" No valid data found in S3.")


flight_data.coalesce(1).write.mode("overwrite").json("s3://flight-price-etl-github/merged-flight-data")
