from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import boto3
import os

# Environment Variables
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_FOLDER = "raw_flight_data/"
PROCESSED_FOLDER = "processed_flight_data/"


s3 = boto3.client("s3")
# Initialize Spark
# Get AWS credentials from environment variables
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")


#Get JAR directory from GitHub Actions workspace
jar_dir = os.getenv("GITHUB_WORKSPACE", ".") + "/jars"

spark = SparkSession.builder \
    .appName("FlightPriceAnalysis") \
    .config("spark.jars", f"{jar_dir}/hadoop-aws-3.3.4.jar,{jar_dir}/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

df = spark.read.option("multiline", "true").json("s3a:/flight-price-db-github/raw_flight_data/")


def process_flight_data():
    # List all JSON files in the raw folder
    objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=RAW_FOLDER)
    files = [obj["Key"] for obj in objects.get("Contents", [])]

    if not files:
        print("❌ No raw flight data files found in S3.")
        return

    # Read and process the latest file
    latest_file = sorted(files)[-1]
    s3_uri = f"s3a://{S3_BUCKET}/{latest_file}"
    
    df = spark.read.option("multiline", "true").json(s3_uri)
    df = df.select("flight_number", "airline", "departure", "arrival", "price", "duration")

    # Save cleaned data back to S3
    output_path = f"s3a://{S3_BUCKET}/{PROCESSED_FOLDER}flight_prices_cleaned.csv"
    df.write.mode("overwrite").option("header", "true").csv(output_path)

    print(f"✅ Processed data saved to: {output_path}")

if __name__ == "__main__":
    process_flight_data()
