import psycopg2
import pandas as pd
import boto3
import os

# Environment Variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
S3_BUCKET = os.getenv("S3_BUCKET")
PROCESSED_FOLDER = "processed_flight_data/"

# Initialize S3 Client
s3 = boto3.client("s3")

def get_latest_file():
    objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=PROCESSED_FOLDER)
    files = [obj["Key"] for obj in objects.get("Contents", [])]
    
    return sorted(files)[-1] if files else None

def store_flight_data():
    latest_file = get_latest_file()
    
    if not latest_file:
        print("❌ No processed flight data found in S3.")
        return

    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_file)
    df = pd.read_csv(obj["Body"])
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS flight_prices (
            flight_number TEXT,
            airline TEXT,
            departure TEXT,
            arrival TEXT,
            price FLOAT,
            duration TEXT
        )
    """)
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO flight_prices (flight_number, airline, departure, arrival, price, duration)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Data successfully stored in PostgreSQL.")

if __name__ == "__main__":
    store_flight_data()
