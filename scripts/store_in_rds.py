import re
import psycopg2
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Store Flight Data in RDS") \
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<AWS_ACCESS_KEY_ID>")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<AWS_SECRET_ACCESS_KEY>")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")


df_post = spark.read.csv("s3a://flight-price-etl-github/flight_prices_test.csv/", header=True, inferSchema=True)

# Convert to Pandas DataFrame for PostgreSQL storage
pandas_df = df_post.toPandas()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="flightprice_db_github",
    user="shreyas",
    password="SG7966.cgi",
    host="16.171.119.139",
    port="5432"
)
cur = conn.cursor()

# Function to clean price (convert ₹ 51,898 → 51898)
def clean_price(price_str):
    return int(re.sub(r"[^\d]", "", price_str)) if isinstance(price_str, str) else price_str

# Insert data into PostgreSQL table
for _, row in pandas_df.iterrows():
    try:
        fetch_date = row["fetch_date"]
        flight_number = row["flightNumber"]
        origin = row["origin"]
        destination = row["destination"]
        price = clean_price(row["formattedPrice"])
        departure_time = row["departure"]
        duration = row["duration"]
        marketingCarrier = row["marketingCarrier"]
        operatingCarrier = row["operatingCarrier"]
        layover = row["layover"]
        numStops = row["numStops"]

        # Execute the INSERT statement for each row
        cur.execute("""
            INSERT INTO flight_prices (fetch_date, flight_number, origin, destination, price, departure_time, duration, marketingCarrier, operatingCarrier, layover, numStops)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (fetch_date, flight_number, origin, destination, price, departure_time, duration, marketingCarrier, operatingCarrier, layover, numStops))

    except Exception as e:
        print(f" Error inserting row {row}: {e}")

# Commit and close connection
conn.commit()
cur.close()
conn.close()

print(" Cleaned flight price data successfully stored in PostgreSQL!")
