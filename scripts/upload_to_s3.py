import boto3

s3 = boto3.client("s3")
BUCKET_NAME = "flight-price-etl-github"

s3.upload_file("data/flight_prices.json", BUCKET_NAME, "flight_prices.json")
print("Data uploaded to S3 successfully.")
