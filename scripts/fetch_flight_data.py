import os
import requests
import json
from datetime import datetime
import boto3

# Flight API details
API_URL = "https://flights-sky.p.rapidapi.com/flights/search-one-way"
API_KEY = "fdcf5105b0mshb60125cb25ee57ep1565acjsndd740fc93e14"
headers = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "flights-sky.p.rapidapi.com"
}

# Fetch date (when we are collecting data)
fetch_date = datetime.today().strftime('%Y-%m-%d')

# Depart date (when the flight is scheduled)
depart_date = "2025-05-31"

# Request parameters
params = {
    "fromEntityId": "BLR",
    "toEntityId": "JFK",
    "departDate": depart_date,
    "currency": "INR",
    "cabinClass": "economy",
    "stops": "direct,1stop,2stops"
}

# API request
response = requests.get(API_URL, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()

    # Ensure directory exists
    output_dir = f"flight_prices_latest/{fetch_date}"
    os.makedirs(output_dir, exist_ok=True)

    # Save JSON file
    file_path = f"{output_dir}/{depart_date}.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    print("Flight price data fetched successfully.")

    ## Upload file to AWS S3
    s3 = boto3.client("s3")
    BUCKET_NAME = "flight-price-etl-github"

    try:
        s3.upload_file(file_path, BUCKET_NAME, f"{depart_date}.json")
        print(" Data uploaded to S3 successfully.")
    except Exception as e:
        print(f" Failed to upload to S3: {str(e)}")

else:
    print(f" API request failed: {response.status_code} - {response.text}")

