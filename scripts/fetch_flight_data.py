import requests
import boto3
import json
import os
from datetime import datetime

# Environment Variables
S3_BUCKET = os.getenv("S3_BUCKET")
S3_FOLDER = "raw_flight_data/"

# Initialize S3 Client
s3 = boto3.client("s3")

def fetch_flight_data():
    url = "https://flights-sky.p.rapidapi.com/flights/search-one-way"

    querystring = {"fromEntityId":"BLR","toEntityId":"DFW","departDate":"2025-05-31","market":"IN","locale":"en-US","currency":"INR","stops":"direct,1stop,2stops","cabinClass":"economy"}

    headers = {
    	"x-rapidapi-key": "fdcf5105b0mshb60125cb25ee57ep1565acjsndd740fc93e14",
    	"x-rapidapi-host": "flights-sky.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    
    if response.status_code == 200:
        data = response.json()
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"flight_prices_{timestamp}.json"

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_FOLDER + file_name,
            Body=json.dumps(data)
        )
        print(f"✅ Data saved to S3: {file_name}")
    else:
        print(f"❌ Failed to fetch data: {response.status_code}")

if __name__ == "__main__":
    fetch_flight_data()
