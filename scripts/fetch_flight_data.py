import requests
import json
from datetime import datetime,timedelta

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

    params = {
        "fromEntityId": "BLR",
        "toEntityId": "JFK",
        "departDate": depart_date,
        "currency": "INR",
        "cabinClass": "economy",
        "stops":"direct,1stop,2stops"
    }


response = requests.get(API_URL, headers=headers, params=params)
if response.status_code == 200:
    data = response.json()
    with open(f"flight_prices_latest/{fetch_date}/{depart_date}.json", "w") as f:
        json.dump(data, f)
    print("Flight price data fetched successfully.")
else:
    print(f" API request failed: {response.status_code} - {response.text}")
