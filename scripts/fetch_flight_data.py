import requests
import json

API_URL = "https://flights-sky.p.rapidapi.com/flights/search-one-way"
API_KEY = "fdcf5105b0mshb60125cb25ee57ep1565acjsndd740fc93e14"

response = requests.get(API_URL, headers={"Authorization": f"Bearer {API_KEY}"})
if response.status_code == 200:
    data = response.json()
    with open("data/flight_prices.json", "w") as f:
        json.dump(data, f)
    print("Flight price data fetched successfully.")
else:
    print(f" API request failed: {response.status_code} - {response.text}")
