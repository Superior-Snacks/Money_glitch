import requests

url = "https://data-api.polymarket.com/trades?limit=5"
response = requests.get(url)
data = response.json()

for trade in data:
    print(trade["eventSlug"])

def get_market_data():
    ...