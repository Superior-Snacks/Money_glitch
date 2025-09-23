import requests

url = "https://data-api.polymarket.com/trades?market=MARKET_ID&limit=5&sort=asc"
response = requests.get(url)
data = response.json()

for trade in data:
    print(trade)


def get_history():
    ...

def get_market_data():
    ...

def save_to_history():
    ...