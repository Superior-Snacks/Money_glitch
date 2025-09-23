import requests

url = "https://gamma-api.polymarket.com/events"
response = requests.get(url)
data = response.json()
for trade in data:
    print(trade["ticker"])


def get_history():
    ...

def get_market_data():
    ...

def save_to_history():
    ...