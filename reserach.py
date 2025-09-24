import requests

url = "https://gamma-api.polymarket.com/markets"
params = {
    "limit": 100,
    "offset": 0,
    "sortBy": "creationTime"
}

resp = requests.get(url, params=params)
data = resp.json()

for m in data:
    print(m["question"], m["createdAt"])


def get_history():
    ...

def get_market_data():
    ...

def save_to_history():
    ...