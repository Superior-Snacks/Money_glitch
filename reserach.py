import requests

url = "https://gamma-api.polymarket.com/markets"
params = {
    "limit": 20,
    "offset": 0,
    "sortBy": "creationTime",
    "order": "asc"
}

resp = requests.get(url, params=params)
data = resp.json()
print(data)

for m in data:
    print(m)


def get_history():
    ...

def get_market_data():
    ...

def save_to_history():
    ...