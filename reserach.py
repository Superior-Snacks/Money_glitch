import requests

url = "https://gamma-api.polymarket.com/markets"
params = {
    "sortBy": "creationTime"
}

resp = requests.get(url, params=params)
data = resp.json()
print(data)

for m in data:
    print(m["question"], m["createdAt"])


def get_history():
    ...

def get_market_data():
    ...

def save_to_history():
    ...