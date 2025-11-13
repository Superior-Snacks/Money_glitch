import requests; print(requests.get("https://data-api.polymarket.com/trades?market=deadbeef&limit=1", timeout=10).status_code)
