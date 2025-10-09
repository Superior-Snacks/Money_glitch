import json
import time
import requests
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt
import time, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass
import os

"""
fyrst sækja alla active markaði, síðan fara yfir book hjáhverjum og einum og sim hvort/hversu mikið er keypt,
savea kaupið og potential winnings og allt info i kringum það í file, síðan á 10 min fresti kanski 1 min sækja alla nýja markaði og ef það er liquidity kaupa,
annars bíða þar til það er liquidity, hafa sér pending file, svo 1 klst fresti checka hvort markaðir hafa klárast.
ef það eru mikið af markets þá byrja þannig ég hef efni á continuous trades á nýjum mörkuðum, (eldri markaðir ekki lokaðir eru oftast "settled")
hafa sterk guards á eldri mörkuðum þannig tap er ekki of hátt.
"""

def fetch_markets():
    """
    fetch all active markets, for the use of their id's
    """

def fetch_book(market_id):
    """
    fetch book for a single market
    """

def value_book(book):
    """
    calculate how much value can be taken
    """

def decide_book(bookvalue):
    """
    given value book, decide 
    """


def save_to_file(file, data):
    ...

def main():
    ...


if __name__ == "__main__":
    main()