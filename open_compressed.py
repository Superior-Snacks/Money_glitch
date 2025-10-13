import gzip, json
import pandas as pd

path = "logs/trades_taken_2025-10-12.jsonl.gz"  # your file

with gzip.open(path, "rt", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        print(record)

df = pd.read_json(path, lines=True, compression="gzip")
print(df.head())