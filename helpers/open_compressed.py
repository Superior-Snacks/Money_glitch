import gzip, json
import pandas as pd

path = input("gz file:")  # your file

with gzip.open(path, "rt", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        print(record)

df = pd.read_json(path, lines=True, compression="gzip")
print(df.head())