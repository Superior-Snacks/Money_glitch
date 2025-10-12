import gzip, json

path = "logs/trades_taken_2025-10-11.jsonl.gz"  # your file

with gzip.open(path, "rt", encoding="utf-8") as f:
    for line in f:
        record = json.loads(line)
        print(record)