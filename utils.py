import json
import csv
from datetime import datetime
from pathlib import Path

def append_manifest_row(row, manifest_path="derived/manifest_log.csv"):
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = manifest_path.exists()

    with open(manifest_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)

def save_posts(posts, subreddit, source):
    now = datetime.now()

    folder = Path(f"data/{now.year}/{now.month:02d}/{now.day:02d}")
    folder.mkdir(parents=True, exist_ok=True)

    filename = folder / f"{subreddit}_{source}.jsonl"

    with open(filename, "a", encoding="utf-8") as f:
        for post in posts:
            f.write(json.dumps(post) + "\n")

    print(f"Saved {len(posts)} posts → {filename}")