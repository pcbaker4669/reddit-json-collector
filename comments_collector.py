import csv
import json
import time
from pathlib import Path

import requests

USER_AGENT = "AcademicResearch/0.1 (by u/SimTekGameDev)"


def comments_file_exists(subreddit, source, post_id, base_dir="comments"):
    from datetime import datetime

    now = datetime.now()
    path = (
        Path(base_dir)
        / f"{now.year}"
        / f"{now.month:02d}"
        / f"{now.day:02d}"
        / f"{subreddit}_{source}"
        / f"{post_id}.json"
    )
    return path.exists()


def load_burst_candidates(csv_path):
    candidates = []

    csv_path = Path(csv_path)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return candidates

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("post_id"):
                continue

            candidates.append({
                "post_id": row["post_id"],
                "subreddit": row["subreddit"],
                "source": row["source"],
                "title": row.get("title", ""),
            })

    return candidates


def fetch_comments_json(subreddit, post_id):
    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
    headers = {"User-Agent": USER_AGENT}

    try:
        response = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        print(f"Request failed for {subreddit}/{post_id}: {e}")
        return None

    if response.status_code != 200:
        print(f"Comment fetch failed: {response.status_code} for {subreddit}/{post_id}")
        return None

    try:
        return response.json()
    except ValueError:
        print(f"Invalid JSON returned for {subreddit}/{post_id}")
        return None


def save_comments_json(data, subreddit, source, post_id, base_dir="comments"):
    from datetime import datetime

    now = datetime.now()
    folder = (
        Path(base_dir)
        / f"{now.year}"
        / f"{now.month:02d}"
        / f"{now.day:02d}"
        / f"{subreddit}_{source}"
    )
    folder.mkdir(parents=True, exist_ok=True)

    out_path = folder / f"{post_id}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    return out_path


def collect_comments_for_bursts(burst_csv_path):
    candidates = load_burst_candidates(burst_csv_path)

    print(f"Loaded {len(candidates)} burst candidates")

    saved_count = 0
    skipped_count = 0
    failed_count = 0

    for candidate in candidates:
        post_id = candidate["post_id"]
        subreddit = candidate["subreddit"]
        source = candidate["source"]

        if comments_file_exists(subreddit, source, post_id):
            print(f"Already collected today: {subreddit}/{source}/{post_id}")
            skipped_count += 1
            continue

        data = fetch_comments_json(subreddit, post_id)

        if data is not None:
            out_path = save_comments_json(data, subreddit, source, post_id)
            print(f"Saved comments → {out_path}")
            saved_count += 1
        else:
            failed_count += 1

        time.sleep(5)

    return {
        "candidates": len(candidates),
        "saved": saved_count,
        "skipped": skipped_count,
        "failed": failed_count,
    }