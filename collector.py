import requests
from config import USER_AGENT
from datetime import datetime


def fetch_reddit(subreddit, source, limit=10):
    url = f"https://www.reddit.com/r/{subreddit}/{source}.json?limit={limit}"

    headers = {
        "User-Agent": USER_AGENT
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error {response.status_code} for {subreddit}/{source}")
        return []

    data = response.json()

    posts = []
    for item in data["data"]["children"]:
        d = item["data"]

        posts.append({
            "id": d["id"],
            "title": d["title"],
            "author": d["author"],
            "created_utc": d["created_utc"],
            "score": d["score"],
            "num_comments": d["num_comments"],
            "subreddit": subreddit,
            "source": source,
            "observed_at": datetime.utcnow().timestamp()
        })

    return posts