import json
from collections import defaultdict

FILE_PATH = "data/2026/04/17/news_hot.jsonl"  # adjust as needed

posts = defaultdict(list)

with open(FILE_PATH, "r", encoding="utf-8") as f:
    for line in f:
        d = json.loads(line)
        posts[d["id"]].append(d)

for post_id in posts:
    posts[post_id].sort(key=lambda x: x.get("observed_at", 0))

print(f"Unique posts: {len(posts)}")

tracked = {k: v for k, v in posts.items() if len(v) > 1}
print(f"Posts observed multiple times: {len(tracked)}")

for post_id, history in tracked.items():
    print("\nPOST ID:", post_id)
    print(f"title: {history[0].get('title', '')}")

    for i in range(1, len(history)):
        prev = history[i - 1]
        curr = history[i]

        dt = curr.get("observed_at", 0) - prev.get("observed_at", 0)
        dscore = curr["score"] - prev["score"]
        dcomments = curr["num_comments"] - prev["num_comments"]

        if dt > 0:
            score_rate = dscore / dt
            comment_rate = dcomments / dt
        else:
            score_rate = 0
            comment_rate = 0

        print(
            f"dt={dt:.1f}s | "
            f"dscore={dscore} ({score_rate:.3f}/s) | "
            f"dcomments={dcomments} ({comment_rate:.3f}/s)"
        )

    break