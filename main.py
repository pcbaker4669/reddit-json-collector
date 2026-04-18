import time
import random
from datetime import datetime
from pathlib import Path

from config import SUBREDDITS, SOURCES, POST_LIMIT, REQUEST_DELAY
from collector import fetch_reddit
from utils import save_posts
from summarize_posts import summarize_day
from utils import save_posts, append_manifest_row


def run_collector():
    cycle = 0

    while True:
        raw_posts_saved = 0
        requests_made = 0
        print(f"\nCycle {cycle + 1}...\n")

        for subreddit in SUBREDDITS:
            for source in SOURCES:
                posts = fetch_reddit(subreddit, source, POST_LIMIT)
                requests_made += 1

                time.sleep(random.uniform(2, 5))

                if posts:
                    save_posts(posts, subreddit, source)
                    raw_posts_saved += len(posts)

                time.sleep(REQUEST_DELAY)

        # rebuild today's derived files
        now = datetime.now()
        data_dir = Path(f"data/{now.year}/{now.month:02d}/{now.day:02d}")
        derived_dir = Path(f"derived/{now.year}/{now.month:02d}/{now.day:02d}")

        summary_path = derived_dir / "all_post_summary.csv"
        burst_path = derived_dir / "burst_posts_0.4.csv"

        all_rows, bursts = summarize_day(
            data_dir=data_dir,
            out_path=summary_path,
            burst_threshold=0.4,
            burst_out_path=burst_path,
        )

        append_manifest_row({
            "run_at": datetime.now().isoformat(),
            "cycle": cycle + 1,
            "data_dir": str(data_dir),
            "summary_path": str(summary_path),
            "burst_path": str(burst_path),
            "requests_made": requests_made,
            "raw_posts_saved": raw_posts_saved,
            "tracked_posts": len(all_rows),
            "burst_posts": len(bursts),
        })

        print(f"\nUpdated summary: {summary_path}")
        print(f"Tracked posts: {len(all_rows)}")
        print(f"Burst posts: {len(bursts)}")

        print("\nCycle complete. Sleeping...\n")
        cycle += 1
        time.sleep(random.uniform(60, 180))


if __name__ == "__main__":
    run_collector()