import time
import random
from datetime import datetime
from pathlib import Path

from config import SUBREDDITS, SOURCES, POST_LIMIT, REQUEST_DELAY
from collector import fetch_reddit
from summarize_posts import summarize_day
from utils import save_posts, append_manifest_row


def run_end_of_day_processing(cycle_count: int):
    """
    Finalize the day's pipeline after collection ends.

    Steps:
    1. Rebuild today's summary from raw data
    2. Rebuild today's burst file
    3. Optionally collect comments for burst posts
    4. Append a final manifest row
    """
    now = datetime.now()
    data_dir = Path(f"data/{now.year}/{now.month:02d}/{now.day:02d}")
    derived_dir = Path(f"derived/{now.year}/{now.month:02d}/{now.day:02d}")

    summary_path = derived_dir / "all_post_summary.csv"
    burst_path = derived_dir / "burst_posts_0.4.csv"

    print("\nRunning end-of-day processing...\n")

    all_rows, bursts = summarize_day(
        data_dir=data_dir,
        out_path=summary_path,
        burst_threshold=0.4,
        burst_out_path=burst_path,
    )

    print(f"Final summary updated: {summary_path}")
    print(f"Tracked posts: {len(all_rows)}")
    print(f"Burst posts: {len(bursts)}")

    # Default comment stats
    comment_stats = {
        "candidates": 0,
        "saved": 0,
        "skipped": 0,
        "failed": 0,
    }

    # Optional: collect comments for burst posts
    if burst_path.exists() and len(bursts) > 0:
        try:
            from comments_collector import collect_comments_for_bursts

            print("\nCollecting comments for burst posts...\n")
            comment_stats = collect_comments_for_bursts(str(burst_path))
            print("\nComment collection complete.\n")

        except ImportError:
            print("comments_collector.py not found yet. Skipping comment collection.")
        except Exception as e:
            print(f"Comment collection failed: {e}")
    else:
        print("No burst posts found, so comment collection was skipped.")

    # Final manifest row goes AFTER comment collection
    append_manifest_row({
        "run_at": datetime.now().isoformat(),
        "cycle": cycle_count,
        "data_dir": str(data_dir),
        "summary_path": str(summary_path),
        "burst_path": str(burst_path),
        "requests_made": 0,
        "raw_posts_saved": 0,
        "tracked_posts": len(all_rows),
        "burst_posts": len(bursts),
        "event": "end_of_day_processing",
        "comment_candidates": comment_stats["candidates"],
        "comments_saved": comment_stats["saved"],
        "comments_skipped": comment_stats["skipped"],
        "comments_failed": comment_stats["failed"],
    })


def run_collector(max_cycles=2):
    cycle = 0

    while cycle < max_cycles:
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
            "event": "cycle_update",
        })

        print(f"\nUpdated summary: {summary_path}")
        print(f"Tracked posts: {len(all_rows)}")
        print(f"Burst posts: {len(bursts)}")

        print("\nCycle complete. Sleeping...\n")
        cycle += 1
        time.sleep(random.uniform(60, 180))

    return cycle


if __name__ == "__main__":
    final_cycle = run_collector(max_cycles=180)
    run_end_of_day_processing(cycle_count=final_cycle)