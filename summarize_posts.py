import json
import csv
from collections import defaultdict
from pathlib import Path


def summarize_day(data_dir, out_path, burst_threshold=0.4, burst_out_path=None):
    data_dir = Path(data_dir)
    all_rows = []

    def summarize_file(file_path):
        posts = defaultdict(list)

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                d = json.loads(line)
                posts[d["id"]].append(d)

        for post_id in posts:
            posts[post_id].sort(key=lambda x: x.get("observed_at", 0))

        rows = []

        for post_id, history in posts.items():
            if len(history) < 2:
                continue

            total_dscore = 0
            total_dcomments = 0
            total_dt = 0
            max_score_rate = 0
            max_comment_rate = 0

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

                total_dscore += dscore
                total_dcomments += dcomments
                total_dt += dt
                max_score_rate = max(max_score_rate, score_rate)
                max_comment_rate = max(max_comment_rate, comment_rate)

            avg_score_rate = total_dscore / total_dt if total_dt > 0 else 0
            avg_comment_rate = total_dcomments / total_dt if total_dt > 0 else 0
            score_per_comment = (
                total_dscore / total_dcomments if total_dcomments > 0 else None
            )

            first_obs = history[0].get("observed_at", 0)
            last_obs = history[-1].get("observed_at", 0)
            lifespan_seconds = last_obs - first_obs if last_obs and first_obs else 0

            rows.append({
                "file_name": file_path.name,
                "subreddit": history[0].get("subreddit", ""),
                "source": history[0].get("source", ""),
                "post_id": post_id,
                "title": history[0].get("title", ""),
                "n_observations": len(history),
                "first_observed_at": first_obs,
                "last_observed_at": last_obs,
                "lifespan_seconds": round(lifespan_seconds, 1),
                "start_score": history[0]["score"],
                "end_score": history[-1]["score"],
                "start_comments": history[0]["num_comments"],
                "end_comments": history[-1]["num_comments"],
                "total_dscore": total_dscore,
                "total_dcomments": total_dcomments,
                "total_dt_seconds": round(total_dt, 1),
                "avg_score_rate": round(avg_score_rate, 4),
                "avg_comment_rate": round(avg_comment_rate, 4),
                "max_score_rate": round(max_score_rate, 4),
                "max_comment_rate": round(max_comment_rate, 4),
                "score_per_comment": round(score_per_comment, 4) if score_per_comment is not None else None,
                "burst_flag": round(max_score_rate, 4) > burst_threshold,
            })

        return rows

    for file_path in data_dir.glob("*.jsonl"):
        file_rows = summarize_file(file_path)
        all_rows.extend(file_rows)

    all_rows.sort(key=lambda x: x["max_score_rate"], reverse=True)

    if all_rows:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
            writer.writeheader()
            writer.writerows(all_rows)

        bursts = [row for row in all_rows if row["max_score_rate"] > burst_threshold]

        if burst_out_path is not None:
            burst_out_path = Path(burst_out_path)
            burst_out_path.parent.mkdir(parents=True, exist_ok=True)

            if bursts:
                with open(burst_out_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=bursts[0].keys())
                    writer.writeheader()
                    writer.writerows(bursts)
            else:
                with open(burst_out_path, "w", newline="", encoding="utf-8") as f:
                    f.write("")

        return all_rows, bursts

    return [], []