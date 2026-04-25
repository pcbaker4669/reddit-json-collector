# Reddit JSON Collector

A lightweight data collection pipeline for tracking Reddit posts over time using public JSON endpoints. This tool repeatedly samples subreddit feeds (e.g., /new, /hot) and stores timestamped observations for downstream analysis of temporal dynamics and burst behavior.

---

## Overview

This collector is designed to support time-resolved measurement of post activity. Instead of taking a single snapshot, it records repeated observations of the same posts, enabling reconstruction of growth trajectories and calculation of engagement rates.

The collected data is used in a companion analysis project to detect high-velocity events and study concentration in online attention.

---

## Features

- No API key required (uses public Reddit JSON endpoints)
- Supports multiple subreddits and feed types
- Timestamped observations for each post
- Append-only data storage (no overwriting raw data)
- Simple and modular design

---

## Data Structure

Data is stored in a date-partitioned directory structure:

data/YYYY/MM/DD/
├── politics_new.jsonl
├── politics_hot.jsonl
├── news_new.jsonl
├── news_hot.jsonl

Each file contains newline-delimited JSON (.jsonl) with one record per observation.

---

## Example Record

{
  "post_id": "abc123",
  "title": "Example post",
  "score": 152,
  "num_comments": 34,
  "subreddit": "news",
  "source": "new",
  "observed_at": 1713800000
}

---

## How It Works

The collector runs in cycles:

1. Fetch posts from /r/{subreddit}/new.json and /hot.json
2. Record score and comment count
3. Attach a timestamp (observed_at)
4. Append results to .jsonl files
5. Sleep for a short interval
6. Repeat

Over time, this builds a time series for each post.

---

## Usage

Run the collector:

python main.py

You can control the number of cycles in the script:

run_collector(max_cycles=180)

---

## Configuration

Edit config.py to set:

- Subreddits to track
- Feed types (new, hot)
- Number of posts per request
- Delay between requests

---

## Notes

- This tool is intended for research and educational use
- It does not use the official Reddit API and may be subject to rate limits
- Data collection should be performed responsibly and at reasonable intervals

---

## Related Project

This repository provides the data collection layer for:

Reddit Burst Analysis  
A companion project that processes collected data to detect high-velocity events and analyze attention dynamics.

---

## License

MIT License