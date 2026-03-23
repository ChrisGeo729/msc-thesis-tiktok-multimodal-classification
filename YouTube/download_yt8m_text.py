import csv
import json
import os
import time
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

API_KEY = os.environ["YOUTUBE_API_KEY"]

_DATA      = Path(__file__).resolve().parent.parent / "data" / "Youtube"
INPUT_IDS  = str(_DATA / "yt8m_id_to_youtube_id.csv")
OUTPUT_CSV = str(_DATA / "youtube8m_text.csv")

BATCH_SIZE = 50
RETRIES = 6
SLEEP_SECS = 0.05


def read_ids(path):
    with open(path, newline="", encoding="utf-8") as f:
        ids = [(row.get("youtube_id") or "").strip() for row in csv.DictReader(f)]
    return list(dict.fromkeys(v for v in ids if v))


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def fetch_batch(youtube, ids):
    resp = youtube.videos().list(
        part="snippet",
        id=",".join(ids),
        maxResults=len(ids),
    ).execute()

    out = {}
    for item in resp.get("items", []):
        vid = item.get("id")
        snip = item.get("snippet", {}) or {}
        out[vid] = {
            "title": snip.get("title"),
            "tags": snip.get("tags", []) or [],
            "channelTitle": snip.get("channelTitle"),
            "publishedAt": snip.get("publishedAt"),
        }
    return out


def main():
    youtube = build("youtube", "v3", developerKey=API_KEY)
    ids = read_ids(INPUT_IDS)
    print(f"Loaded {len(ids)} youtube ids")

    ok = 0
    missing = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["youtube_id", "title", "tags_json", "channelTitle", "publishedAt", "status"],
        )
        w.writeheader()

        for batch in chunked(ids, BATCH_SIZE):
            for attempt in range(RETRIES):
                try:
                    data = fetch_batch(youtube, batch)
                    break
                except HttpError as e:
                    wait = (2 ** attempt) + 0.2
                    print(f"HttpError: {e}. retry {attempt+1}/{RETRIES} in {wait:.1f}s")
                    time.sleep(wait)
            else:
                data = {}

            for vid in batch:
                if vid in data:
                    row = data[vid]
                    status = "ok"
                    ok += 1
                else:
                    row = {"title": None, "tags": [], "channelTitle": None, "publishedAt": None}
                    status = "missing/private/deleted"
                    missing += 1

                w.writerow({
                    "youtube_id": vid,
                    "title": row["title"],
                    "tags_json": json.dumps(row["tags"], ensure_ascii=False),
                    "channelTitle": row["channelTitle"],
                    "publishedAt": row["publishedAt"],
                    "status": status,
                })

            time.sleep(SLEEP_SECS)

    print(f"Done. ok={ok}, missing={missing}")
    print(f"Wrote {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
