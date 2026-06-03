import csv
import json
import os
import time
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

API_KEY    = "REDACTED"
_DATA      = Path(__file__).resolve().parent.parent / "data" / "Youtube"
INPUT_IDS  = str(_DATA / "yt8m_id_to_youtube_id.csv")
OUTPUT_CSV = str(_DATA / "youtube8m_text.csv")
PROGRESS   = str(_DATA / "youtube8m_text.progress.jsonl")
BATCH_SIZE = 50
RETRIES    = 6
SLEEP_SECS = 0.05


def read_ids(path):
    with open(path, newline="", encoding="utf-8") as f:
        ids = [(row.get("youtube_id") or "").strip() for row in csv.DictReader(f)]
    return list(dict.fromkeys(v for v in ids if v))


def load_done(path):
    done = set()
    if not os.path.exists(path):
        return done
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                done.update(json.loads(line))
    return done


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

    done = load_done(PROGRESS)
    remaining = [v for v in ids if v not in done]
    print(f"Already fetched: {len(done)}, remaining: {len(remaining)}")

    write_header = not os.path.exists(OUTPUT_CSV) or os.path.getsize(OUTPUT_CSV) == 0
    ok = missing = 0

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f, \
         open(PROGRESS, "a", encoding="utf-8") as pf:

        w = csv.DictWriter(
            f,
            fieldnames=["youtube_id", "title", "tags_json", "channelTitle", "publishedAt", "status"],
        )
        if write_header:
            w.writeheader()

        for batch in chunked(remaining, BATCH_SIZE):
            for attempt in range(RETRIES):
                try:
                    data = fetch_batch(youtube, batch)
                    break
                except HttpError as e:
                    if e.resp.status == 403 and "quotaExceeded" in str(e):
                        print(f"Quota exceeded. Exiting — resume tomorrow (resets 08:00 Amsterdam).")
                        print(f"Progress so far: ok={ok}, missing={missing}")
                        return
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

            pf.write(json.dumps(batch) + "\n")
            pf.flush()
            f.flush()
            time.sleep(SLEEP_SECS)

    print(f"Done. ok={ok}, missing={missing}")
    print(f"Wrote {OUTPUT_CSV}")


if __name__ == "__main__":
    main()