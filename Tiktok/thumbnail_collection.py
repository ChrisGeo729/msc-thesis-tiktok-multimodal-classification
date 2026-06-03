import csv
import subprocess
import time
from pathlib import Path
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_DATA    = Path(__file__).resolve().parent.parent / "data" / "TikTok"
CSV_PATH = _DATA / "/Users/christosgeorghiou/Desktop/MSc Thesis/data/TikTok/query_english.csv"
OUT_DIR  = _DATA / "tiktok_thumbnails"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV  = _DATA / "thumbnails_map.csv"

LIMIT = 200000
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.tiktok.com/",
}

session = requests.Session()
retry = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retry))


def safe(s):
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)[:140]


def ytdlp_print_thumbnail(url):
    cmd = ["yt-dlp", "--skip-download", "--print", "thumbnail", url]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return None
    thumb = (p.stdout or "").strip()
    return thumb if thumb.startswith("http") else None


def download_file(url, out_path):
    try:
        r = session.get(url, headers=HEADERS, timeout=40)
        if not r.ok or not r.content:
            return False
        out_path.write_bytes(r.content)
        return True
    except requests.exceptions.ReadTimeout:
        print("  Timeout, skipping")
        return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


def ext_from_url(url):
    u = url.lower()
    if ".webp" in u: return ".webp"
    if ".png"  in u: return ".png"
    return ".jpg"


def load_done_ids():
    if not OUT_CSV.exists():
        return set()
    with OUT_CSV.open("r", encoding="utf-8", newline="") as f:
        return {row["VideoId"] for row in csv.DictReader(f)}


def main():
    done_ids = load_done_ids()
    results  = []
    seen     = 0

    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            video_id = (row.get("VideoId")    or "").strip()
            author   = (row.get("AuthorName") or "").strip()

            if not video_id or not author or video_id in done_ids:
                continue

            seen += 1
            if seen > LIMIT:
                break

            i   = seen
            url = f"https://www.tiktok.com/@{author}/video/{video_id}"

            thumb_url = ytdlp_print_thumbnail(url)
            if not thumb_url:
                print(f"[{i}] NO  {video_id} (no thumbnail via yt-dlp)")
                results.append({"VideoId": video_id, "AuthorName": author, "Url": url,
                                 "ThumbnailUrl": "", "LocalPath": "", "Status": "no_thumb"})
                time.sleep(0.7)
                continue

            out_path = OUT_DIR / f"{safe(video_id)}{ext_from_url(thumb_url)}"
            ok = True if out_path.exists() else download_file(thumb_url, out_path)

            status = "ok" if ok else "download_failed"
            print(f"[{i}] {'OK ' if ok else 'NO '} {video_id}")
            results.append({"VideoId": video_id, "AuthorName": author, "Url": url,
                             "ThumbnailUrl": thumb_url,
                             "LocalPath": str(out_path) if ok else "",
                             "Status": status})
            time.sleep(0.7)

    write_header = not OUT_CSV.exists()
    with OUT_CSV.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["VideoId", "AuthorName", "Url", "ThumbnailUrl", "LocalPath", "Status"])
        if write_header:
            writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved thumbnails to: {OUT_DIR}")
    print(f"Saved mapping CSV to: {OUT_CSV}")


if __name__ == "__main__":
    main()