import csv
import subprocess
import time
from pathlib import Path
import re
import requests

_DATA   = Path(__file__).resolve().parent.parent / "data" / "TikTok"
CSV_PATH = _DATA / "query_result_2026-01-17T19_26_44.636561Z.csv"
OUT_DIR  = _DATA / "tiktok_thumbnails"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = _DATA / "thumbnails_map1.csv"

LIMIT = 1000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Referer": "https://www.tiktok.com/",
}

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
    r = requests.get(url, headers=HEADERS, timeout=25)
    if not r.ok or not r.content:
        return False
    out_path.write_bytes(r.content)
    return True

def ext_from_url(url):
    u = url.lower()
    if ".webp" in u: return ".webp"
    if ".png" in u: return ".png"
    return ".jpg"

def main():
    results = []
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            if i > LIMIT:
                break

            video_id = (row.get("VideoId") or "").strip()
            author = (row.get("AuthorName") or "").strip()
            if not video_id or not author:
                continue

            url = f"https://www.tiktok.com/@{author}/video/{video_id}"

            thumb_url = ytdlp_print_thumbnail(url)
            if not thumb_url:
                print(f"[{i}] NO  {video_id} (no thumbnail via yt-dlp)")
                results.append({"VideoId": video_id, "AuthorName": author, "Url": url,
                                "ThumbnailUrl": "", "LocalPath": "", "Status": "no_thumb"})
                time.sleep(0.7)
                continue

            out_path = OUT_DIR / f"{safe(video_id)}{ext_from_url(thumb_url)}"
            if not out_path.exists():
                ok = download_file(thumb_url, out_path)
            else:
                ok = True

            status = "ok" if ok else "download_failed"
            print(f"[{i}] {'OK ' if ok else 'NO '} {video_id}")

            results.append({"VideoId": video_id, "AuthorName": author, "Url": url,
                            "ThumbnailUrl": thumb_url, "LocalPath": str(out_path) if ok else "",
                            "Status": status})

            time.sleep(0.7)

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["VideoId","AuthorName","Url","ThumbnailUrl","LocalPath","Status"])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved thumbnails to: {OUT_DIR}")
    print(f"Saved mapping CSV to: {OUT_CSV}")

if __name__ == "__main__":
    main()
