import re
import csv
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

_DATA      = Path(__file__).resolve().parent.parent / "data" / "Youtube"
INPUT_IDS  = str(_DATA / "video_ids_all_train.txt")
OUTPUT_MAP = str(_DATA / "yt8m_id_to_youtube_id.csv")

BASE = "http://data.yt8m.org/2/j/i"   
TIMEOUT = 10
RETRIES = 3
MAX_WORKERS = 32
PAT = re.compile(r'i\("([^"]+)","([^"]+)"\);')

def url_for(yt8m_id: str) -> str:
    return f"{BASE}/{yt8m_id[:2]}/{yt8m_id}.js"

def lookup_one(yt8m_id: str) -> tuple[str, str | None, str]:
    url = url_for(yt8m_id)
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                return yt8m_id, None, f"http_{r.status_code}"
            m = PAT.search(r.text.strip())
            if not m:
                return yt8m_id, None, "parse_fail"
            # m.group(1) is internal id, m.group(2) is youtube id
            return yt8m_id, m.group(2), "ok"
        except requests.RequestException:
            time.sleep(0.2 * (2 ** attempt))
    return yt8m_id, None, "request_fail"

def load_ids(path: str) -> list[str]:
    ids = [l.strip() for l in Path(path).read_text(encoding="utf-8").splitlines() if l.strip()]
    seen, out = set(), []
    for v in ids:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out

def main():
    ids = load_ids(INPUT_IDS)

    # write header fresh (overwrite)
    with open(OUTPUT_MAP, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["yt8m_id", "youtube_id", "status"])
        w.writeheader()

        ok = 0
        miss = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(lookup_one, vid) for vid in ids]

            done = 0
            for fut in as_completed(futures):
                yt8m_id, youtube_id, status = fut.result()
                w.writerow({
                    "yt8m_id": yt8m_id,
                    "youtube_id": youtube_id or "",
                    "status": status,
                })
                done += 1
                if status == "ok":
                    ok += 1
                else:
                    miss += 1

                if done % 100 == 0 or done == len(ids):
                    print(f"{done}/{len(ids)} done | ok={ok} failed={miss}")

    print(f"Saved: {OUTPUT_MAP}")

if __name__ == "__main__":
    main()
