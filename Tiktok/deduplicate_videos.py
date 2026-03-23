import csv
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent / "data" / "TikTok"

INPUT_CSV  = _DATA / "query_result_2026-01-17T19_26_44.636561Z.csv"
OUTPUT_CSV = _DATA / "videos_unique.csv"

VIDEO_ID_COL = "VideoId"
PROGRESS_EVERY = 50_000


def normalize_video_id(x):
    s = "" if x is None else str(x).strip()
    if "e" in s.lower():
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    if s.endswith(".0"):
        s = s[:-2]
    return s.strip()


def main():
    seen = set()
    total = 0
    written = 0

    with INPUT_CSV.open("r", encoding="utf-8", newline="") as fin, \
         OUTPUT_CSV.open("w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames or []

        if VIDEO_ID_COL not in fieldnames:
            raise ValueError(f"Missing column {VIDEO_ID_COL}")

        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1

            vid = normalize_video_id(row.get(VIDEO_ID_COL))
            if not vid:
                continue

            if vid in seen:
                continue

            seen.add(vid)
            writer.writerow(row)
            written += 1

            if total % PROGRESS_EVERY == 0:
                print(f"Processed {total:,} rows | unique videos written: {written:,}")

    print(f"Rows read: {total:,}")
    print(f"Unique videos written: {written:,}")
    print(f"Saved: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
