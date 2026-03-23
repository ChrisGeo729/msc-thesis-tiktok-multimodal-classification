import csv
import re
from pathlib import Path

_DATA = Path(__file__).resolve().parent.parent / "data" / "TikTok"

DATASET_CSV = _DATA / "videos_unique.csv"
VTT_ROOT    = _DATA / "tiktok_transcripts_out"
OUT_CSV     = _DATA / "query_with_captions_filled.csv"
TIMESTAMP_RE = re.compile(r"^\d{2}:\d{2}:\d{2}\.\d{3}\s-->\s\d{2}:\d{2}:\d{2}\.\d{3}")
STYLE_TAG_RE = re.compile(r"</?[^>]+>")
NOTE_RE = re.compile(r"^NOTE\b", re.IGNORECASE)
FILL_ONLY_IF_EMPTY = False   
PROGRESS_EVERY = 10_000




def clean_vtt_text(vtt_content):
    lines = vtt_content.splitlines()
    out = []
    prev = None

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        line = STYLE_TAG_RE.sub("", line)
        line = line.replace("&nbsp;", " ").replace("\u200b", "").strip()

        # remove consecutive duplicates
        if prev is None or line != prev:
            out.append(line)
            prev = line

    text = " ".join(out)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_empty(x):
    return x is None or str(x).strip() == "" or str(x).strip().lower() == "nan"

# creates lookup dict video_id -> vtt_file_path
def build_vtt_index(vtt_root):
    index: dict[str, Path] = {}
    if not vtt_root.exists():
        raise FileNotFoundError(f"VTT_ROOT does not exist: {vtt_root}")

    for vtt_path in vtt_root.rglob("*.vtt"):
        video_id = vtt_path.parent.name
        if video_id not in index:
            index[video_id] = vtt_path

    return index


def main():
    # index VTT files 
    print("Indexing VTT files...")
    vtt_index = build_vtt_index(VTT_ROOT)
    print(f"Indexed {len(vtt_index)} VTT files.")

    # stream input CSV and write output CSV row-by-row
    filled = 0
    missing_vtt = 0
    parse_errors = 0
    total_rows = 0

    with DATASET_CSV.open("r", encoding="utf-8", newline="") as fin, \
         OUT_CSV.open("w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames or []

        if "VideoId" not in fieldnames:
            raise ValueError(f"Missing required column: VideoId")
        if "Captions" not in fieldnames:
            raise ValueError(f"Missing required column: Captions")

        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total_rows += 1

            vid = (row.get("VideoId") or "").strip()
            if not vid:
                writer.writerow(row)
                continue

            current_caps = row.get("Captions")

            if FILL_ONLY_IF_EMPTY and not is_empty(current_caps):
                writer.writerow(row)
                continue

            vtt_path = vtt_index.get(vid)
            if not vtt_path:
                missing_vtt += 1
                writer.writerow(row)
                continue

            try:
                content = vtt_path.read_text(encoding="utf-8", errors="replace")
                transcript = clean_vtt_text(content)

                if transcript:
                    row["Captions"] = transcript
                    filled += 1
                else:
                    empty_after_clean += 1

            except Exception:
                parse_errors += 1

            writer.writerow(row)

            if total_rows % PROGRESS_EVERY == 0:
                print(
                    f"Processed {total_rows:,} | filled={filled:,} | "
                )

    print("\nDONE")
    print(f"Rows total: {total_rows:,}")
    print(f"Filled Captions from VTT: {filled:,}")
    print(f"Missing VTT: {missing_vtt:,}")
    print(f"Parse errors: {parse_errors:,}")
    print(f"Saved: {OUT_CSV}")


if __name__ == "__main__":
    main()
