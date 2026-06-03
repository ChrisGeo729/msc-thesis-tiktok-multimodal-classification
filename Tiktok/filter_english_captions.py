import csv
import re
from pathlib import Path
from langdetect import detect, LangDetectException

_DATA = Path(__file__).resolve().parent.parent / "data" / "TikTok"
IN_CSV  = _DATA / "query_with_captions_filled1.csv"
OUT_CSV = _DATA / "query_english.csv"

PROGRESS_EVERY = 5_000
MIN_TEXT_LEN   = 15

_TIMESTAMP_RE = re.compile(r"^\d{2}:\d{2}:\d{2}[\.,]\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}[\.,]\d{3}")
_STYLE_TAG_RE = re.compile(r"</?[^>]+>")
_CUE_NUM_RE   = re.compile(r"^\d+$")


def extract_text(caption_field: str) -> str:
    """Strip VTT metadata and return plain text suitable for language detection."""
    lines = caption_field.splitlines()
    out = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if line.upper().startswith("WEBVTT"):
            continue
        if _TIMESTAMP_RE.match(line):
            continue
        if _CUE_NUM_RE.match(line):
            continue
        line = _STYLE_TAG_RE.sub("", line)
        line = line.replace("&nbsp;", " ").replace("\x00", "").strip()
        if line:
            out.append(line)
    return " ".join(out)


def is_english(text: str) -> bool:
    if len(text) < MIN_TEXT_LEN:
        return False
    try:
        return detect(text) == "en"
    except LangDetectException:
        return False


def main():
    kept = skipped = total = 0
    kept_by_caption = kept_by_description = 0

    with IN_CSV.open("r", encoding="utf-8", newline="") as fin, \
         OUT_CSV.open("w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            caption     = row.get("Captions", "").strip()
            description = row.get("Description", "").strip()

            caption_en     = is_english(extract_text(caption)) if caption else False
            description_en = is_english(description) if description else False

            if caption_en or description_en:
                writer.writerow(row)
                kept += 1
                if caption_en:
                    kept_by_caption += 1
                if description_en:
                    kept_by_description += 1
            else:
                skipped += 1

            if total % PROGRESS_EVERY == 0:
                print(f"Processed {total:,} | kept={kept:,} | skipped={skipped:,}")

    print("\nDone")
    print(f"Total rows                 {total:,}")
    print(f"Kept                       {kept:,}")
    print(f"  English captions         {kept_by_caption:,}")
    print(f"  English descriptions     {kept_by_description:,}")
    print(f"Skipped                    {skipped:,}")
    print(f"Saved to                   {OUT_CSV}")


if __name__ == "__main__":
    main()