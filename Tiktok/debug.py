import csv
import re
from pathlib import Path

vtt_root = Path(__file__).resolve().parent.parent / "data" / "TikTok" / "tiktok_transcripts_out"
index: dict[str, Path] = {}

for vtt_path in vtt_root.rglob("*.vtt"):
        video_id = vtt_path.parent.name  
        if video_id not in index:
            index[video_id] = vtt_path

print(len(index))