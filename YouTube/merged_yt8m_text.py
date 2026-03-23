import glob
import json
import os
import pandas as pd
import tensorflow as tf
from pathlib import Path

_DATA           = Path(__file__).resolve().parent.parent / "data" / "Youtube"
VIDEO_LEVEL_DIR = _DATA / "yt8m" / "video_level"
TEXT_CSV        = _DATA / "youtube8m_text.csv"
LABEL_NAMES_CSV = _DATA / "label_names.csv"
MAP_CSV         = _DATA / "yt8m_id_to_youtube_id.csv"
OUTPUT_CSV      = _DATA / "yt8m_merged.csv"


def find_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    raise ValueError(f"Column not found. Available: {list(df.columns)}")

def get_bytes(ex, key):
    f = ex.features.feature.get(key)
    return f.bytes_list.value[0] if f and f.bytes_list.value else None

def get_int64_list(ex, key):
    f = ex.features.feature.get(key)
    return list(f.int64_list.value) if f else []

def get_float_list(ex, key):
    f = ex.features.feature.get(key)
    vals = list(f.float_list.value) if f else []
    return vals if vals else None


labels_df = pd.read_csv(LABEL_NAMES_CSV)
label_map = dict(zip(
    labels_df[find_col(labels_df, ["label_id", "id", "index"])].astype(int),
    labels_df[find_col(labels_df, ["label_name", "name", "display_name"])].astype(str),
))

map_df = pd.read_csv(MAP_CSV)
yt8m_to_youtube = dict(zip(
    map_df[find_col(map_df, ["yt8m_id"])].astype(str),
    map_df[find_col(map_df, ["YouTube_id", "youtube_id"])].astype(str),
))

text_df = pd.read_csv(TEXT_CSV)
text_id_col = find_col(text_df, ["YouTube_id", "youtube_id"])
if text_id_col != "YouTube_id":
    text_df = text_df.rename(columns={text_id_col: "YouTube_id"})

rows = []
missing_mapping = bad_files = missing_features = 0

files = sorted(glob.glob(str(VIDEO_LEVEL_DIR / "*.tfrecord")) +
               glob.glob(str(VIDEO_LEVEL_DIR / "*.tfrecord.gz")))

for path in files:
    try:
        for raw in tf.data.TFRecordDataset(path):
            ex = tf.train.Example()
            ex.ParseFromString(raw.numpy())

            id_bytes = get_bytes(ex, "id")
            if id_bytes is None:
                continue
            yt8m_id = id_bytes.decode("utf-8")

            youtube_id = yt8m_to_youtube.get(yt8m_id)
            if youtube_id is None:
                missing_mapping += 1
                continue

            label_ids  = get_int64_list(ex, "labels")
            mean_rgb   = get_float_list(ex, "mean_rgb")
            mean_audio = get_float_list(ex, "mean_audio")

            if mean_rgb is None and mean_audio is None:
                missing_features += 1

            rows.append({
                "yt8m_id":          yt8m_id,
                "YouTube_id":       youtube_id,
                "label_ids_json":   json.dumps(label_ids),
                "label_names_json": json.dumps([label_map[i] for i in label_ids if i in label_map]),
                "num_labels":       len(label_ids),
                "mean_rgb_json":    json.dumps(mean_rgb)   if mean_rgb   is not None else None,
                "mean_audio_json":  json.dumps(mean_audio) if mean_audio is not None else None,
                "rgb_dim":          len(mean_rgb)   if mean_rgb   is not None else 0,
                "audio_dim":        len(mean_audio) if mean_audio is not None else 0,
            })

    except tf.errors.DataLossError:
        print(f"Skipping corrupted file: {os.path.basename(path)}")
        bad_files += 1

video_df = pd.DataFrame(rows)
print(f"Video rows: {len(video_df):,}")
print(f"Missing yt8m→YouTube mapping: {missing_mapping:,}")
print(f"Corrupted files skipped: {bad_files}")
print(f"Records missing both mean_rgb & mean_audio: {missing_features:,}")

merged = video_df.merge(text_df, on="YouTube_id", how="left")
merged.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"Wrote {OUTPUT_CSV}")
print(f"Text available for {merged['title'].notna().sum():,} videos")
