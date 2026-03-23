import os, glob
import numpy as np
import pandas as pd
import tensorflow as tf

_DATA    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "Youtube")
DATA_DIR = os.path.join(_DATA, "yt8m", "video_level")
OUT_CSV  = os.path.join(_DATA, "yt8m_eda_sample.csv")

files = sorted(glob.glob(os.path.join(DATA_DIR, "train*.tfrecord")))[:30]

feature_description = {
    "id": tf.io.FixedLenFeature([], tf.string),
    "mean_rgb": tf.io.FixedLenFeature([1024], tf.float32),
    "mean_audio": tf.io.FixedLenFeature([128], tf.float32),
    "labels": tf.io.VarLenFeature(tf.int64),
}

def parse_example(example_proto):
    ex = tf.io.parse_single_example(example_proto, feature_description)
    ex["labels"] = tf.sparse.to_dense(ex["labels"])
    return ex

ds = tf.data.TFRecordDataset(files)
ds = ds.map(parse_example)

rows = []

for ex in ds.take(20000):
    rgb = ex["mean_rgb"].numpy()
    aud = ex["mean_audio"].numpy()
    labels = ex["labels"].numpy().astype(int)

    rows.append({
        "video_id": ex["id"].numpy().decode("utf-8", errors="ignore"),
        "num_labels": len(labels),
        "labels": ",".join(map(str, labels)),
        "rgb_l2": float(np.linalg.norm(rgb)),
        "audio_l2": float(np.linalg.norm(aud)),
        "rgb_mean": float(rgb.mean()),
        "audio_mean": float(aud.mean()),
        "rgb_std": float(rgb.std()),
        "audio_std": float(aud.std()),
    })

df = pd.DataFrame(rows)
df.to_csv(OUT_CSV, index=False)

print("Saved:", OUT_CSV)
print("Shape:", df.shape)
