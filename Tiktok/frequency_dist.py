import json
import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT_DIR = "/home/cgeorghiou/projects/msc-thesis"

# TKGO
df_tk = pd.read_csv("/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_with_visual_features.csv",low_memory=False)

df_tk = df_tk.drop_duplicates(subset="VideoId", keep="first")
df_tk["label_list"] = df_tk["Labels"].apply(lambda s: re.findall(r'"([^"]+)"', s) if isinstance(s, str) else [])
df_tk = df_tk[df_tk["label_list"].apply(len) > 0]

tk_labels = df_tk["label_list"].explode()
tk_counts = tk_labels.value_counts().sort_values(ascending=False)
print(f"TKGO: {len(tk_counts)} classes, {len(df_tk):,} labelled videos")
print(f"  Top 5: {tk_counts.head().to_dict()}")
print(f"  Bottom 5: {tk_counts.tail().to_dict()}")

# YT8M
df_yt = pd.read_parquet("/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_metadata.parquet")
df_yt = df_yt[df_yt["status"] == "ok"]
df_yt["label_names"] = df_yt["label_names_json"].apply(lambda s: json.loads(s) if pd.notna(s) else [])
df_yt = df_yt[df_yt["label_names"].apply(len) > 0]

if len(df_yt) > 1_000_000:
    df_yt = df_yt.sample(n=1_000_000, random_state=42)
yt_labels = df_yt["label_names"].explode()
yt_counts = yt_labels.value_counts().sort_values(ascending=False)
print(f"\nYT8M-T: {len(yt_counts)} classes, {len(df_yt):,} labelled videos")
print(f"  Top 5: {yt_counts.head().to_dict()}")
print(f"  Bottom 5: {yt_counts.tail().to_dict()}")

fig, axes = plt.subplots(2, 1, figsize=(8, 9))

ax = axes[0]
ax.bar(range(len(tk_counts)), tk_counts.values, color="#4CAF50", width=1.0)
ax.set_yscale("log")
ax.set_xlabel("Class rank", fontsize=11)
ax.set_ylabel("Number of videos (log scale)", fontsize=11)
ax.set_title("TKGO", fontsize=12)
ax.set_xlim(-1, len(tk_counts))
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.annotate(f"{len(tk_counts)} classes", xy=(0.95, 0.95),
            xycoords="axes fraction", ha="right", va="top", fontsize=9)

ax = axes[1]
ax.bar(range(len(yt_counts)), yt_counts.values, color="#2196F3", width=1.0)
ax.set_yscale("log")
ax.set_xlabel("Class rank", fontsize=11)
ax.set_ylabel("Number of videos (log scale)", fontsize=11)
ax.set_title("YouTube-8M-T", fontsize=12)
ax.set_xlim(-1, len(yt_counts))
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.annotate(f"{len(yt_counts)} classes", xy=(0.95, 0.95),
            xycoords="axes fraction", ha="right", va="top", fontsize=9)

plt.tight_layout()
plt.savefig(f"{OUT_DIR}/label_frequency_distribution.png", dpi=150, bbox_inches="tight")
plt.close()
print(f"\nSaved")