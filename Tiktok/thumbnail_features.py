import torch
import numpy as np
import pandas as pd
import open_clip
from PIL import Image
from pathlib import Path

THUMB_DIR = Path('/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_thumbnails_new')
OUT_PATH  = Path('/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_visual_features.csv')

DEVICE     = "cuda" if torch.cuda.is_available() else "cpu"
BATCH_SIZE = 64
EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}

print(THUMB_DIR)
print(f"Device: {DEVICE}")

model, _, preprocess = open_clip.create_model_and_transforms(
    'ViT-L-14', pretrained='laion2b_s32b_b82k'
)
model = model.to(DEVICE).eval()

if OUT_PATH.exists():
    df_existing = pd.read_csv(OUT_PATH)
    done_ids    = set(df_existing['VideoId'].tolist())
    print(f"Already processed: {len(done_ids):,}")
else:
    df_existing = pd.DataFrame()
    done_ids    = set()

paths, vids = [], []
for f in THUMB_DIR.iterdir():
    if f.suffix.lower() in EXTENSIONS:
        try:
            vid = int(f.stem)
            if vid not in done_ids:
                paths.append(f)
                vids.append(vid)
        except ValueError:
            pass

print(f"Found {len(paths) + len(done_ids):,} thumbnails on disk")
print(f"Remaining to process: {len(paths):,}")

CHECKPOINT_EVERY = 100
results = []

for i in range(0, len(paths), BATCH_SIZE):
    batch_paths = paths[i : i + BATCH_SIZE]
    batch_vids  = vids[i : i + BATCH_SIZE]

    tensors, valid_idx = [], []
    for j, p in enumerate(batch_paths):
        try:
            img = preprocess(Image.open(p).convert('RGB'))
            tensors.append(img)
            valid_idx.append(j)
        except Exception as e:
            print(f"Failed on {p.name}: {e}")

    if not tensors:
        continue

    batch = torch.stack(tensors).to(DEVICE)
    with torch.no_grad():
        features = model.encode_image(batch).cpu().numpy()

    for k, j in enumerate(valid_idx):
        emb = features[k]
        results.append({
            'VideoId':   batch_vids[j],
            'rgb_l2':    float(np.linalg.norm(emb)),
            'rgb_mean':  float(np.mean(emb)),
            'rgb_std':   float(np.std(emb)),
            'embedding': emb.tolist(),
        })

    batch_num = i // BATCH_SIZE
    if batch_num % 50 == 0:
        print(f"Processed {i + len(tensors):,} / {len(paths):,}")

    if batch_num > 0 and batch_num % CHECKPOINT_EVERY == 0:
        df_chunk = pd.DataFrame(results)
        if OUT_PATH.exists():
            df_chunk.to_csv(OUT_PATH, mode='a', header=False, index=False)
        else:
            df_chunk.to_csv(OUT_PATH, index=False)
        print(f"  Checkpoint: flushed {len(results):,} rows to {OUT_PATH}")
        results = []

if results:
    df_chunk = pd.DataFrame(results)
    if OUT_PATH.exists():
        df_chunk.to_csv(OUT_PATH, mode='a', header=False, index=False)
    else:
        df_chunk.to_csv(OUT_PATH, index=False)

total = sum(1 for _ in open(OUT_PATH)) - 1
print(f"Done: {total:,} total videos saved -> {OUT_PATH}")