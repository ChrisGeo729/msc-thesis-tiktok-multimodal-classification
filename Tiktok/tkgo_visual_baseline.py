import json
import re
import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, precision_score, recall_score
import warnings
warnings.filterwarnings("ignore")

DATA_PATH  = "/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_with_visual_features.csv"
VISUAL_DIM = 768

df = pd.read_csv(DATA_PATH, low_memory=False)
df["label_list"] = df["Labels"].apply(
    lambda s: re.findall(r'"([^"]+)"', s) if isinstance(s, str) else []
)
df = df[df["label_list"].apply(len) > 0].copy()
df = df[df["embedding"].notna()].copy()
df = df.drop_duplicates(subset="VideoId", keep="first").reset_index(drop=True)
print(f"Videos with visual features + labels: {len(df):,}")

print("Parsing embeddings...")
X_all      = np.zeros((len(df), VISUAL_DIM), dtype=np.float32)
valid_mask = np.ones(len(df), dtype=bool)
for i, s in enumerate(df["embedding"]):
    try:
        vec = json.loads(s)
        if len(vec) == VISUAL_DIM:
            X_all[i] = vec
        else:
            valid_mask[i] = False
    except Exception:
        valid_mask[i] = False

df    = df[valid_mask].reset_index(drop=True)
X_all = X_all[valid_mask]
print(f"Videos after embedding validation: {len(df):,}")

mlb         = MultiLabelBinarizer()
Y_all       = mlb.fit_transform(df["label_list"].values)
num_classes = Y_all.shape[1]
print(f"Label space: {num_classes}")

X_train, X_test, Y_train, Y_test = train_test_split(
    X_all, Y_all, test_size=0.2, random_state=42
)
print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

scaler  = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test  = scaler.transform(X_test)


def global_average_precision(y_true, y_prob, top_k=20):
    precisions = []
    for i in range(y_true.shape[0]):
        hits = 0
        for rank, idx in enumerate(np.argsort(y_prob[i])[::-1][:top_k], 1):
            if y_true[i, idx] == 1:
                hits += 1
                precisions.append(hits / rank)
    return np.mean(precisions) if precisions else 0.0


print("\nFitting visual baseline (LR on OpenCLIP embeddings)...")
model = OneVsRestClassifier(
    SGDClassifier(loss="log_loss", max_iter=1000, random_state=42),
    n_jobs=4
)
model.fit(X_train, Y_train)
Y_prob = model.predict_proba(X_test)

gap20 = global_average_precision(Y_test, Y_prob, top_k=20)
gap5  = global_average_precision(Y_test, Y_prob, top_k=5)
print(f"\nVisual baseline — OpenCLIP ViT-L-14 + LR")
print(f"GAP@20={gap20:.4f}  GAP@5={gap5:.4f}")
for thr in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
    Y_pred = (Y_prob >= thr).astype(int)
    p  = precision_score(Y_test, Y_pred, average="micro", zero_division=0)
    r  = recall_score(Y_test, Y_pred, average="micro", zero_division=0)
    mi = f1_score(Y_test, Y_pred, average="micro", zero_division=0)
    ma = f1_score(Y_test, Y_pred, average="macro", zero_division=0)
    print(f"thr={thr:.1f}  P={p:.4f}  R={r:.4f}  Micro F1={mi:.4f}  Macro F1={ma:.4f}")