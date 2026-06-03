import json
import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, precision_score, recall_score
import warnings
warnings.filterwarnings("ignore")

PARQUET_PATH = "/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_metadata.parquet"
RGB_PATH     = "/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_rgb_features.npy"
MAX_ROWS     = 250_000

df = pd.read_parquet(PARQUET_PATH)
df = df[df["status"] == "ok"].copy()
df["label_names"] = df["label_names_json"].apply(
    lambda s: json.loads(s) if pd.notna(s) else []
)
df = df[df["label_names"].apply(len) > 0].reset_index(drop=True)

X_all = np.load(RGB_PATH, mmap_mode="r")
print(f"Loaded: {len(df):,} videos, {X_all.shape} features")

if len(df) > MAX_ROWS:
    sample_idx = np.random.RandomState(42).choice(len(df), MAX_ROWS, replace=False)
    sample_idx.sort()
    df    = df.iloc[sample_idx].reset_index(drop=True)
    X_all = np.array(X_all[sample_idx])
    print(f"Sampled down to {len(df):,} videos")
else:
    X_all = np.array(X_all)

print(f"Feature matrix: {X_all.shape}")

mlb         = MultiLabelBinarizer()
Y_all       = mlb.fit_transform(df["label_names"].values)
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


print("\nFitting visual baseline (LR on RGB features)...")
model = OneVsRestClassifier(
    SGDClassifier(loss="log_loss", max_iter=1000, random_state=42),
    n_jobs=4
)
model.fit(X_train, Y_train)
Y_prob = model.predict_proba(X_test)

gap20 = global_average_precision(Y_test, Y_prob, top_k=20)
gap5  = global_average_precision(Y_test, Y_prob, top_k=5)
print(f"\nVisual baseline — RGB features + LR")
print(f"GAP@20={gap20:.4f}  GAP@5={gap5:.4f}")
for thr in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
    Y_pred = (Y_prob >= thr).astype(int)
    p  = precision_score(Y_test, Y_pred, average="micro", zero_division=0)
    r  = recall_score(Y_test, Y_pred, average="micro", zero_division=0)
    mi = f1_score(Y_test, Y_pred, average="micro", zero_division=0)
    ma = f1_score(Y_test, Y_pred, average="macro", zero_division=0)
    print(f"thr={thr:.1f}  P={p:.4f}  R={r:.4f}  Micro F1={mi:.4f}  Macro F1={ma:.4f}")