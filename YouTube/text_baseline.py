import json
import re
import pandas as pd
import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import f1_score, precision_score, recall_score
import warnings
warnings.filterwarnings("ignore")

DATA_PATH = "/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_metadata.parquet"
MAX_ROWS  = 250_000


def parse_json_list(s):
    try:
        return json.loads(s) if isinstance(s, str) else []
    except (json.JSONDecodeError, TypeError):
        return []

def clean_text(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def valid_text(s):
    return isinstance(s, str) and len(s.strip().split()) >= 2

def global_average_precision(y_true, y_prob, top_k=20):
    precisions = []
    for i in range(y_true.shape[0]):
        hits = 0
        for rank, idx in enumerate(np.argsort(y_prob[i])[::-1][:top_k], 1):
            if y_true[i, idx] == 1:
                hits += 1
                precisions.append(hits / rank)
    return np.mean(precisions) if precisions else 0.0

def run_baseline(df_model, name):
    print(f"\nTF-IDF baseline — {name}")
    print(f"Videos: {len(df_model):,}  |  Label space: {df_model['label_names'].explode().nunique():,}")

    mlb = MultiLabelBinarizer()
    Y   = mlb.fit_transform(df_model["label_names"])
    X   = df_model["text"]

    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

    pipe = Pipeline([
        ("tfidf", TfidfVectorizer(max_features=10_000)),
        ("clf",   OneVsRestClassifier(
            SGDClassifier(loss="log_loss", max_iter=1000, random_state=42),
            n_jobs=4
        ))
    ])
    pipe.fit(X_train, Y_train)
    Y_prob = pipe.predict_proba(X_test)

    gap20 = global_average_precision(Y_test, Y_prob, top_k=20)
    gap5  = global_average_precision(Y_test, Y_prob, top_k=5)
    print(f"GAP@20={gap20:.4f}  GAP@5={gap5:.4f}")
    for thr in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
        Y_pred = (Y_prob >= thr).astype(int)
        p  = precision_score(Y_test, Y_pred, average="micro", zero_division=0)
        r  = recall_score(Y_test, Y_pred, average="micro", zero_division=0)
        mi = f1_score(Y_test, Y_pred, average="micro", zero_division=0)
        ma = f1_score(Y_test, Y_pred, average="macro", zero_division=0)
        print(f"thr={thr:.1f}  P={p:.4f}  R={r:.4f}  Micro F1={mi:.4f}  Macro F1={ma:.4f}")


df = pd.read_parquet(DATA_PATH)
df = df[df["status"] == "ok"].copy()
df["label_names"] = df["label_names_json"].apply(parse_json_list)
df = df[df["label_names"].apply(len) > 0].copy()

if len(df) > MAX_ROWS:
    df = df.sample(n=MAX_ROWS, random_state=42).reset_index(drop=True)
    print(f"Sampled down to {len(df):,} videos")

df_tags = df.copy()
df_tags["text"] = df_tags["tags_json"].apply(lambda s: clean_text(" ".join(parse_json_list(s))))
df_tags = df_tags[df_tags["text"].apply(valid_text)].reset_index(drop=True)
run_baseline(df_tags, "Tags only")

df_both = df[df["title"].notna()].copy()
df_both["tags_str"] = df_both["tags_json"].apply(lambda s: clean_text(" ".join(parse_json_list(s))))
df_both["text"]     = (df_both["title"].apply(clean_text) + " " + df_both["tags_str"]).str.strip()
df_both = df_both[df_both["text"].apply(valid_text)].reset_index(drop=True)
run_baseline(df_both, "Titles + tags")