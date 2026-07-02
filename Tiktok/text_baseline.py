import re
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.multiclass import OneVsRestClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import f1_score, precision_score, recall_score
from langdetect import detect, DetectorFactory
import warnings
warnings.filterwarnings("ignore")

DATA_PATH = "/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_with_visual_features.csv"
DetectorFactory.seed = 0


def parse_labels(s):
    if not isinstance(s, str):
        return []
    return re.findall(r'"([^"]+)"', s)


def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"(https?://\S+|www\.\S+)", "", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def is_english(s):
    if not isinstance(s, str) or len(s.strip().split()) < 2:
        return False
    try:
        return detect(s) == "en"
    except Exception:
        return False


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
    print(f"Videos: {len(df_model):,}  |  Label space: {df_model['labels_parsed'].explode().nunique():,}")

    mlb = MultiLabelBinarizer()
    Y = mlb.fit_transform(df_model["labels_parsed"])
    X = df_model["text"]
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    print(f"Train: {len(X_train):,}  Test: {len(X_test):,}")

    pipe = Pipeline([
        ("tfidf", TfidfVectorizer()),
        ("clf", OneVsRestClassifier(
            LogisticRegression(max_iter=1000, solver="liblinear", class_weight="balanced")
        )),
    ])
    pipe.fit(X_train, Y_train)
    Y_prob = pipe.predict_proba(X_test)

    gap20 = global_average_precision(Y_test, Y_prob, top_k=20)
    gap5 = global_average_precision(Y_test, Y_prob, top_k=5)
    print(f"GAP@20={gap20:.4f}  GAP@5={gap5:.4f}")
    for thr in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
        Y_pred = (Y_prob >= thr).astype(int)
        p = precision_score(Y_test, Y_pred, average="micro", zero_division=0)
        r = recall_score(Y_test, Y_pred, average="micro", zero_division=0)
        mi = f1_score(Y_test, Y_pred, average="micro", zero_division=0)
        ma = f1_score(Y_test, Y_pred, average="macro", zero_division=0)
        print(f"thr={thr:.1f}  P={p:.4f}  R={r:.4f}  Micro F1={mi:.4f}  Macro F1={ma:.4f}")


df = pd.read_csv(DATA_PATH, low_memory=False)
df["labels_parsed"] = df["Labels"].apply(parse_labels)
df = df[df["labels_parsed"].apply(len) > 0].copy()


def prep(frame, source_col_expr):
    f = frame.copy()
    f["text"] = source_col_expr(f)
    f = f[f["text"].apply(is_english)]
    f = f.drop_duplicates(subset="text")
    f = f[f["text"].apply(valid_text)]
    return f.reset_index(drop=True)


df_cap = prep(df[df["Captions"].notna()], lambda f: f["Captions"].apply(clean_text))
run_baseline(df_cap, "Captions only")

df_desc = prep(df[df["Description"].notna()], lambda f: f["Description"].apply(clean_text))
run_baseline(df_desc, "Descriptions only")

df_all = prep(df[df["Captions"].notna() | df["Description"].notna()], lambda f: (f["Captions"].apply(clean_text) + " " + f["Description"].apply(clean_text)).str.strip())
run_baseline(df_all, "Captions + descriptions")