import json
import re
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from gensim.models import KeyedVectors

PARQUET_PATH = "/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_metadata.parquet"
W2V_PATH     = "/home/cgeorghiou/projects/msc-thesis/data/word2vec/GoogleNews-vectors-negative300.bin"
SAVE_PATH    = "/home/cgeorghiou/projects/msc-thesis/YouTube/textcnn_yt8m_tags.pt"
MAX_ROWS     = 1_000_000
HIDDEN_DIM   = 4096
BATCH_SIZE   = 512
DEVICE       = "cuda"

def parse_json_list(s):
    try:
        return json.loads(s) if isinstance(s, str) else []
    except:
        return []

def clean_text(text):
    if not isinstance(text, str):
        return []
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^\w\s]", "", text)
    tokens = text.split()
    return tokens if len(tokens) >= 2 else []

df = pd.read_parquet(PARQUET_PATH)
df = df[df["status"] == "ok"].copy()
df["label_names"] = df["label_names_json"].apply(parse_json_list)
df = df[df["label_names"].apply(len) > 0].copy()

if len(df) > MAX_ROWS:
    df = df.sample(n=MAX_ROWS, random_state=42).reset_index(drop=True)

df["tags_tokens"] = df["tags_json"].apply(parse_json_list).apply(
    lambda tags: clean_text(" ".join(tags)) if tags else []
)
has_tags = df["tags_tokens"].apply(len) > 0

mlb = MultiLabelBinarizer()
mlb.fit_transform(df["label_names"].values)
num_classes = len(mlb.classes_)

idx_train, _ = train_test_split(np.arange(len(df)), test_size=0.2, random_state=42)

print("Loading Word2Vec...")
checkpoint = torch.load(SAVE_PATH)
vocab_size = checkpoint['embedding.weight'].shape[0]
embed_matrix = np.zeros((vocab_size, 300), dtype=np.float32)
print(f"Vocab size from checkpoint: {vocab_size:,}")

def encode(tokens, max_len=32):
    ids = [1] * min(len(tokens), max_len)  # all UNK
    ids += [0] * (max_len - len(ids))
    return ids

X_tags = np.array([encode(t) for t in df["tags_tokens"]], dtype=np.int64)

class TextCNN(nn.Module):
    def __init__(self):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, 300, padding_idx=0)
        self.embedding.weight = nn.Parameter(torch.tensor(embed_matrix))
        self.convs = nn.ModuleList([
            nn.Sequential(nn.Conv1d(300, 512, k), nn.BatchNorm1d(512), nn.ReLU())
            for k in range(1, 9)
        ])
        self.hidden = nn.Sequential(
            nn.Linear(512 * 8, HIDDEN_DIM), nn.BatchNorm1d(HIDDEN_DIM),
            nn.ReLU(), nn.Dropout(0.5)
        )
        self.classifier = nn.Linear(HIDDEN_DIM, num_classes)

    def extract_features(self, x):
        x = self.embedding(x).permute(0, 2, 1)
        x = [torch.max(c(x), dim=2).values for c in self.convs]
        return self.hidden(torch.cat(x, dim=1))

model = TextCNN().to(DEVICE)
model.load_state_dict(torch.load(SAVE_PATH))
model.eval()

print("Extracting tags features...")
has_tags_arr = has_tags.values
features = np.zeros((len(df), HIDDEN_DIM), dtype=np.float32)
with torch.no_grad():
    for i in range(0, len(df), BATCH_SIZE):
        batch = torch.tensor(X_tags[i:i+BATCH_SIZE], dtype=torch.long).to(DEVICE)
        features[i:i+len(batch)] = model.extract_features(batch).cpu().numpy()
features[~has_tags_arr] = 0.0

feat_path = "/scratch-shared/cgeorghiou/textcnn_yt8m_tags_features.npy"
np.save(feat_path, features)
print(f"Saved: {features.shape} → {feat_path}")