import json
import re
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import f1_score, precision_score, recall_score
from gensim.models import KeyedVectors
import warnings
warnings.filterwarnings("ignore")

W2V_PATH    = "/home/cgeorghiou/projects/msc-thesis/data/word2vec/GoogleNews-vectors-negative300.bin"
SAVE_DIR    = "/home/cgeorghiou/projects/msc-thesis/YouTube"
SCRATCH_DIR = "/scratch-shared/cgeorghiou"
MAX_ROWS    = 1_000_000

MAX_LEN      = 32
EMBED_DIM    = 300
NUM_FILTERS  = 512
KERNEL_SIZES = list(range(1, 9))
HIDDEN_DIM   = 4096
DROPOUT      = 0.5
BATCH_SIZE   = 512
EPOCHS       = 5
LR           = 1e-3
L2_DECAY     = 1e-7
PATIENCE     = 3
DEVICE       = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {DEVICE}")

df = pd.read_parquet('/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_metadata.parquet')
df = df[df["status"] == "ok"].copy()

def parse_json_list(s):
    try:
        return json.loads(s) if isinstance(s, str) else []
    except (json.JSONDecodeError, TypeError):
        return []

df["label_names"] = df["label_names_json"].apply(parse_json_list)
df = df[df["label_names"].apply(len) > 0].copy()

if len(df) > MAX_ROWS:
    df = df.sample(n=MAX_ROWS, random_state=42).reset_index(drop=True)
    print(f"Sampled down to {len(df):,} videos")

def clean_text(text):
    if not isinstance(text, str):
        return []
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^\w\s]", "", text)
    tokens = text.split()
    return tokens if len(tokens) >= 2 else []

df["title_tokens"] = df["title"].apply(clean_text)
df["tags_tokens"]  = df["tags_json"].apply(
    lambda s: clean_text(" ".join(parse_json_list(s)))
)

has_title = df["title_tokens"].apply(len) > 0
has_tags  = df["tags_tokens"].apply(len) > 0
print(f"Videos with labels       : {len(df):,}")
print(f"Videos with usable titles: {has_title.sum():,} ({has_title.mean():.1%})")
print(f"Videos with usable tags  : {has_tags.sum():,}  ({has_tags.mean():.1%})")

mlb = MultiLabelBinarizer()
Y_all = mlb.fit_transform(df["label_names"].values)
num_classes = Y_all.shape[1]
print(f"Label space: {num_classes}")

indices                  = np.arange(len(df))
idx_train, idx_test      = train_test_split(indices, test_size=0.2, random_state=42)
Y_train, Y_test          = Y_all[idx_train], Y_all[idx_test]

print("Loading Word2Vec...")
wv = KeyedVectors.load_word2vec_format(W2V_PATH, binary=True)

vocab = {"<PAD>": 0, "<UNK>": 1}
for tokens in df["title_tokens"].iloc[idx_train]:
    for t in tokens:
        if t not in vocab:
            vocab[t] = len(vocab)
for tokens in df["tags_tokens"].iloc[idx_train]:
    for t in tokens:
        if t not in vocab:
            vocab[t] = len(vocab)

embed_matrix = np.zeros((len(vocab), EMBED_DIM), dtype=np.float32)
n_found = 0
for word, idx in vocab.items():
    if word in wv:
        embed_matrix[idx] = wv[word]
        n_found += 1
print(f"Vocab size: {len(vocab)}  |  W2V coverage: {n_found/len(vocab):.1%}")
del wv

def encode(tokens, max_len=MAX_LEN):
    ids = [vocab.get(t, 1) for t in tokens[:max_len]]
    ids += [0] * (max_len - len(ids))
    return ids

X_title = np.array([encode(t) for t in df["title_tokens"]], dtype=np.int64)
X_tags  = np.array([encode(t) for t in df["tags_tokens"]],  dtype=np.int64)

def make_loader(X, Y, shuffle):
    dtype = torch.long if X.dtype == np.int64 else torch.float32
    return DataLoader(
        TensorDataset(torch.tensor(X, dtype=dtype), torch.tensor(Y, dtype=torch.float32)),
        batch_size=BATCH_SIZE, shuffle=shuffle
    )

def make_split(X_encoded, has_mask):
    has_arr = has_mask.values
    X_tr    = X_encoded[idx_train][has_arr[idx_train]]
    Y_tr    = Y_train[has_arr[idx_train]]
    X_te    = X_encoded[idx_test][has_arr[idx_test]]
    Y_te    = Y_test[has_arr[idx_test]]
    return X_tr, Y_tr, X_te, Y_te, has_arr

def global_average_precision(y_true, y_prob, top_k=20):
    precisions = []
    for i in range(len(y_true)):
        top_idx    = np.argsort(y_prob[i])[::-1][:top_k]
        n_correct  = 0
        for rank, idx in enumerate(top_idx, 1):
            if y_true[i, idx] == 1:
                n_correct += 1
                precisions.append(n_correct / rank)
    return np.mean(precisions) if precisions else 0.0

def print_metrics(Y_prob, Y_true, label=""):
    if label:
        print(f"\n{label}")
    gap20 = global_average_precision(Y_true, Y_prob, top_k=20)
    gap5  = global_average_precision(Y_true, Y_prob, top_k=5)
    print(f"GAP@20={gap20:.4f}  GAP@5={gap5:.4f}")
    for thr in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
        Y_pred = (Y_prob >= thr).astype(int)
        p    = precision_score(Y_true, Y_pred, average="micro", zero_division=0)
        r    = recall_score(Y_true, Y_pred, average="micro", zero_division=0)
        mif1 = f1_score(Y_true, Y_pred, average="micro", zero_division=0)
        maf1 = f1_score(Y_true, Y_pred, average="macro", zero_division=0)
        print(f"thr={thr:.1f}  P={p:.4f}  R={r:.4f}  Micro F1={mif1:.4f}  Macro F1={maf1:.4f}")

def train_loop(model, optimizer, criterion, train_ld, val_ld, save_path):
    best_val, patience_counter = float("inf"), 0
    for epoch in range(1, EPOCHS + 1):
        model.train()
        total_loss = 0.0
        for X_batch, Y_batch in train_ld:
            X_batch, Y_batch = X_batch.to(DEVICE), Y_batch.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(X_batch), Y_batch)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for X_batch, Y_batch in val_ld:
                val_loss += criterion(model(X_batch.to(DEVICE)), Y_batch.to(DEVICE)).item()
        val_loss /= len(val_ld)
        print(f"Epoch {epoch}/{EPOCHS}  train={total_loss/len(train_ld):.4f}  val={val_loss:.4f}")

        if val_loss < best_val:
            best_val = val_loss
            torch.save(model.state_dict(), save_path)
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                print(f"Early stopping at epoch {epoch}")
                break

    model.load_state_dict(torch.load(save_path))
    return model

def evaluate(model, loader):
    model.eval()
    all_probs, all_labels = [], []
    with torch.no_grad():
        for X_batch, Y_batch in loader:
            all_probs.append(torch.sigmoid(model(X_batch.to(DEVICE))).cpu().numpy())
            all_labels.append(Y_batch.numpy())
    return np.vstack(all_probs), np.vstack(all_labels)


class TextCNN(nn.Module):
    def __init__(self):
        super().__init__()
        self.embedding = nn.Embedding(len(vocab), EMBED_DIM, padding_idx=0)
        self.embedding.weight = nn.Parameter(torch.tensor(embed_matrix))
        self.convs = nn.ModuleList([
            nn.Sequential(
                nn.Conv1d(EMBED_DIM, NUM_FILTERS, k),
                nn.BatchNorm1d(NUM_FILTERS),
                nn.ReLU(),
            ) for k in KERNEL_SIZES
        ])
        self.hidden = nn.Sequential(
            nn.Linear(NUM_FILTERS * len(KERNEL_SIZES), HIDDEN_DIM),
            nn.BatchNorm1d(HIDDEN_DIM),
            nn.ReLU(),
            nn.Dropout(DROPOUT),
        )
        self.classifier = nn.Linear(HIDDEN_DIM, num_classes)

    def _encode(self, x):
        x = self.embedding(x).permute(0, 2, 1)
        x = torch.cat([torch.max(c(x), dim=2).values for c in self.convs], dim=1)
        return self.hidden(x)

    def forward(self, x):
        return self.classifier(self._encode(x))

    def extract_features(self, x):
        return self._encode(x)


criterion = nn.BCEWithLogitsLoss()


def run_textcnn(X_encoded, has_mask, name):
    X_tr, Y_tr, X_te, Y_te, has_arr = make_split(X_encoded, has_mask)
    X_tr, X_val, Y_tr, Y_val = train_test_split(X_tr, Y_tr, test_size=0.1, random_state=42)
    print(f"\nTrain: {len(X_tr):,}  Val: {len(X_val):,}  Test: {len(X_te):,}")

    train_ld = make_loader(X_tr,  Y_tr,  shuffle=True)
    val_ld   = make_loader(X_val, Y_val, shuffle=False)
    test_ld  = make_loader(X_te,  Y_te,  shuffle=False)

    save_path = f"{SCRATCH_DIR}/textcnn_yt8m_{name}.pt"
    model     = TextCNN().to(DEVICE)
    optimizer = torch.optim.Adam([
        {"params": model.embedding.parameters(),  "lr": 1e-5},
        {"params": model.convs.parameters(),      "lr": LR, "weight_decay": L2_DECAY},
        {"params": model.hidden.parameters(),     "lr": LR, "weight_decay": L2_DECAY},
        {"params": model.classifier.parameters(), "lr": LR, "weight_decay": L2_DECAY},
    ])

    print(f"Training TextCNN on {name}...")
    model = train_loop(model, optimizer, criterion, train_ld, val_ld, save_path)

    Y_prob, Y_true = evaluate(model, test_ld)
    print_metrics(Y_prob, Y_true, f"TextCNN ({name}, standalone)")

    features = np.zeros((len(df), HIDDEN_DIM), dtype=np.float32)
    model.eval()
    with torch.no_grad():
        for i in range(0, len(df), BATCH_SIZE):
            batch = torch.tensor(X_encoded[i:i+BATCH_SIZE], dtype=torch.long).to(DEVICE)
            features[i:i+len(batch)] = model.extract_features(batch).cpu().numpy()
    features[~has_arr] = 0.0

    feat_path = f"{SCRATCH_DIR}/textcnn_yt8m_{name}_features.npy"
    vid_path  = f"{SCRATCH_DIR}/textcnn_yt8m_{name}_videoids.npy"
    np.save(feat_path, features)
    np.save(vid_path,  df["YouTube_id"].values)
    print(f"Saved features: {features.shape} -> {feat_path}")
    print(f"Saved video IDs -> {vid_path}")
    return features

title_features = run_textcnn(X_title, has_title, "titles")
tags_features  = run_textcnn(X_tags,  has_tags,  "tags")

df["tags_str"] = df["tags_json"].apply(
    lambda s: " ".join(parse_json_list(s)) if isinstance(s, str) else ""
)
tfidf   = TfidfVectorizer(max_features=1000)
tfidf.fit(df["tags_str"].iloc[idx_train])
X_tfidf = tfidf.transform(df["tags_str"]).toarray().astype(np.float32)


class FusionClassifier(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 512),
            nn.BatchNorm1d(512),
            nn.ReLU(),
            nn.Dropout(DROPOUT),
            nn.Linear(512, num_classes),
        )

    def forward(self, x):
        return self.net(x)


idx_tr, idx_vl     = train_test_split(np.arange(len(idx_train)), test_size=0.1, random_state=42)
X_tr = np.hstack([title_features[idx_train[idx_tr]], X_tfidf[idx_train[idx_tr]]])
X_vl = np.hstack([title_features[idx_train[idx_vl]], X_tfidf[idx_train[idx_vl]]])
X_te = np.hstack([title_features[idx_test],          X_tfidf[idx_test]])
Y_tr, Y_vl = Y_train[idx_tr], Y_train[idx_vl]
print(f"\nFusion input dim: {X_tr.shape[1]}")

train_ld = make_loader(X_tr, Y_tr,    shuffle=True)
val_ld   = make_loader(X_vl, Y_vl,    shuffle=False)
test_ld  = make_loader(X_te, Y_test,  shuffle=False)

fusion_model = FusionClassifier(X_tr.shape[1]).to(DEVICE)
optimizer    = torch.optim.Adam(fusion_model.parameters(), lr=LR)

print("Training fusion classifier (titles + TF-IDF tags)...")
fusion_model = train_loop(fusion_model, optimizer, criterion, train_ld, val_ld,
                          f"{SCRATCH_DIR}/textcnn_yt8m_fusion.pt")

Y_prob, Y_true = evaluate(fusion_model, test_ld)
print_metrics(Y_prob, Y_true, "TextCNN (titles) + TF-IDF (tags) fusion")

np.save(f"{SCRATCH_DIR}/preds_text_prob.npy",    Y_prob)
np.save(f"{SCRATCH_DIR}/preds_text_true.npy",    Y_true)
np.save(f"{SCRATCH_DIR}/preds_text_classes.npy", mlb.classes_)