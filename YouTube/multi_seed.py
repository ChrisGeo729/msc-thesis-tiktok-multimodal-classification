import json
import re
import os
import gc
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from collections import Counter
from torch.utils.data import Dataset, TensorDataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import f1_score
from gensim.models import KeyedVectors
import warnings
warnings.filterwarnings("ignore")

PARQUET_PATH = "/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_metadata.parquet"
RGB_PATH     = "/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_rgb_features.npy"
W2V_PATH     = "/home/cgeorghiou/projects/msc-thesis/data/word2vec/GoogleNews-vectors-negative300.bin"
SAVE_DIR     = "/home/cgeorghiou/projects/msc-thesis/YouTube"
OUT_CSV      = f"{SAVE_DIR}/multi_seed_results_yt8m.csv"
SCRATCH_DIR  = "/scratch-shared/cgeorghiou"


SEEDS      = [42, 123, 456]
MAX_ROWS   = 1_000_000
THR        = 0.3
BATCH_SIZE = 512
EPOCHS     = 5
LR         = 1e-3
L2_DECAY   = 1e-7
PATIENCE   = 3
DROPOUT    = 0.5
DEVICE     = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {DEVICE}")

VISUAL_DIM      = 1024
HIDDEN_DIM      = 4096
MAX_LEN         = 64
EMBED_DIM       = 300
NUM_FILTERS     = 512
KERNEL_SIZES    = list(range(1, 9))
TFIDF_MAX_FEATS = 1000
MIN_FREQ        = 10


def set_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)

def global_average_precision(y_true, y_prob, top_k=20):
    precisions = []
    for i in range(y_true.shape[0]):
        hits = 0
        for rank, idx in enumerate(np.argsort(y_prob[i])[::-1][:top_k], 1):
            if y_true[i, idx] == 1:
                hits += 1
                precisions.append(hits / rank)
    return np.mean(precisions) if precisions else 0.0

def get_metrics(y_prob, y_true, thr):
    gap20 = global_average_precision(y_true, y_prob, top_k=20)
    y_pred = (y_prob >= thr).astype(int)
    micro_f1 = f1_score(y_true, y_pred, average="micro", zero_division=0)
    macro_f1 = f1_score(y_true, y_pred, average="macro", zero_division=0)
    return {"GAP@20": gap20, "micro_f1": micro_f1, "macro_f1": macro_f1}

def make_loader(X, Y, shuffle):
    dtype = torch.long if X.dtype == np.int64 else torch.float32
    return DataLoader(
        TensorDataset(torch.tensor(X, dtype=dtype), torch.tensor(Y, dtype=torch.float32)),
        batch_size=BATCH_SIZE, shuffle=shuffle
    )

def train_loop(model, optimizer, criterion, train_ld, val_ld, save_path):
    best_val, patience_counter = float("inf"), 0
    for epoch in range(1, EPOCHS + 1):
        model.train()
        for X_b, Y_b in train_ld:
            X_b, Y_b = X_b.to(DEVICE), Y_b.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(X_b), Y_b)
            loss.backward()
            optimizer.step()

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for X_b, Y_b in val_ld:
                val_loss += criterion(model(X_b.to(DEVICE)), Y_b.to(DEVICE)).item()
        val_loss /= len(val_ld)

        if val_loss < best_val:
            best_val = val_loss
            torch.save(model.state_dict(), save_path)
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                break

    model.load_state_dict(torch.load(save_path))
    return model

def evaluate(model, loader):
    model.eval()
    all_probs, all_labels = [], []
    with torch.no_grad():
        for X_b, Y_b in loader:
            all_probs.append(torch.sigmoid(model(X_b.to(DEVICE))).cpu().numpy())
            all_labels.append(Y_b.numpy())
    return np.vstack(all_probs), np.vstack(all_labels)

def extract_features(model, X, batch_size=BATCH_SIZE):
    model.eval()
    feats = []
    with torch.no_grad():
        for i in range(0, len(X), batch_size):
            dtype = torch.long if X.dtype == np.int64 else torch.float32
            batch = torch.tensor(X[i:i+batch_size], dtype=dtype).to(DEVICE)
            feats.append(model.extract_features(batch).cpu().numpy().astype(np.float16))
    return np.vstack(feats)


print("Loading data")
df = pd.read_parquet(PARQUET_PATH)
df = df[df["status"] == "ok"].copy()
df["label_names"] = df["label_names_json"].apply(
    lambda s: json.loads(s) if pd.notna(s) else []
)
df = df[df["label_names"].apply(len) > 0].reset_index(drop=True)
print(f"Total labelled: {len(df):,}")

X_visual_all = np.load(RGB_PATH, mmap_mode="r")

if len(df) > MAX_ROWS:
    sample_idx = np.random.RandomState(42).choice(len(df), MAX_ROWS, replace=False)
    sample_idx.sort()
    df = df.iloc[sample_idx].reset_index(drop=True)
    X_visual_all = np.array(X_visual_all[sample_idx], dtype=np.float32)
    print(f"Sampled to {len(df):,}")
else:
    X_visual_all = np.array(X_visual_all, dtype=np.float32)

mlb = MultiLabelBinarizer()
Y_all = mlb.fit_transform(df["label_names"].values).astype(np.float32)
num_classes = Y_all.shape[1]
print(f"Classes: {num_classes}")

indices = np.arange(len(df))
idx_train, idx_test = train_test_split(indices, test_size=0.2, random_state=42)
vis_X_tr, vis_X_te = X_visual_all[idx_train], X_visual_all[idx_test]
vis_Y_tr, vis_Y_te = Y_all[idx_train], Y_all[idx_test]
vis_X_tr, vis_X_val, vis_Y_tr, vis_Y_val = train_test_split(
    vis_X_tr, vis_Y_tr, test_size=0.1, random_state=42
)
print(f"Visual: {len(vis_X_tr):,} train / {len(vis_X_val):,} val / {len(vis_X_te):,} test")


def clean_tokens(text):
    if not isinstance(text, str):
        return []
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    tokens = text.split()
    return tokens if len(tokens) >= 2 else []

def tags_to_text(s):
    try:
        tags = json.loads(s) if isinstance(s, str) else []
        return " ".join(tags).lower() if isinstance(tags, list) else ""
    except Exception:
        return ""

df["title_tokens"] = df["title"].apply(clean_tokens)
df["tags_text"]    = df["tags_json"].apply(tags_to_text)
has_text = (df["title_tokens"].apply(len) > 0) & (df["tags_text"].str.split().apply(len) >= 1)
has_text_arr = has_text.values
print(f"Videos with title+tags: {has_text.sum():,}")

word_counts = Counter()
for tokens in df["title_tokens"].iloc[idx_train]:
    word_counts.update(tokens)

vocab = {"<PAD>": 0, "<UNK>": 1}
for word, count in word_counts.items():
    if count >= MIN_FREQ:
        vocab[word] = len(vocab)
print(f"Vocab: {len(vocab):,} (min_freq={MIN_FREQ})")

def encode(tokens):
    ids = [vocab.get(t, 1) for t in tokens[:MAX_LEN]]
    return ids + [0] * (MAX_LEN - len(ids))

X_title = np.array([encode(t) for t in df["title_tokens"]], dtype=np.int64)

print("Loading Word2Vec")
wv = KeyedVectors.load_word2vec_format(W2V_PATH, binary=True)
embed_matrix = np.zeros((len(vocab), EMBED_DIM), dtype=np.float32)
for word, idx in vocab.items():
    if word in wv:
        embed_matrix[idx] = wv[word]
del wv
gc.collect()

print("Fitting TF-IDF on tags")
tfidf = TfidfVectorizer(max_features=TFIDF_MAX_FEATS)
tfidf.fit(df["tags_text"].iloc[idx_train])
X_tags = tfidf.transform(df["tags_text"]).astype(np.float32)


class VisualHead(nn.Module):
    def __init__(self, n_classes):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(VISUAL_DIM, HIDDEN_DIM), nn.BatchNorm1d(HIDDEN_DIM), nn.ReLU(), nn.Dropout(DROPOUT)
        )
        self.classifier = nn.Linear(HIDDEN_DIM, n_classes)

    def forward(self, x):
        return self.classifier(self.net(x))

    def extract_features(self, x):
        return self.net(x)


class TextCNN(nn.Module):
    def __init__(self, n_classes):
        super().__init__()
        self.embedding = nn.Embedding(len(vocab), EMBED_DIM, padding_idx=0)
        self.embedding.weight = nn.Parameter(torch.tensor(embed_matrix))
        self.convs = nn.ModuleList([
            nn.Sequential(nn.Conv1d(EMBED_DIM, NUM_FILTERS, k), nn.BatchNorm1d(NUM_FILTERS), nn.ReLU())
            for k in KERNEL_SIZES
        ])
        self.hidden = nn.Sequential(
            nn.Linear(NUM_FILTERS * len(KERNEL_SIZES), HIDDEN_DIM),
            nn.BatchNorm1d(HIDDEN_DIM), nn.ReLU(), nn.Dropout(DROPOUT)
        )
        self.classifier = nn.Linear(HIDDEN_DIM, n_classes)

    def _encode(self, x):
        x = self.embedding(x).permute(0, 2, 1)
        x = torch.cat([torch.max(c(x), dim=2).values for c in self.convs], dim=1)
        return self.hidden(x)

    def forward(self, x):
        return self.classifier(self._encode(x))

    def extract_features(self, x):
        return self._encode(x)


class FusionClassifier(nn.Module):
    def __init__(self, input_dim, n_classes):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 512), nn.BatchNorm1d(512), nn.ReLU(), nn.Dropout(DROPOUT),
            nn.Linear(512, n_classes)
        )

    def forward(self, x):
        return self.net(x)


class EarlyFusion(nn.Module):
    def __init__(self, input_dim, n_classes):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, HIDDEN_DIM), nn.BatchNorm1d(HIDDEN_DIM), nn.ReLU(), nn.Dropout(DROPOUT),
            nn.Linear(HIDDEN_DIM, n_classes)
        )

    def forward(self, x):
        return self.net(x)


class MoE(nn.Module):
    def __init__(self, input_dim, num_experts, n_classes):
        super().__init__()
        self.experts = nn.ModuleList([
            nn.Sequential(
                nn.Linear(input_dim, 1024), nn.ReLU(), nn.Dropout(DROPOUT), nn.Linear(1024, n_classes)
            )
            for _ in range(num_experts)
        ])
        self.gate = nn.Linear(input_dim, num_experts)

    def forward(self, x):
        gate_weights = torch.softmax(self.gate(x), dim=-1)
        expert_out = torch.stack([e(x) for e in self.experts], dim=1)
        return (gate_weights.unsqueeze(-1) * expert_out).sum(dim=1)


class ArrayDataset(Dataset):
    def __init__(self, X, labels):
        self.X = X
        self.labels = labels

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, i):
        x = torch.tensor(np.asarray(self.X[i], dtype=np.float32), dtype=torch.float32)
        y = torch.tensor(self.labels[i], dtype=torch.float32)
        return x, y


def make_array_loader(X, Y, shuffle):
    return DataLoader(ArrayDataset(X, Y), batch_size=BATCH_SIZE, shuffle=shuffle)

class FusedIndexDataset(Dataset):
    def __init__(self, text, visual, labels, idx):
        self.text = text
        self.visual = visual
        self.labels = labels
        self.idx = idx

    def __len__(self):
        return len(self.idx)

    def __getitem__(self, j):
        i = self.idx[j]
        t = np.asarray(self.text[i], dtype=np.float32)
        v = np.asarray(self.visual[i], dtype=np.float32)
        x = torch.tensor(np.concatenate([t, v]), dtype=torch.float32)
        y = torch.tensor(self.labels[i], dtype=torch.float32)
        return x, y

def make_fused_idx_loader(text, visual, labels, idx, shuffle):
    return DataLoader(FusedIndexDataset(text, visual, labels, idx),
                      batch_size=BATCH_SIZE, shuffle=shuffle)


criterion = nn.BCEWithLogitsLoss()
all_results = []

for seed in SEEDS:
    print(f"\nSeed {seed}")
    set_seed(seed)

    print("Training visual head")
    save_p = f"{SAVE_DIR}/seed_{seed}_visual.pt"
    model = VisualHead(num_classes).to(DEVICE)
    opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=L2_DECAY)
    model = train_loop(model, opt, criterion,
                       make_loader(vis_X_tr, vis_Y_tr, shuffle=True),
                       make_loader(vis_X_val, vis_Y_val, shuffle=False), save_p)
    y_prob, y_true = evaluate(model, make_loader(vis_X_te, vis_Y_te, shuffle=False))
    m = get_metrics(y_prob, y_true, THR)
    print(f"  Visual: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "Visual head", **m})
    if seed == 42:
        np.save(f"{SCRATCH_DIR}/preds_visual_prob.npy", y_prob)
        np.save(f"{SCRATCH_DIR}/preds_visual_true.npy", y_true)
        np.save(f"{SCRATCH_DIR}/preds_visual_classes.npy", mlb.classes_)

    vis_feats_all = extract_features(model, X_visual_all)
    del model, opt
    os.remove(save_p)
    torch.cuda.empty_cache()
    gc.collect()

    print("  Training TextCNN (titles)")
    set_seed(seed)
    save_p = f"{SAVE_DIR}/seed_{seed}_textcnn_titles.pt"
    X_tr_t = X_title[idx_train][has_text_arr[idx_train]]
    Y_tr_t = Y_all[idx_train][has_text_arr[idx_train]]
    X_te_t = X_title[idx_test][has_text_arr[idx_test]]
    Y_te_t = Y_all[idx_test][has_text_arr[idx_test]]
    X_tr_t, X_vl_t, Y_tr_t, Y_vl_t = train_test_split(X_tr_t, Y_tr_t, test_size=0.1, random_state=42)
    model_t = TextCNN(num_classes).to(DEVICE)
    opt_t = torch.optim.Adam(model_t.parameters(), lr=LR, weight_decay=L2_DECAY)
    model_t = train_loop(model_t, opt_t, criterion,
                         make_loader(X_tr_t, Y_tr_t, shuffle=True),
                         make_loader(X_vl_t, Y_vl_t, shuffle=False), save_p)
    y_prob_t, y_true_t = evaluate(model_t, make_loader(X_te_t, Y_te_t, shuffle=False))
    m = get_metrics(y_prob_t, y_true_t, THR)
    print(f"  TextCNN (titles): GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "TextCNN (titles)", **m})

    text_feats_all = extract_features(model_t, X_title)
    text_feats_all[~has_text_arr] = 0.0
    del model_t, opt_t, X_tr_t, X_vl_t, Y_tr_t, Y_vl_t, X_te_t, Y_te_t
    os.remove(save_p)
    torch.cuda.empty_cache()
    gc.collect()

    print("  Training text fusion (titles + tags)")
    set_seed(seed)
    save_p = f"{SAVE_DIR}/seed_{seed}_text_fusion.pt"
    tr_mask = has_text_arr[idx_train]
    te_mask = has_text_arr[idx_test]

    X_fused_tr_full = np.hstack([
        text_feats_all[idx_train][tr_mask].astype(np.float32),
        X_tags[idx_train][tr_mask].toarray().astype(np.float32)
    ])
    Y_fused_tr_full = Y_all[idx_train][tr_mask]
    X_fused_tr, X_fused_vl, Y_fused_tr, Y_fused_vl = train_test_split(
        X_fused_tr_full, Y_fused_tr_full, test_size=0.1, random_state=42
    )
    del X_fused_tr_full, Y_fused_tr_full
    gc.collect()

    fused_dim = X_fused_tr.shape[1]
    model_f = FusionClassifier(fused_dim, num_classes).to(DEVICE)
    opt_f = torch.optim.Adam(model_f.parameters(), lr=LR)
    model_f = train_loop(model_f, opt_f, criterion,
                         make_loader(X_fused_tr, Y_fused_tr, shuffle=True),
                         make_loader(X_fused_vl, Y_fused_vl, shuffle=False), save_p)
    del X_fused_tr, X_fused_vl, Y_fused_tr, Y_fused_vl
    gc.collect()

    X_fused_te = np.hstack([
        text_feats_all[idx_test][te_mask].astype(np.float32),
        X_tags[idx_test][te_mask].toarray().astype(np.float32)
    ])
    Y_fused_te = Y_all[idx_test][te_mask]
    y_prob_f, y_true_f = evaluate(model_f, make_loader(X_fused_te, Y_fused_te, shuffle=False))
    m = get_metrics(y_prob_f, y_true_f, THR)
    print(f"  Text fusion: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "Text fusion (titles + tags)", **m})
    if seed == 42:
        np.save(f"{SCRATCH_DIR}/preds_text_prob.npy", y_prob_f)
        np.save(f"{SCRATCH_DIR}/preds_text_true.npy", y_true_f)
        np.save(f"{SCRATCH_DIR}/preds_text_classes.npy", mlb.classes_)
    del X_fused_te, Y_fused_te, model_f, opt_f
    os.remove(save_p)
    torch.cuda.empty_cache()
    gc.collect()

    print("  Building multimodal subset")
    mm_text = text_feats_all[has_text_arr]
    mm_vis  = vis_feats_all[has_text_arr]
    mm_Y    = Y_all[has_text_arr]
    print(f"  Multimodal subset: {len(mm_Y):,} videos")

    del vis_feats_all, text_feats_all
    gc.collect()

    mm_indices = np.arange(len(mm_Y))
    mm_idx_train, mm_idx_test = train_test_split(mm_indices, test_size=0.2, random_state=42)
    mm_idx_tr, mm_idx_vl = train_test_split(mm_idx_train, test_size=0.1, random_state=42)

    mm_train_ld = make_fused_idx_loader(mm_text, mm_vis, mm_Y, mm_idx_tr,   shuffle=True)
    mm_val_ld   = make_fused_idx_loader(mm_text, mm_vis, mm_Y, mm_idx_vl,   shuffle=False)
    mm_test_ld  = make_fused_idx_loader(mm_text, mm_vis, mm_Y, mm_idx_test, shuffle=False)
    mm_input_dim = HIDDEN_DIM + HIDDEN_DIM

    print("  Training early fusion")
    set_seed(seed)
    save_p = f"{SAVE_DIR}/seed_{seed}_early_fusion.pt"
    ef = EarlyFusion(mm_input_dim, num_classes).to(DEVICE)
    opt_ef = torch.optim.Adam(ef.parameters(), lr=LR, weight_decay=L2_DECAY)
    ef = train_loop(ef, opt_ef, criterion, mm_train_ld, mm_val_ld, save_p)
    y_prob_ef, y_true_ef = evaluate(ef, mm_test_ld)
    m = get_metrics(y_prob_ef, y_true_ef, THR)
    print(f"  Early fusion: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "Early fusion", **m})
    if seed == 42:
        np.save(f"{SCRATCH_DIR}/preds_multimodal_early_prob.npy", y_prob_ef)
        np.save(f"{SCRATCH_DIR}/preds_multimodal_early_true.npy", y_true_ef)
        np.save(f"{SCRATCH_DIR}/preds_multimodal_early_classes.npy", mlb.classes_)
    del ef, opt_ef
    os.remove(save_p)
    torch.cuda.empty_cache()
    gc.collect()

    print("  Training MoE")
    set_seed(seed)
    save_p = f"{SAVE_DIR}/seed_{seed}_moe.pt"
    moe = MoE(mm_input_dim, 4, num_classes).to(DEVICE)
    opt_moe = torch.optim.Adam(moe.parameters(), lr=LR, weight_decay=L2_DECAY)
    moe = train_loop(moe, opt_moe, criterion, mm_train_ld, mm_val_ld, save_p)
    y_prob_moe, y_true_moe = evaluate(moe, mm_test_ld)
    m = get_metrics(y_prob_moe, y_true_moe, THR)
    print(f"  MoE: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "MoE (4 experts)", **m})
    del moe, opt_moe
    os.remove(save_p)
    torch.cuda.empty_cache()

    del mm_text, mm_vis, mm_Y, mm_train_ld, mm_val_ld, mm_test_ld
    gc.collect()


results_df = pd.DataFrame(all_results)
results_df.to_csv(OUT_CSV, index=False)
print(f"\nSaved: {OUT_CSV}")

summary = results_df.groupby("model").agg(
    GAP20_mean=("GAP@20",    "mean"), GAP20_std=("GAP@20",    "std"),
    microF1_mean=("micro_f1", "mean"), microF1_std=("micro_f1", "std"),
    macroF1_mean=("macro_f1", "mean"), macroF1_std=("macro_f1", "std"),
).reset_index()

print("\nSummary: mean ± std across seeds")
for _, row in summary.iterrows():
    print(f"\n{row['model']}:")
    print(f"  GAP@20   = {row['GAP20_mean']:.4f} +/- {row['GAP20_std']:.4f}")
    print(f"  Micro F1 = {row['microF1_mean']:.4f} +/- {row['microF1_std']:.4f}")
    print(f"  Macro F1 = {row['macroF1_mean']:.4f} +/- {row['macroF1_std']:.4f}")

summary_path = OUT_CSV.replace(".csv", "_summary.csv")
summary.to_csv(summary_path, index=False)
print(f"\nSaved: {summary_path}")