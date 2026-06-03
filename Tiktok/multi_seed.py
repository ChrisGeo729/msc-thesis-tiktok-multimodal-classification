import json
import re
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, TensorDataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import f1_score
from gensim.models import KeyedVectors
from wordsegment import load, segment
load()
import warnings, os
warnings.filterwarnings("ignore")

DATA_PATH = "/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_with_visual_features.csv"
W2V_PATH  = "/home/cgeorghiou/projects/msc-thesis/data/word2vec/GoogleNews-vectors-negative300.bin"
SAVE_DIR  = "/home/cgeorghiou/projects/msc-thesis/Tiktok"
OUT_CSV   = f"{SAVE_DIR}/multi_seed_results.csv"

SEEDS      = [42, 123, 456]
THR        = 0.3
BATCH_SIZE = 512
EPOCHS     = 5
LR         = 1e-3
L2_DECAY   = 1e-7
PATIENCE   = 3
DROPOUT    = 0.6
DEVICE     = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {DEVICE}")


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
    gap20    = global_average_precision(y_true, y_prob, top_k=20)
    y_pred   = (y_prob >= thr).astype(int)
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
        total_loss = 0.0
        for X_b, Y_b in train_ld:
            X_b, Y_b = X_b.to(DEVICE), Y_b.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(X_b), Y_b)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

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
            feats.append(model.extract_features(batch).cpu().numpy())
    return np.vstack(feats)


print("Loading data...")
df_visual = pd.read_csv(DATA_PATH, low_memory=False)
df_visual["label_list"] = df_visual["Labels"].apply(
    lambda s: re.findall(r'"([^"]+)"', s) if isinstance(s, str) else []
)
df_visual = df_visual[df_visual["label_list"].apply(len) > 0].copy()
df_visual = df_visual[df_visual["embedding"].notna()].copy()
df_visual = df_visual.drop_duplicates(subset="VideoId", keep="first").reset_index(drop=True)

VISUAL_DIM   = 768
X_visual_all = np.zeros((len(df_visual), VISUAL_DIM), dtype=np.float32)
valid_mask   = np.ones(len(df_visual), dtype=bool)
for i, s in enumerate(df_visual["embedding"]):
    try:
        vec = json.loads(s)
        if len(vec) == VISUAL_DIM:
            X_visual_all[i] = vec
        else:
            valid_mask[i] = False
    except Exception:
        valid_mask[i] = False

df_visual    = df_visual[valid_mask].reset_index(drop=True)
X_visual_all = X_visual_all[valid_mask]

mlb_visual        = MultiLabelBinarizer()
Y_visual_all      = mlb_visual.fit_transform(df_visual["label_list"].values)
num_classes_visual = Y_visual_all.shape[1]

vis_indices                  = np.arange(len(df_visual))
vis_idx_train, vis_idx_test  = train_test_split(vis_indices, test_size=0.2, random_state=42)
vis_X_tr, vis_X_te           = X_visual_all[vis_idx_train], X_visual_all[vis_idx_test]
vis_Y_tr, vis_Y_te           = Y_visual_all[vis_idx_train], Y_visual_all[vis_idx_test]
vis_X_tr, vis_X_val, vis_Y_tr, vis_Y_val = train_test_split(
    vis_X_tr, vis_Y_tr, test_size=0.1, random_state=42
)
print(f"Visual: {len(vis_X_tr):,} train / {len(vis_X_val):,} val / {len(vis_X_te):,} test  |  {num_classes_visual} classes")

df_text = pd.read_csv(DATA_PATH, low_memory=False)
df_text["label_list"] = df_text["Labels"].apply(
    lambda s: re.findall(r'"([^"]+)"', s) if isinstance(s, str) else []
)
df_text = df_text[df_text["label_list"].apply(len) > 0].copy()
df_text = df_text[df_text["Description"].notna()].reset_index(drop=True)

try:
    from langdetect import detect, LangDetectException
    def is_english(text):
        if not isinstance(text, str) or len(text.strip()) < 10:
            return False
        try:
            return detect(text) == "en"
        except LangDetectException:
            return False
    df_text = df_text[df_text["Description"].apply(is_english)].reset_index(drop=True)
except ImportError:
    pass

df_text = df_text.drop_duplicates(subset="VideoId", keep="first").reset_index(drop=True)

mlb_text          = MultiLabelBinarizer()
Y_text_all        = mlb_text.fit_transform(df_text["label_list"].values)
num_classes_text  = Y_text_all.shape[1]
text_indices                     = np.arange(len(df_text))
text_idx_train, text_idx_test    = train_test_split(text_indices, test_size=0.2, random_state=42)
Y_text_train, Y_text_test        = Y_text_all[text_idx_train], Y_text_all[text_idx_test]

MAX_LEN, EMBED_DIM, NUM_FILTERS = 64, 300, 512
KERNEL_SIZES    = list(range(1, 9))
HIDDEN_DIM_TEXT = 1024

def clean_text(text):
    if not isinstance(text, str):
        return []
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"#(\w+)", lambda m: " ".join(segment(m.group(1))), text)
    text = re.sub(r"[^\w\s]", "", text)
    tokens = text.split()
    return tokens if len(tokens) >= 2 else []

df_text["desc_tokens"] = df_text["Description"].apply(clean_text)
has_desc     = df_text["desc_tokens"].apply(len) > 0
has_desc_arr = has_desc.values

vocab = {"<PAD>": 0, "<UNK>": 1}
for tokens in df_text["desc_tokens"].iloc[text_idx_train]:
    for t in tokens:
        if t not in vocab:
            vocab[t] = len(vocab)

def encode(tokens):
    ids = [vocab.get(t, 1) for t in tokens[:MAX_LEN]]
    return ids + [0] * (MAX_LEN - len(ids))

X_desc = np.array([encode(t) for t in df_text["desc_tokens"]], dtype=np.int64)

print("Loading Word2Vec...")
wv = KeyedVectors.load_word2vec_format(W2V_PATH, binary=True)
embed_matrix = np.zeros((len(vocab), EMBED_DIM), dtype=np.float32)
for word, idx in vocab.items():
    if word in wv:
        embed_matrix[idx] = wv[word]
del wv
print(f"Text: {has_desc.sum():,} usable / {len(df_text):,} total  |  {num_classes_text} classes")

df_text["hashtags"] = df_text["Description"].apply(
    lambda t: " ".join(re.findall(r"#(\w+)", t.lower())) if isinstance(t, str) else ""
)
tfidf = TfidfVectorizer(max_features=5000)
tfidf.fit(df_text["hashtags"].iloc[text_idx_train])
X_hashtags = tfidf.transform(df_text["hashtags"]).toarray().astype(np.float32)


class VisualHead(nn.Module):
    def __init__(self, n_classes):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(VISUAL_DIM, 1024), nn.BatchNorm1d(1024), nn.ReLU(), nn.Dropout(DROPOUT)
        )
        self.classifier = nn.Linear(1024, n_classes)

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
            nn.Linear(NUM_FILTERS * len(KERNEL_SIZES), HIDDEN_DIM_TEXT),
            nn.BatchNorm1d(HIDDEN_DIM_TEXT), nn.ReLU(), nn.Dropout(DROPOUT)
        )
        self.classifier = nn.Linear(HIDDEN_DIM_TEXT, n_classes)

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
            nn.Linear(input_dim, 1024), nn.BatchNorm1d(1024), nn.ReLU(), nn.Dropout(DROPOUT),
            nn.Linear(1024, n_classes)
        )

    def forward(self, x):
        return self.net(x)


class MoE(nn.Module):
    def __init__(self, input_dim, num_experts, n_classes):
        super().__init__()
        self.experts = nn.ModuleList([
            nn.Sequential(
                nn.Linear(input_dim, 512), nn.ReLU(), nn.Dropout(DROPOUT), nn.Linear(512, n_classes)
            )
            for _ in range(num_experts)
        ])
        self.gate = nn.Linear(input_dim, num_experts)

    def forward(self, x):
        gate_weights = torch.softmax(self.gate(x), dim=-1)
        expert_out   = torch.stack([e(x) for e in self.experts], dim=1)
        return (gate_weights.unsqueeze(-1) * expert_out).sum(dim=1)


class FusedDataset(Dataset):
    def __init__(self, text, visual, labels):
        self.text   = torch.tensor(text,   dtype=torch.float32)
        self.visual = torch.tensor(visual, dtype=torch.float32)
        self.labels = torch.tensor(labels, dtype=torch.float32)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, i):
        return torch.cat([self.text[i], self.visual[i]]), self.labels[i]


def make_fused_loader(text, visual, labels, shuffle):
    return DataLoader(FusedDataset(text, visual, labels), batch_size=BATCH_SIZE, shuffle=shuffle)


criterion   = nn.BCEWithLogitsLoss()
all_results = []

for seed in SEEDS:
    print(f"\nSeed {seed}")
    set_seed(seed)

    # visual head
    print(f"  Training visual head...")
    save_p   = f"{SAVE_DIR}/seed_{seed}_visual.pt"
    model    = VisualHead(num_classes_visual).to(DEVICE)
    opt      = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=L2_DECAY)
    train_ld = make_loader(vis_X_tr,  vis_Y_tr,  shuffle=True)
    val_ld   = make_loader(vis_X_val, vis_Y_val, shuffle=False)
    test_ld  = make_loader(vis_X_te,  vis_Y_te,  shuffle=False)
    model    = train_loop(model, opt, criterion, train_ld, val_ld, save_p)
    y_prob, y_true = evaluate(model, test_ld)
    m = get_metrics(y_prob, y_true, THR)
    print(f"  Visual: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "Visual head", **m})

    vis_feats_all  = extract_features(model, X_visual_all)
    vis_feat_lookup = {vid: vis_feats_all[i] for i, vid in enumerate(df_visual["VideoId"])}
    os.remove(save_p)

    # TextCNN on descriptions
    print(f"  Training TextCNN (descriptions)...")
    set_seed(seed)
    save_p    = f"{SAVE_DIR}/seed_{seed}_textcnn_desc.pt"
    X_tr_desc = X_desc[text_idx_train][has_desc_arr[text_idx_train]]
    Y_tr_desc = Y_text_train[has_desc_arr[text_idx_train]]
    X_te_desc = X_desc[text_idx_test][has_desc_arr[text_idx_test]]
    Y_te_desc = Y_text_test[has_desc_arr[text_idx_test]]
    X_tr_d, X_vl_d, Y_tr_d, Y_vl_d = train_test_split(X_tr_desc, Y_tr_desc, test_size=0.1, random_state=42)
    model_t = TextCNN(num_classes_text).to(DEVICE)
    opt_t   = torch.optim.Adam(model_t.parameters(), lr=LR, weight_decay=L2_DECAY)
    model_t = train_loop(model_t, opt_t, criterion,
                         make_loader(X_tr_d, Y_tr_d, shuffle=True),
                         make_loader(X_vl_d, Y_vl_d, shuffle=False), save_p)
    y_prob_t, y_true_t = evaluate(model_t, make_loader(X_te_desc, Y_te_desc, shuffle=False))
    m = get_metrics(y_prob_t, y_true_t, THR)
    print(f"  TextCNN (desc): GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "TextCNN (descriptions)", **m})

    text_feats_all              = extract_features(model_t, X_desc)
    text_feats_all[~has_desc_arr] = 0.0
    text_feat_lookup            = {vid: text_feats_all[i] for i, vid in enumerate(df_text["VideoId"])}
    os.remove(save_p)

    # text fusion (desc + hashtags)
    print(f"  Training text fusion (desc + hashtags)...")
    set_seed(seed)
    save_p   = f"{SAVE_DIR}/seed_{seed}_text_fusion.pt"
    X_fused  = np.hstack([text_feats_all, X_hashtags])
    X_fused_tr, X_fused_vl, Y_fused_tr, Y_fused_vl = train_test_split(
        X_fused[text_idx_train], Y_text_train, test_size=0.1, random_state=42
    )
    model_f = FusionClassifier(X_fused.shape[1], num_classes_text).to(DEVICE)
    opt_f   = torch.optim.Adam(model_f.parameters(), lr=LR)
    model_f = train_loop(model_f, opt_f, criterion,
                         make_loader(X_fused_tr, Y_fused_tr, shuffle=True),
                         make_loader(X_fused_vl, Y_fused_vl, shuffle=False), save_p)
    y_prob_f, y_true_f = evaluate(model_f, make_loader(X_fused[text_idx_test], Y_text_test, shuffle=False))
    m = get_metrics(y_prob_f, y_true_f, THR)
    print(f"  Text fusion: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "Text fusion (desc + hashtags)", **m})
    os.remove(save_p)

    # multimodal — align by VideoId intersection
    common_ids = sorted(set(text_feat_lookup.keys()) & set(vis_feat_lookup.keys()))
    df_mm      = df_visual[df_visual["VideoId"].isin(common_ids)].drop_duplicates(
        subset="VideoId", keep="first").reset_index(drop=True)
    X_mm_text  = np.array([text_feat_lookup[vid] for vid in df_mm["VideoId"]], dtype=np.float32)
    X_mm_vis   = np.array([vis_feat_lookup[vid]  for vid in df_mm["VideoId"]], dtype=np.float32)
    mlb_mm     = MultiLabelBinarizer()
    Y_mm       = mlb_mm.fit_transform(df_mm["label_list"].values)
    num_classes_mm = Y_mm.shape[1]

    mm_idx                       = np.arange(len(df_mm))
    mm_idx_train, mm_idx_test    = train_test_split(mm_idx, test_size=0.2, random_state=42)
    mm_idx_tr, mm_idx_vl         = train_test_split(mm_idx_train, test_size=0.1, random_state=42)
    mm_train_ld = make_fused_loader(X_mm_text[mm_idx_tr],   X_mm_vis[mm_idx_tr],   Y_mm[mm_idx_tr],   shuffle=True)
    mm_val_ld   = make_fused_loader(X_mm_text[mm_idx_vl],   X_mm_vis[mm_idx_vl],   Y_mm[mm_idx_vl],   shuffle=False)
    mm_test_ld  = make_fused_loader(X_mm_text[mm_idx_test], X_mm_vis[mm_idx_test], Y_mm[mm_idx_test], shuffle=False)
    mm_input_dim = HIDDEN_DIM_TEXT + 1024

    # early fusion
    print(f"  Training early fusion...")
    set_seed(seed)
    save_p = f"{SAVE_DIR}/seed_{seed}_early_fusion.pt"
    ef     = EarlyFusion(mm_input_dim, num_classes_mm).to(DEVICE)
    opt_ef = torch.optim.Adam(ef.parameters(), lr=LR, weight_decay=L2_DECAY)
    ef     = train_loop(ef, opt_ef, criterion, mm_train_ld, mm_val_ld, save_p)
    y_prob_ef, y_true_ef = evaluate(ef, mm_test_ld)
    m = get_metrics(y_prob_ef, y_true_ef, THR)
    print(f"  Early fusion: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "Early fusion", **m})
    os.remove(save_p)

    # MoE
    print(f"  Training MoE...")
    set_seed(seed)
    save_p  = f"{SAVE_DIR}/seed_{seed}_moe.pt"
    moe     = MoE(mm_input_dim, 4, num_classes_mm).to(DEVICE)
    opt_moe = torch.optim.Adam(moe.parameters(), lr=LR, weight_decay=L2_DECAY)
    moe     = train_loop(moe, opt_moe, criterion, mm_train_ld, mm_val_ld, save_p)
    y_prob_moe, y_true_moe = evaluate(moe, mm_test_ld)
    m = get_metrics(y_prob_moe, y_true_moe, THR)
    print(f"  MoE: GAP@20={m['GAP@20']:.4f}  micro_f1={m['micro_f1']:.4f}  macro_f1={m['macro_f1']:.4f}")
    all_results.append({"seed": seed, "model": "MoE (4 experts)", **m})
    os.remove(save_p)


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
    print(f"  GAP@20   = {row['GAP20_mean']:.4f} ± {row['GAP20_std']:.4f}")
    print(f"  Micro F1 = {row['microF1_mean']:.4f} ± {row['microF1_std']:.4f}")
    print(f"  Macro F1 = {row['macroF1_mean']:.4f} ± {row['macroF1_std']:.4f}")

summary_path = OUT_CSV.replace(".csv", "_summary.csv")
summary.to_csv(summary_path, index=False)
print(f"\nSaved: {summary_path}")