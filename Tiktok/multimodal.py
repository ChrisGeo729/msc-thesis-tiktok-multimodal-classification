import json
import re
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import f1_score, precision_score, recall_score
import warnings
warnings.filterwarnings("ignore")

DATA_PATH   = "/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_with_visual_features.csv"
TEXT_FEAT   = "/home/cgeorghiou/projects/msc-thesis/Tiktok/textcnn_descriptions_features.npy"
TEXT_IDS    = "/home/cgeorghiou/projects/msc-thesis/Tiktok/textcnn_descriptions_videoids.npy"
VISUAL_FEAT = "/home/cgeorghiou/projects/msc-thesis/Tiktok/visual_deep_tiktok_features.npy"
VISUAL_IDS  = "/home/cgeorghiou/projects/msc-thesis/Tiktok/visual_deep_tiktok_videoids.npy"
SAVE_DIR    = "/home/cgeorghiou/projects/msc-thesis/Tiktok"

TEXT_DIM    = 1024
VISUAL_DIM  = 1024
NUM_EXPERTS = 4
DROPOUT     = 0.6
BATCH_SIZE  = 512
EPOCHS      = 5
LR          = 1e-3
L2_DECAY    = 1e-7
PATIENCE    = 3
DEVICE      = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {DEVICE}")

print("Loading features...")
text_feats   = np.load(TEXT_FEAT).astype(np.float32)
text_ids     = np.load(TEXT_IDS,   allow_pickle=True)
visual_feats = np.load(VISUAL_FEAT).astype(np.float32)
visual_ids   = np.load(VISUAL_IDS, allow_pickle=True)
print(f"Text features:   {text_feats.shape}")
print(f"Visual features: {visual_feats.shape}")

text_lookup   = {vid: feat for vid, feat in zip(text_ids,   text_feats)}
visual_lookup = {vid: feat for vid, feat in zip(visual_ids, visual_feats)}
common_ids    = sorted(set(text_lookup.keys()) & set(visual_lookup.keys()))
print(f"Videos in intersection: {len(common_ids):,}")

df = pd.read_csv(DATA_PATH, low_memory=False)
df["label_list"] = df["Labels"].apply(
    lambda s: re.findall(r'"([^"]+)"', s) if isinstance(s, str) else []
)
df = df[df["label_list"].apply(len) > 0].copy()
df = df[df["VideoId"].isin(common_ids)].drop_duplicates(
    subset="VideoId", keep="first").reset_index(drop=True)
print(f"Videos with labels in intersection: {len(df):,}")

X_text   = np.array([text_lookup[vid]   for vid in df["VideoId"]], dtype=np.float32)
X_visual = np.array([visual_lookup[vid] for vid in df["VideoId"]], dtype=np.float32)

mlb = MultiLabelBinarizer()
Y_all = mlb.fit_transform(df["label_list"].values)
num_classes = Y_all.shape[1]
print(f"Label space: {num_classes}")

indices                  = np.arange(len(df))
idx_train, idx_test      = train_test_split(indices,   test_size=0.2, random_state=42)
idx_tr, idx_vl           = train_test_split(idx_train, test_size=0.1, random_state=42)
Y_tr, Y_vl, Y_test       = Y_all[idx_tr], Y_all[idx_vl], Y_all[idx_test]
print(f"Train: {len(idx_tr):,}  Val: {len(idx_vl):,}  Test: {len(idx_test):,}")


class FusedDataset(Dataset):
    def __init__(self, text, visual, labels):
        self.text   = torch.tensor(text,   dtype=torch.float32)
        self.visual = torch.tensor(visual, dtype=torch.float32)
        self.labels = torch.tensor(labels, dtype=torch.float32)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, i):
        return torch.cat([self.text[i], self.visual[i]]), self.labels[i]


def make_loader(text, visual, labels, shuffle):
    return DataLoader(FusedDataset(text, visual, labels), batch_size=BATCH_SIZE, shuffle=shuffle)

train_loader = make_loader(X_text[idx_tr],   X_visual[idx_tr],   Y_tr,   shuffle=True)
val_loader   = make_loader(X_text[idx_vl],   X_visual[idx_vl],   Y_vl,   shuffle=False)
test_loader  = make_loader(X_text[idx_test], X_visual[idx_test], Y_test, shuffle=False)


def global_average_precision(y_true, y_prob, top_k=20):
    precisions = []
    for i in range(y_true.shape[0]):
        hits = 0
        for rank, idx in enumerate(np.argsort(y_prob[i])[::-1][:top_k], 1):
            if y_true[i, idx] == 1:
                hits += 1
                precisions.append(hits / rank)
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


criterion = nn.BCEWithLogitsLoss()


class EarlyFusion(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 1024),
            nn.BatchNorm1d(1024),
            nn.ReLU(),
            nn.Dropout(DROPOUT),
            nn.Linear(1024, num_classes),
        )

    def forward(self, x):
        return self.net(x)


class MoE(nn.Module):
    def __init__(self, input_dim, num_experts, num_classes):
        super().__init__()
        self.experts = nn.ModuleList([
            nn.Sequential(
                nn.Linear(input_dim, 512),
                nn.ReLU(),
                nn.Dropout(DROPOUT),
                nn.Linear(512, num_classes),
            ) for _ in range(num_experts)
        ])
        self.gate = nn.Linear(input_dim, num_experts)

    def forward(self, x):
        gate_weights = torch.softmax(self.gate(x), dim=-1)
        expert_out   = torch.stack([e(x) for e in self.experts], dim=1)
        return (gate_weights.unsqueeze(-1) * expert_out).sum(dim=1)


input_dim   = TEXT_DIM + VISUAL_DIM
early_model = EarlyFusion(input_dim).to(DEVICE)
optimizer   = torch.optim.Adam(early_model.parameters(), lr=LR, weight_decay=L2_DECAY)

print("\nTraining early fusion...")
early_model = train_loop(early_model, optimizer, criterion, train_loader, val_loader,
                         f"{SAVE_DIR}/multimodal_early_fusion.pt")
Y_prob, Y_true = evaluate(early_model, test_loader)
print_metrics(Y_prob, Y_true, "Early fusion (text + visual)")
np.save(f"{SAVE_DIR}/preds_multimodal_early_prob.npy",    Y_prob)
np.save(f"{SAVE_DIR}/preds_multimodal_early_true.npy",    Y_true)
np.save(f"{SAVE_DIR}/preds_multimodal_early_classes.npy", mlb.classes_)

moe_model = MoE(input_dim, NUM_EXPERTS, num_classes).to(DEVICE)
optimizer = torch.optim.Adam(moe_model.parameters(), lr=LR, weight_decay=L2_DECAY)

print("\nTraining MoE (4 experts)...")
moe_model = train_loop(moe_model, optimizer, criterion, train_loader, val_loader,
                       f"{SAVE_DIR}/multimodal_moe.pt")
Y_prob, Y_true = evaluate(moe_model, test_loader)
print_metrics(Y_prob, Y_true, "MoE fusion (text + visual, 4 experts)")
np.save(f"{SAVE_DIR}/preds_multimodal_moe_prob.npy",    Y_prob)
np.save(f"{SAVE_DIR}/preds_multimodal_moe_true.npy",    Y_true)
np.save(f"{SAVE_DIR}/preds_multimodal_moe_classes.npy", mlb.classes_)