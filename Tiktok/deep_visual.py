import json
import re
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics import f1_score, precision_score, recall_score
import warnings
warnings.filterwarnings("ignore")

DATA_PATH = "/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_with_visual_features.csv"
SAVE_PATH = "/home/cgeorghiou/projects/msc-thesis/Tiktok/visual_deep_tiktok.pt"
SAVE_DIR  = "/home/cgeorghiou/projects/msc-thesis/Tiktok"

VISUAL_DIM = 768
HIDDEN_DIM = 1024
DROPOUT    = 0.6
BATCH_SIZE = 512
EPOCHS     = 5
LR         = 1e-3
L2_DECAY   = 1e-7
PATIENCE   = 3
DEVICE     = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device: {DEVICE}")

df = pd.read_csv(DATA_PATH, low_memory=False)
df["label_list"] = df["Labels"].apply(
    lambda s: re.findall(r'"([^"]+)"', s) if isinstance(s, str) else []
)
df = df[df["label_list"].apply(len) > 0].copy()
df = df[df["embedding"].notna()].copy().reset_index(drop=True)
df = df.drop_duplicates(subset="VideoId", keep="first").reset_index(drop=True)
print(f"Videos with visual features + labels: {len(df):,}")

print("Parsing embeddings...")
X_all = np.zeros((len(df), VISUAL_DIM), dtype=np.float32)
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

df = df[valid_mask].reset_index(drop=True)
X_all = X_all[valid_mask]
print(f"Videos after embedding validation: {len(df):,}")

mlb = MultiLabelBinarizer()
Y_all = mlb.fit_transform(df["label_list"].values)
num_classes = Y_all.shape[1]
print(f"Label space: {num_classes}")

indices = np.arange(len(df))
idx_train, idx_test = train_test_split(indices, test_size=0.2, random_state=42)

X_train, X_test = X_all[idx_train], X_all[idx_test]
Y_train, Y_test = Y_all[idx_train], Y_all[idx_test]

X_tr, X_val, Y_tr, Y_val = train_test_split(X_train, Y_train, test_size=0.1, random_state=42)
print(f"Train: {len(X_tr):,}  Val: {len(X_val):,}  Test: {len(X_test):,}")

def make_loader(X, Y, shuffle):
    return DataLoader(
        TensorDataset(torch.tensor(X, dtype=torch.float32), torch.tensor(Y, dtype=torch.float32)),
        batch_size=BATCH_SIZE, shuffle=shuffle
    )

train_loader = make_loader(X_tr,   Y_tr,   shuffle=True)
val_loader   = make_loader(X_val,  Y_val,  shuffle=False)
test_loader  = make_loader(X_test, Y_test, shuffle=False)

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


class VisualHead(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(VISUAL_DIM, HIDDEN_DIM),
            nn.BatchNorm1d(HIDDEN_DIM),
            nn.ReLU(),
            nn.Dropout(DROPOUT),
        )
        self.classifier = nn.Linear(HIDDEN_DIM, num_classes)

    def forward(self, x):
        return self.classifier(self.net(x))

    def extract_features(self, x):
        return self.net(x)


model     = VisualHead().to(DEVICE)
optimizer = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=L2_DECAY)
criterion = nn.BCEWithLogitsLoss()

print("\nTraining visual head...")
model = train_loop(model, optimizer, criterion, train_loader, val_loader, SAVE_PATH)

Y_prob, Y_true = evaluate(model, test_loader)
print_metrics(Y_prob, Y_true, "Visual head (standalone)")
np.save(f"{SAVE_DIR}/preds_visual_prob.npy", Y_prob)
np.save(f"{SAVE_DIR}/preds_visual_true.npy", Y_true)
np.save(f"{SAVE_DIR}/preds_visual_classes.npy", mlb.classes_)

print("\nExtracting visual features...")
all_features = np.zeros((len(df), HIDDEN_DIM), dtype=np.float32)
model.eval()
with torch.no_grad():
    for i in range(0, len(df), BATCH_SIZE):
        batch = torch.tensor(X_all[i:i+BATCH_SIZE], dtype=torch.float32).to(DEVICE)
        all_features[i:i+len(batch)] = model.extract_features(batch).cpu().numpy()

feat_save = SAVE_PATH.replace(".pt", "_features.npy")
vid_save  = SAVE_PATH.replace(".pt", "_videoids.npy")
np.save(feat_save, all_features)
np.save(vid_save, df["VideoId"].values)
print(f"Saved features: {all_features.shape} -> {feat_save}")
print(f"Saved video IDs -> {vid_save}")