import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from sklearn.metrics import f1_score, precision_score, recall_score
import warnings
warnings.filterwarnings("ignore")

SAVE_DIR  = "/home/cgeorghiou/projects/msc-thesis/Tiktok"
OUT_DIR   = "/home/cgeorghiou/projects/msc-thesis/Tiktok/analysis"
THR       = 0.3
TOP_N     = 20

import os
os.makedirs(OUT_DIR, exist_ok=True)

models = {
    "Text (desc + hashtags)": {
        "prob":    np.load(f"{SAVE_DIR}/preds_text_prob.npy"),
        "true":    np.load(f"{SAVE_DIR}/preds_text_true.npy"),
        "classes": np.load(f"{SAVE_DIR}/preds_text_classes.npy", allow_pickle=True),
    },
    "Visual (OpenCLIP)": {
        "prob":    np.load(f"{SAVE_DIR}/preds_visual_prob.npy"),
        "true":    np.load(f"{SAVE_DIR}/preds_visual_true.npy"),
        "classes": np.load(f"{SAVE_DIR}/preds_visual_classes.npy", allow_pickle=True),
    },
    "Multimodal (early fusion)": {
        "prob":    np.load(f"{SAVE_DIR}/preds_multimodal_prob.npy"),
        "true":    np.load(f"{SAVE_DIR}/preds_multimodal_true.npy"),
        "classes": np.load(f"{SAVE_DIR}/preds_multimodal_classes.npy", allow_pickle=True),
    },
}


def per_class_metrics(prob, true, classes, thr):
    pred = (prob >= thr).astype(int)
    rows = []
    for i, cls in enumerate(classes):
        support = int(true[:, i].sum())
        if support == 0:
            continue
        p  = precision_score(true[:, i], pred[:, i], zero_division=0)
        r  = recall_score(true[:, i], pred[:, i], zero_division=0)
        f  = f1_score(true[:, i], pred[:, i], zero_division=0)
        fp = int(((pred[:, i] == 1) & (true[:, i] == 0)).sum())
        fn = int(((pred[:, i] == 0) & (true[:, i] == 1)).sum())
        rows.append({"class": cls, "support": support, "precision": p,
                     "recall": r, "f1": f, "fp": fp, "fn": fn})
    return pd.DataFrame(rows).sort_values("support", ascending=False).reset_index(drop=True)

for name, m in models.items():
    m["metrics"] = per_class_metrics(m["prob"], m["true"], m["classes"], THR)
    print(f"\n{name} — {len(m['metrics'])} classes with support")
    print(m["metrics"].head(10).to_string(index=False))

colors = {"Text (desc + hashtags)": "#2196F3",
          "Visual (OpenCLIP)": "#FF5722",
          "Multimodal (early fusion)": "#4CAF50"}

fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=False)
for ax, (name, m) in zip(axes, models.items()):
    df = m["metrics"]
    ax.scatter(df["support"], df["f1"], alpha=0.6, s=40,
               color=colors[name], edgecolors="none")
    for _, row in df.head(5).iterrows():
        ax.annotate(row["class"], (row["support"], row["f1"]),
                    fontsize=7, alpha=0.8,
                    xytext=(4, 2), textcoords="offset points")
    ax.set_xlabel("Training support (# positive examples)", fontsize=11)
    ax.set_ylabel("F1 score", fontsize=11)
    ax.set_title(name, fontsize=12, fontweight="bold")
    ax.set_ylim(-0.05, 1.05)
    ax.axhline(df["f1"].mean(), color="gray", linestyle="--",
               linewidth=0.8, label=f"Mean F1={df['f1'].mean():.2f}")
    ax.legend(fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

fig.suptitle("Label frequency vs F1 score per class (thr=0.3)", fontsize=14, y=1.02)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/freq_vs_f1_scatter.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: freq_vs_f1_scatter.png")

BINS       = [0, 50, 100, 500, float("inf")]
BIN_LABELS = ["<50", "50-100", "100-500", ">500"]

bin_rows = []
for name, m in models.items():
    df = m["metrics"].copy()
    df["bin"] = pd.cut(df["support"], bins=BINS, labels=BIN_LABELS, right=False)
    for b in BIN_LABELS:
        subset = df[df["bin"] == b]
        bin_rows.append({
            "model": name, "bin": b,
            "mean_f1":   subset["f1"].mean() if len(subset) else 0,
            "std_f1":    subset["f1"].std()  if len(subset) > 1 else 0,
            "n_classes": len(subset),
        })

bin_df = pd.DataFrame(bin_rows)
print("\nFrequency-binned F1:")
print(bin_df.to_string(index=False))
bin_df.to_csv(f"{OUT_DIR}/frequency_bin_f1.csv", index=False)

fig, ax = plt.subplots(figsize=(10, 5))
model_names = list(models.keys())
x     = np.arange(len(BIN_LABELS))
width = 0.25

for i, mname in enumerate(model_names):
    sub = bin_df[bin_df["model"] == mname]
    ax.bar(x + i * width, sub["mean_f1"], width, yerr=sub["std_f1"],
           label=mname, color=list(colors.values())[i], alpha=0.85,
           capsize=3, error_kw={"linewidth": 0.8})

ax.set_xticks(x + width)
ax.set_xticklabels(BIN_LABELS)
ax.set_xlabel("Class support (# positive examples in test set)", fontsize=11)
ax.set_ylabel("Mean per-class F1", fontsize=11)
ax.set_title("Mean F1 by label frequency bin (thr=0.3)", fontsize=12)
ax.set_ylim(0, 1.05)
ax.legend(fontsize=9)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/frequency_bin_f1.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: frequency_bin_f1.png")

fig, axes = plt.subplots(3, 1, figsize=(16, 18))
for ax, (name, m) in zip(axes, models.items()):
    df = m["metrics"].head(30)
    ax.bar(range(len(df)), df["f1"], color=colors[name], alpha=0.8, edgecolor="none")
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df["class"], rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("F1 score", fontsize=10)
    ax.set_title(f"{name} — top 30 classes by support", fontsize=11, fontweight="bold")
    ax.set_ylim(0, 1.05)
    ax.axhline(df["f1"].mean(), color="gray", linestyle="--",
               linewidth=0.8, label=f"Mean={df['f1'].mean():.2f}")
    ax.legend(fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for i, (_, row) in enumerate(df.iterrows()):
        ax.text(i, row["f1"] + 0.01, str(row["support"]),
                ha="center", va="bottom", fontsize=6, color="gray")

plt.tight_layout()
plt.savefig(f"{OUT_DIR}/per_class_f1_bars.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: per_class_f1_bars.png")


def co_occurrence_heatmap(prob, true, classes, thr, top_n, title, ax):
    pred    = (prob >= thr).astype(int)
    support = true.sum(axis=0)
    top_idx = np.argsort(support)[::-1][:top_n]
    top_classes = classes[top_idx]

    matrix = np.zeros((top_n, top_n), dtype=int)
    for i in range(top_n):
        for j in range(top_n):
            matrix[i, j] = int(((pred[:, top_idx[i]] == 1) & (true[:, top_idx[j]] == 1)).sum())

    col_sums = matrix.sum(axis=0, keepdims=True)
    col_sums[col_sums == 0] = 1
    matrix_norm = matrix / col_sums

    im = ax.imshow(matrix_norm, cmap="Blues", aspect="auto", vmin=0, vmax=1)
    ax.set_xticks(range(top_n))
    ax.set_yticks(range(top_n))
    ax.set_xticklabels(top_classes, rotation=45, ha="right", fontsize=7)
    ax.set_yticklabels(top_classes, fontsize=7)
    ax.set_xlabel("True label", fontsize=9)
    ax.set_ylabel("Predicted label", fontsize=9)
    ax.set_title(title, fontsize=10, fontweight="bold")
    return im

fig, axes = plt.subplots(1, 3, figsize=(24, 8))
for ax, (name, m) in zip(axes, models.items()):
    im = co_occurrence_heatmap(
        m["prob"], m["true"], m["classes"], THR, TOP_N, name, ax
    )
    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Prediction rate")

fig.suptitle(f"Co-occurrence heatmap — top {TOP_N} classes by support (thr={THR})",
             fontsize=13, y=1.02)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/cooccurrence_heatmap.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: cooccurrence_heatmap.png")

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
for ax, (name, m) in zip(axes, models.items()):
    df = m["metrics"].head(20)
    x  = range(len(df))
    ax.barh(x, df["fp"], color="#EF5350", alpha=0.8, label="False Positives")
    ax.barh(x, [-v for v in df["fn"]], color="#42A5F5", alpha=0.8, label="False Negatives")
    ax.set_yticks(list(x))
    ax.set_yticklabels(df["class"], fontsize=8)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Count", fontsize=10)
    ax.set_title(f"{name}\nFP / FN top 20 classes", fontsize=10, fontweight="bold")
    ax.legend(fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

plt.tight_layout()
plt.savefig(f"{OUT_DIR}/fp_fn_analysis.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: fp_fn_analysis.png")

fig, axes = plt.subplots(1, 3, figsize=(18, 5))
for ax, (name, m) in zip(axes, models.items()):
    prob, true = m["prob"], m["true"]
    pos_probs  = prob[true == 1]
    neg_probs  = prob[true == 0]
    rng = np.random.RandomState(42)
    if len(neg_probs) > 500_000:
        neg_probs = rng.choice(neg_probs, 500_000, replace=False)

    ax.hist(neg_probs, bins=100, alpha=0.6, color="#90A4AE", label="Negative", density=True)
    ax.hist(pos_probs, bins=100, alpha=0.7, color=colors[name], label="Positive", density=True)
    ax.set_xlabel("Predicted probability", fontsize=11)
    ax.set_ylabel("Density", fontsize=11)
    ax.set_title(name, fontsize=12)
    ax.axvline(THR, color="black", linestyle="--", linewidth=0.8, label=f"thr={THR}")
    ax.legend(fontsize=9)
    ax.set_xlim(-0.02, 1.02)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

fig.suptitle("Predicted probability distribution: positive vs negative labels",
             fontsize=13, y=1.02)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/output_distribution.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: output_distribution.png")

print("\nOutput distribution summary:")
for name, m in models.items():
    prob, true = m["prob"], m["true"]
    pos = prob[true == 1]
    neg = prob[true == 0]
    print(f"  {name}:")
    print(f"    Positive — mean={pos.mean():.4f}  median={np.median(pos):.4f}  std={pos.std():.4f}")
    print(f"    Negative — mean={neg.mean():.4f}  median={np.median(neg):.4f}  std={neg.std():.4f}")

for name, m in models.items():
    fname = name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("+", "")
    m["metrics"].to_csv(f"{OUT_DIR}/per_class_metrics_{fname}.csv", index=False)
    print(f"Saved: per_class_metrics_{fname}.csv")

print(f"\nSaved to {OUT_DIR}")