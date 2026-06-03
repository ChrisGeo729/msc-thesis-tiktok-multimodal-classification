import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score

def global_average_precision(y_true, y_prob, top_k=20):
    precisions = []
    for i in range(len(y_true)):
        top_idx = np.argsort(y_prob[i])[::-1][:top_k]
        n_correct = 0
        for rank, idx in enumerate(top_idx, 1):
            if y_true[i, idx] == 1:
                n_correct += 1
                precisions.append(n_correct / rank)
    return np.mean(precisions) if precisions else 0.0

prob = np.load("/scratch-shared/cgeorghiou/preds_text_prob.npy")
true = np.load("/scratch-shared/cgeorghiou/preds_text_true.npy")

print(f"GAP@20={global_average_precision(true, prob, 20):.4f}")
print(f"GAP@5={global_average_precision(true, prob, 5):.4f}")

for thr in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]:
    pred = (prob >= thr).astype(int)
    p = precision_score(true, pred, average="micro", zero_division=0)
    r = recall_score(true, pred, average="micro", zero_division=0)
    mif1 = f1_score(true, pred, average="micro", zero_division=0)
    maf1 = f1_score(true, pred, average="macro", zero_division=0)
    print(f"thr={thr:.1f}  P={p:.4f}  R={r:.4f}  Micro F1={mif1:.4f}  Macro F1={maf1:.4f}")