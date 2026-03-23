import pandas as pd

from pathlib import Path
label_df = pd.read_csv(Path(__file__).resolve().parent.parent / "data" / "Youtube" / "label_names.csv")

print("Shape:", label_df.shape)
print("\nColumns:", label_df.columns.tolist())
print("\nFirst 10 rows:")
print(label_df.head(10))
