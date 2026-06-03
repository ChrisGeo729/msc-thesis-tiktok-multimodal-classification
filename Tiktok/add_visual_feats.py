import pandas as pd


df_main = pd.read_csv('/home/cgeorghiou/projects/msc-thesis/data/TikTok/query_english_captions.csv')
df_features = pd.read_csv('/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_visual_features.csv')

# merge on VideoId
df_merged = df_main.merge(
    df_features[['VideoId', 'rgb_l2', 'rgb_mean', 'rgb_std', 'embedding']],
    on='VideoId',
    how='left'
)

# coverage
total = len(df_merged)
matched = df_merged['rgb_l2'].notna().sum()
print(f"Total rows:     {total}")
print(f"With features:  {matched} ({matched/total*100:.1f}%)")
print(f"Missing:        {total - matched}")

df_merged.to_csv('/home/cgeorghiou/projects/msc-thesis/data/TikTok/tiktok_with_visual_features.csv', index=False)
print("Saved → tiktok_with_visual_features.csv")