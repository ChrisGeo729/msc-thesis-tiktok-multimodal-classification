import pandas as pd

DATA_PATH = '/home/cgeorghiou/projects/msc-thesis/data/Youtube/yt8m_merged.csv'
OUT_DIR   = '/home/cgeorghiou/projects/msc-thesis/data/Youtube'

print('Loading')
df = pd.read_csv(DATA_PATH, usecols=['yt8m_id', 'YouTube_id', 'label_names_json', 
                                      'title', 'tags_json', 'status'],
                 engine='python')
df = df[(df['status'] == 'ok') & (df['label_names_json'].notna())].reset_index(drop=True)
print(f'Rows: {len(df):,}')
df.to_parquet(f'{OUT_DIR}/yt8m_metadata.parquet', index=False)
print('Saved')