# MSc Thesis — Multimodal Video Content Classification

**Author:** Christos Georghiou
**Student ID:** 16072766
**Email:** christos.georghiou@student.uva.nl

---

## Overview

This project investigates multi-label video content classification using two datasets:

- **TKGO (TikTok Global Observatory)** — short-form TikTok videos with spoken captions (via native subtitles or Whisper transcription), thumbnail images (OpenCLIP visual features), and engagement metadata. Accessed via Metabase in collaboration with AI Forensics.
- **YouTube-8M-Text** — large-scale YouTube videos with pre-extracted audio-visual features and textual metadata (titles and tags). Built on the YouTube-8M dataset extended with text features from the YouTube Data API.

The thesis benchmarks a full model progression — TF-IDF baselines, deep text (TextCNN + Word2Vec), deep visual, and multimodal fusion (early fusion and Mixture-of-Experts) — across both datasets, with multi-seed evaluation for robustness.

---

## Repository Structure

```
.
├── data/
│   ├── TikTok/
│   │   ├── query_result_*.csv              # Raw TKGO export from Metabase
│   │   ├── videos_unique.csv               # Deduplicated videos
│   │   ├── query_with_captions_filled.csv  # Enriched with transcripts
│   │   ├── query_english_captions.csv      # English-only filtered subset
│   │   ├── tiktok_visual_features.csv      # OpenCLIP ViT-L-14 (768-d) embeddings per video
│   │   ├── tiktok_with_visual_features.csv # Main model-ready dataset (text + visual merged)
│   │   ├── thumbnails_map.csv              # Thumbnail download log
│   │   ├── tiktok_thumbnails/              # Downloaded thumbnail images
│   │   └── tiktok_transcripts_out/         # Per-video VTT transcript files
│   ├── Youtube/
│   │   ├── label_names.csv                 # YT8M label ID → name mapping
│   │   ├── video_ids_all_train.txt         # Video IDs extracted from TFRecords
│   │   ├── yt8m_id_to_youtube_id.csv       # YT8M internal ID → YouTube ID mapping
│   │   ├── youtube8m_text.csv              # Titles, tags, channel from YouTube API
│   │   ├── yt8m_merged.csv                 # TFRecord features + text, merged
│   │   ├── yt8m_metadata.parquet           # Optimised metadata subset (fast loading)
│   │   ├── yt8m_rgb_features.npy           # mean_rgb (1024-d) as a numpy array
│   │   ├── yt8m_text_clean_baseline.csv    # Preprocessed, model-ready text
│   │   ├── yt8m_eda_sample.csv             # Sample for EDA
│   │   └── yt8m/video_level/               # Raw TFRecord files
│   └── word2vec/
│       └── GoogleNews-vectors-negative300.bin   # Pre-trained Word2Vec (required for deep models)
├── Tiktok/
│   ├── deduplicate_videos.py       # Deduplicate raw Metabase export by VideoId
│   ├── transcript_collection.py    # Download transcripts (subtitles + Whisper fallback)
│   ├── tiktok_transcriptions.py    # Helper functions for transcript extraction
│   ├── extract_subitle.sh          # One-off subtitle extraction shell script
│   ├── thumbnail_collection.py     # Download thumbnails via yt-dlp
│   ├── dataset_with_captions.py    # Merge VTT transcripts into the dataset CSV
│   ├── filter_english_captions.py  # Filter to English captions/descriptions via langdetect
│   ├── thumbnail_features.py       # Extract OpenCLIP ViT-L-14 (768-d) thumbnail embeddings
│   ├── add_visual_feats.py         # Merge visual features into the main dataset CSV
│   ├── text_baseline.py            # TF-IDF + OvR LR baselines (captions / descriptions / both)
│   ├── tkgo_visual_baseline.py     # OpenCLIP embeddings + OvR SGD visual baseline
│   ├── deep_text.py                # TextCNN (Word2Vec 300-d, kernels 1–8) + TF-IDF hashtag fusion
│   ├── deep_visual.py              # MLP visual head (768→1024-d) on OpenCLIP embeddings
│   ├── multimodal.py               # Early fusion and MoE (4 experts) over text + visual features
│   ├── multi_seed.py               # Multi-seed (42/123/456) sweep over all deep models
│   ├── error_analysis.py           # Per-class F1, scatter plots, co-occurrence heatmaps, FP/FN
│   ├── frequency_dist.py           # Label frequency distribution plots for both datasets
│   └── debug.py                    # Ad-hoc debugging utility
├── YouTube/
│   ├── extract_video_ids.py        # Extract video IDs from TFRecord files
│   ├── convert_yt8m_ids.py         # Map YT8M internal IDs to YouTube IDs (32 workers)
│   ├── download_yt8m_text.py       # Fetch titles/tags from YouTube Data API v3
│   ├── merged_yt8m_text.py         # Merge TFRecord features with text metadata
│   ├── yt8m_to_csv.py              # Export TFRecord sample to CSV for EDA
│   ├── optimise_data.py            # Convert yt8m_merged.csv → yt8m_metadata.parquet
│   ├── read_labels.py              # Inspect label_names.csv
│   ├── text_baseline.py            # TF-IDF + OvR SGD baselines (tags / titles+tags)
│   ├── visual_baseline.py          # RGB mean features (1024-d) + OvR SGD visual baseline
│   ├── deep_text.py                # TextCNN (Word2Vec 300-d) on titles + TF-IDF tag fusion
│   ├── deep_visual.py              # MLP visual head (1024→4096-d) on mean_rgb embeddings
│   ├── multimodal.py               # Early fusion and MoE (4 experts) over text + visual features
│   ├── multi_seed.py               # Multi-seed (42/123/456) sweep over all deep models
│   ├── error_analysis.py           # Per-class F1, scatter plots, co-occurrence heatmaps, FP/FN
│   ├── extract_tags.py             # Re-extract tag features from a saved TextCNN checkpoint
│   └── recompute_preds.py          # Recompute metrics from saved .npy prediction files
└── notebooks/
    ├── EDA.ipynb                   # Full EDA: both datasets + TF-IDF baselines
    ├── exploration_TKGO.ipynb      # Exploratory analysis of TKGO
    ├── exploration_yt8m-t.ipynb    # Exploratory analysis of YouTube-8M-Text
    ├── preprocessing.ipynb         # Text preprocessing pipeline for YT8M
    └── baseline_models.ipynb       # TF-IDF + Logistic Regression baselines
```

> All files matched by `.gitignore` (CSVs, TFRecords, VTT files, thumbnails, `.npy` arrays) are excluded from version control due to size or licensing constraints.

---

## Datasets

### TKGO

The raw Metabase export contains roughly 5,000,000 country-level recommendation records (the same video is served to multiple country feeds). Deduplicating by `VideoId` reduces this to 191,100 unique videos, of which 74,918 (39.2%) carry at least one label. Captions were enriched using a two-strategy pipeline: native TikTok `subtitleInfos` where available, falling back to OpenAI Whisper transcription. The text models then apply an English-language filter (`langdetect`), deduplication, and a minimum-length requirement.

| Property | Value |
|---|---|
| Raw recommendation records | ~5,000,000 |
| Unique videos (dedup by VideoId) | 191,100 |
| Labeled videos | 74,918 (39.2%) |
| Labels | 101 |
| English-filtered description population | ~51,600 |
| Videos with valid visual embeddings | 55,557 |
| Text + visual intersection (multimodal) | 35,418 |
| Text features | Descriptions + hashtags (primary); captions |
| Visual features | OpenCLIP ViT-L-14 (768-d) from thumbnails |

Descriptions are the primary text source for all models; captions carry a weaker signal due to low English coverage and are reported for comparison only.

### YouTube-8M-Text

Built by extracting video IDs from YT8M TFRecords, mapping them to YouTube IDs, then fetching metadata via the YouTube Data API v3. For the experiments, 1,000,000 videos are sampled uniformly at random (`random_state=42`) from the full corpus, retaining 3,791 labels. Audio embeddings are available but excluded to keep a consistent bimodal (text–visual) setup across datasets.

| Property | Value |
|---|---|
| Sampled videos (experiments) | 1,000,000 |
| Labels retained | 3,791 |
| Text features | Titles + tags |
| Visual features | `mean_rgb` (1024-d) from Inception-v3, aggregated |

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas scikit-learn numpy matplotlib seaborn \
            tensorflow requests yt-dlp openai-whisper \
            browser-cookie3 google-api-python-client \
            torch gensim open-clip-torch wordsegment langdetect \
            pyarrow fastparquet
```

Set the YouTube API key before running any data-collection scripts:

```bash
export YOUTUBE_API_KEY=your_key_here
```

Download Google News Word2Vec vectors (required for `deep_text.py` and `multi_seed.py`) and place them at `data/word2vec/GoogleNews-vectors-negative300.bin`.

> The deep model and multi-seed scripts are designed to run on a GPU server and contain hardcoded paths pointing to an HPC cluster (`/home/cgeorghiou/projects/msc-thesis/` and `/scratch-shared/cgeorghiou/`). Update `DATA_PATH`, `SAVE_DIR`, `SCRATCH_DIR`, etc. at the top of each script before running locally.

---

## Data Pipeline

### TikTok

```
Metabase export (query_result_*.csv)
    → deduplicate_videos.py         # → data/TikTok/videos_unique.csv
    → transcript_collection.py      # → data/TikTok/tiktok_transcripts_out/
    → thumbnail_collection.py       # → data/TikTok/tiktok_thumbnails/
    → dataset_with_captions.py      # → data/TikTok/query_with_captions_filled.csv
    → filter_english_captions.py    # → data/TikTok/query_english_captions.csv
    → thumbnail_features.py         # → data/TikTok/tiktok_visual_features.csv  (OpenCLIP ViT-L-14)
    → add_visual_feats.py           # → data/TikTok/tiktok_with_visual_features.csv
```

### YouTube-8M

```
TFRecord files (data/Youtube/yt8m/video_level/)
    → extract_video_ids.py          # → data/Youtube/video_ids_all_train.txt
    → convert_yt8m_ids.py           # → data/Youtube/yt8m_id_to_youtube_id.csv
    → download_yt8m_text.py         # → data/Youtube/youtube8m_text.csv  (requires YOUTUBE_API_KEY)
    → merged_yt8m_text.py           # → data/Youtube/yt8m_merged.csv
    → optimise_data.py              # → data/Youtube/yt8m_metadata.parquet  (+yt8m_rgb_features.npy)
```

---

## Models

All deep models share a consistent training setup: 80/20 train/test split, 10% validation hold-out from training data, BCEWithLogitsLoss, Adam optimiser, early stopping (patience=3), and metrics reported at thresholds 0.2–0.7 plus GAP@5 and GAP@20. Trained predictions are saved as `.npy` files and consumed by the corresponding `error_analysis.py` scripts.

### TikTok (TKGO)

| Script | Model | Input |
|---|---|---|
| `text_baseline.py` | TF-IDF + OvR Logistic Regression | Captions / Descriptions / Both |
| `tkgo_visual_baseline.py` | OvR SGD | OpenCLIP embeddings (768-d) |
| `deep_text.py` | TextCNN (Word2Vec 300-d, kernels 1–8) + TF-IDF hashtag fusion | Descriptions + hashtags |
| `deep_visual.py` | MLP visual head (768→1024 hidden, BN+ReLU+Dropout) | OpenCLIP embeddings (768-d) |
| `multimodal.py` | Early fusion (concat → MLP) and MoE (4 experts, softmax gate) | TextCNN + Visual head features |
| `multi_seed.py` | Full pipeline sweep (visual head → TextCNN → text fusion → early fusion → MoE) | As above, seeds 42/123/456 |
| `error_analysis.py` | Post-hoc per-class diagnostics (scatter, bar charts, heatmaps, FP/FN) | Saved `.npy` predictions |

### YouTube-8M-Text (YT8M)

| Script | Model | Input |
|---|---|---|
| `text_baseline.py` | TF-IDF + OvR SGD | Tags only / Titles+Tags |
| `visual_baseline.py` | OvR SGD | mean_rgb (1024-d) |
| `deep_text.py` | TextCNN (Word2Vec 300-d, kernels 1–8) + TF-IDF tag fusion | Titles + tags |
| `deep_visual.py` | MLP visual head (1024→4096 hidden, BN+ReLU+Dropout) | mean_rgb (1024-d) |
| `multimodal.py` | Early fusion and MoE (4 experts) | TextCNN + Visual head features |
| `multi_seed.py` | Full pipeline sweep | As above, seeds 42/123/456 |
| `error_analysis.py` | Post-hoc per-class diagnostics | Saved `.npy` predictions |

Multi-seed summary statistics (mean ± std across 3 seeds) are written to:
- `Tiktok/multi_seed_results.csv` / `Tiktok/multi_seed_results_summary.csv`
- `YouTube/multi_seed_results_yt8m.csv` / `YouTube/multi_seed_results_yt8m_summary.csv`

---

## Notebooks

| Notebook | Description |
|---|---|
| `EDA.ipynb` | Full EDA of both datasets, caption enrichment impact, and TF-IDF baselines with a final comparison table |
| `exploration_TKGO.ipynb` | Deep-dive into TKGO: label distribution, caption coverage, text statistics |
| `exploration_yt8m-t.ipynb` | Deep-dive into YouTube-8M-Text: label distribution, text feature coverage |
| `preprocessing.ipynb` | Text cleaning pipeline for YT8M (lowercase, remove URLs/punctuation, combine title+tags) |
| `baseline_models.ipynb` | TF-IDF + OneVsRest Logistic Regression on YT8M; threshold tuning |

All notebooks use paths relative to the `notebooks/` directory (e.g. `../data/TikTok/...`).

---

## Results

Primary metric is GAP@20. F1 scores are micro/macro at their optimal threshold. Deep and fusion rows are means over three seeds (42, 123, 456).

### TKGO — text-only (English-filtered, deduplicated)

| Model | GAP@20 | Micro-F1 | Macro-F1 |
|---|---|---|---|
| TF-IDF captions | 0.609 | 0.407 | 0.320 |
| TF-IDF descriptions | 0.704 | 0.530 | 0.439 |
| TF-IDF combined | 0.683 | 0.514 | 0.457 |
| TextCNN desc | 0.658 | 0.445 | 0.158 |
| TextCNN desc+hash | 0.697 | 0.544 | 0.290 |

### YouTube-8M-T — text-only

| Model | GAP@20 | Micro-F1 | Macro-F1 |
|---|---|---|---|
| TF-IDF tags | 0.720 | 0.400 | 0.015 |
| TF-IDF titles+tags | 0.716 | 0.397 | 0.014 |
| TextCNN titles | 0.803 | 0.593 | 0.329 |
| TextCNN titles+tags | 0.828 | 0.634 | 0.415 |

### Text vs. visual (best unimodal per tier)

| Dataset | Model | GAP@20 | Micro-F1 | Macro-F1 |
|---|---|---|---|---|
| TKGO | TF-IDF desc | 0.704 | 0.530 | 0.439 |
| TKGO | Visual LR | 0.596 | 0.473 | 0.319 |
| TKGO | TextCNN desc+hash | 0.697 | 0.544 | 0.290 |
| TKGO | Visual head | 0.692 | 0.540 | 0.348 |
| YT8M-T | TF-IDF tags | 0.720 | 0.400 | 0.015 |
| YT8M-T | Visual LR | 0.775 | 0.546 | 0.034 |
| YT8M-T | TextCNN titles+tags | 0.828 | 0.634 | 0.415 |
| YT8M-T | Visual head | 0.877 | 0.721 | 0.226 |

### Multimodal fusion vs. best unimodal

| Dataset | Model | GAP@20 | Micro-F1 | Macro-F1 |
|---|---|---|---|---|
| TKGO | Best unimodal | 0.697 | 0.544 | 0.348 |
| TKGO | Early fusion | 0.802 | 0.697 | 0.477 |
| TKGO | MoE (4 exp.) | 0.792 | 0.684 | 0.455 |
| YT8M-T | Best unimodal | 0.877 | 0.721 | 0.226 |
| YT8M-T | Early fusion | 0.914 | 0.791 | 0.435 |
| YT8M-T | MoE (4 exp.) | 0.909 | 0.784 | 0.428 |

On TKGO, text and visual reach parity at both the baseline and deep tiers; on YouTube-8M-T, visual is the stronger modality at every tier. Early fusion improves over every unimodal model on both datasets and outperforms the Mixture of Experts on GAP@20 and micro-F1.

---

## Notes

- `tiktok_transcripts_out/` contains one subdirectory per video, each holding one or more `.vtt` files named `{video_id}_{format}_{language}_{source}.vtt`. Whisper transcripts are identifiable by `_whisper_` in the filename.
- The YouTube-8M TFRecords used here are video-level files (not segment-level). Each record contains a 1024-d `mean_rgb` and 128-d `mean_audio` embedding alongside label IDs.
- The TKGO data is not publicly available and was accessed under a research collaboration agreement with AI Forensics.
- `frequency_dist.py` generates a single figure (`label_frequency_distribution.png`) comparing label-frequency distributions across both datasets side-by-side.
- The English-language population depends on `langdetect`, which is nondeterministic unless seeded. Scripts set `DetectorFactory.seed = 0` for reproducibility; the reported description population is approximately 51,600 videos.