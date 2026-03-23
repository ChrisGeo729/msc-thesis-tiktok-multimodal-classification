# MSc Thesis — Multimodal Video Content Classification

**Author:** Christos Georghiou
**Student ID:** 16072766
**Email:** christos.georghiou@student.uva.nl

---

## Overview

This project investigates multi-label video content classification using two datasets:

- **TKGO (TikTok Global Observatory)** — short-form TikTok videos with spoken captions (via native subtitles or Whisper transcription), thumbnail images, and engagement metadata. Accessed via Metabase in collaboration with AI Forensics.
- **YouTube-8M-Text** — large-scale YouTube videos with pre-extracted audio-visual features and textual metadata (titles and tags). Built on the YouTube-8M dataset extended with text features from the YouTube Data API.

The thesis compares text-based classification performance across these two datasets, establishing TF-IDF + Logistic Regression baselines before moving to multimodal approaches.

---

## Repository Structure

```
.
├── data/
│   ├── TikTok/
│   │   ├── query_result_*.csv              # Raw TKGO export from Metabase
│   │   ├── videos_unique.csv               # Deduplicated videos (222,552)
│   │   ├── query_with_captions_filled.csv  # Enriched with transcripts
│   │   ├── thumbnails_map.csv              # Thumbnail download log
│   │   ├── tiktok_thumbnails/              # Downloaded thumbnail images
│   │   └── tiktok_transcripts_out/         # Per-video VTT transcript files
│   └── Youtube/
│       ├── label_names.csv                 # YT8M label ID → name mapping
│       ├── video_ids_all_train.txt         # Video IDs extracted from TFRecords
│       ├── yt8m_id_to_youtube_id.csv       # YT8M internal ID → YouTube ID mapping
│       ├── youtube8m_text.csv              # Titles, tags, channel from YouTube API
│       ├── yt8m_merged.csv                 # TFRecord features + text, merged
│       ├── yt8m_text_clean_baseline.csv    # Preprocessed, model-ready
│       ├── yt8m_eda_sample.csv             # Sample for EDA
│       └── yt8m/video_level/               # Raw TFRecord files
├── Tiktok/
│   ├── transcript_collection.py    # Download transcripts (subtitles + Whisper fallback)
│   ├── thumbnail_collection.py     # Download thumbnails via yt-dlp
│   ├── dataset_with_captions.py    # Merge VTT transcripts into the dataset CSV
│   ├── deduplicate_videos.py       # Deduplicate raw Metabase export by VideoId
│   ├── tiktok_transcriptions.py    # Helper functions for transcript extraction
│   └── extract_subitle.sh          # One-off subtitle extraction shell script
├── YouTube/
│   ├── extract_video_ids.py        # Extract video IDs from TFRecord files
│   ├── convert_yt8m_ids.py         # Map YT8M internal IDs to YouTube IDs
│   ├── download_yt8m_text.py       # Fetch titles/tags from YouTube Data API v3
│   ├── merged_yt8m_text.py         # Merge TFRecord features with text metadata
│   └── yt8m_to_csv.py              # Export TFRecord sample to CSV for EDA
└── notebooks/
    ├── EDA.ipynb                   # Full EDA: both datasets + TF-IDF baselines
    ├── exploration_TKGO.ipynb      # Exploratory analysis of TKGO
    ├── exploration_yt8m-t.ipynb    # Exploratory analysis of YouTube-8M-Text
    ├── preprocessing.ipynb         # Text preprocessing pipeline for YT8M
    └── baseline_models.ipynb       # TF-IDF + Logistic Regression baselines
```

> All files matched by `.gitignore` (CSVs, TFRecords, VTT files, thumbnails) are excluded from version control due to size or licensing constraints.

---

## Datasets

### TKGO

| Property | Value |
|---|---|
| Total rows (raw) | 1,000,000 |
| Unique videos | 222,552 |
| Labels | 101 |
| Caption coverage (after enrichment) | ~15% |
| Text feature | Spoken captions |

The raw Metabase export contains duplicate rows (same video served to multiple For You feeds). After deduplication by `VideoId`, 222,552 unique videos remain. Captions were enriched using a two-strategy pipeline: native TikTok `subtitleInfos` where available, falling back to OpenAI Whisper transcription.

### YouTube-8M-Text

| Property | Value |
|---|---|
| Videos | 225,042 |
| Labels | 3,860 |
| Text coverage | ~82% |
| Text feature | Titles + tags |
| Audio-visual features | `mean_rgb` (1024-d), `mean_audio` (128-d) |

Built by extracting video IDs from YT8M TFRecords, mapping them to YouTube IDs, then fetching metadata via the YouTube Data API v3.

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install pandas scikit-learn tensorflow requests yt-dlp openai-whisper \
            browser-cookie3 google-api-python-client matplotlib seaborn
```

Set the YouTube API key before running any YouTube scripts:

```bash
export YOUTUBE_API_KEY=your_key_here
```

---

## Data Pipeline

### TikTok

```
Metabase export (query_result_*.csv)
    → deduplicate_videos.py       # → data/TikTok/videos_unique.csv
    → transcript_collection.py    # → data/TikTok/tiktok_transcripts_out/
    → thumbnail_collection.py     # → data/TikTok/tiktok_thumbnails/
    → dataset_with_captions.py    # → data/TikTok/query_with_captions_filled.csv
```

### YouTube-8M

```
TFRecord files (data/Youtube/yt8m/video_level/)
    → extract_video_ids.py        # → data/Youtube/video_ids_all_train.txt
    → convert_yt8m_ids.py         # → data/Youtube/yt8m_id_to_youtube_id.csv
    → download_yt8m_text.py       # → data/Youtube/youtube8m_text.csv  (requires YOUTUBE_API_KEY)
    → merged_yt8m_text.py         # → data/Youtube/yt8m_merged.csv
```

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

## Baseline Results

| | TKGO | YouTube-8M-T |
|---|---|---|
| Videos (total) | 222,552 | 225,042 |
| Unique labels | 101 | 3,860 |
| Text feature | Captions | Title + Tags |
| TF-IDF Micro F1 | — | 0.4974 |
| TF-IDF Macro F1 | — | 0.3645 |

---

## Notes

- `tiktok_transcripts_out/` contains one subdirectory per video, each holding one or more `.vtt` files named `{video_id}_{format}_{language}_{source}.vtt`. Whisper transcripts are identifiable by `_whisper_` in the filename.
- The YouTube-8M TFRecords used here are video-level files (not segment-level). Each record contains a 1024-d `mean_rgb` and 128-d `mean_audio` embedding alongside label IDs.
- The TKGO data is not publicly available and was accessed under a research collaboration agreement with AI Forensics.
