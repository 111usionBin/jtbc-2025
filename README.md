# JTBC News Analysis Project

## 📋 Overview

This project analyzes JTBC News Room's official YouTube channel content by combining web scraping and AI-powered sentiment analysis.

## 🎯 Project Goals

- **Data Collection**: Scrape comments and news scripts from JTBC News Room YouTube channel (October 31, 2024 - November 1, 2025)
- **Storage**: Store collected data in cloud database
- **Analysis**: Leverage LLM to analyze sentiment in comments and scripts
- **Evaluation**: Assess news fairness and objectivity using LLM API calls

## 📁 Project Structure

| File | Purpose |
|------|---------|
| `data_scrape.py` | Web scraping for YouTube comments (initial script collection attempted but only comments were collected) |
| `stt.py` | Script collection using OpenAI API for speech-to-text conversion |
| `stt_resume.py` | Resume script collection from interruption point (due to bot verification) |
| `collect_missing_videos.py` | **Collect missing videos and transcripts from specific date range (2025-04-13 ~ 2025-07-10)** |
| `llm-ev.py` | LLM-based sentiment analysis on collected comments using OpenAI API |
| `ev-run.py` | **Evaluate news transcripts based on criteria in `ev-index.txt` and generate `evaluation_results.csv`** |
| `ev-index.txt` | **Evaluation criteria for news quality assessment (categories, objectivity, fairness, etc.)** |
| `apitest.py` | OpenAI API key testing |
| `llm-tst.py` | LLM API functionality testing |
| `test_download.py` | Download functionality testing |
| `test_openai.py` | OpenAI API integration testing |
| `requirements-uv.txt` | Project dependencies and libraries |

## 🔄 Workflow

1. **Scrape Comments** → Collect YouTube comments using `data_scrape.py`
2. **Extract Scripts** → Convert video audio to text using `stt.py` (OpenAI Whisper API)
   - If interrupted by bot verification, resume with `stt_resume.py`
3. **Collect Missing Videos** → Use `collect_missing_videos.py` to fetch videos from specific date ranges that were missed
4. **Store** → Save data to cloud database
5. **Analyze** → Process sentiment using `llm-ev.py` (OpenAI API)
6. **Evaluate Transcripts** → Run `ev-run.py` to evaluate news quality based on `ev-index.txt` criteria
   - Generates `evaluation_results.csv` with scores for each transcript

## ⚠️ Known Issues

- **data_scrape.py**: Initially designed to collect both comments and scripts, but only successfully collects comments
- **Bot Verification**: Script collection may be interrupted by bot verification during execution
  - Use `stt_resume.py` to continue from the last successful collection point

## 🚀 Getting Started

Install dependencies:
```bash
pip install -r requirements-uv.txt
```

Run data collection:
```bash
# Collect comments
python data_scrape.py

# Collect scripts via STT
python stt.py

# Resume if interrupted
python stt_resume.py

# Collect missing videos from specific date range
python collect_missing_videos.py
```

Execute sentiment analysis:
```bash
python llm-ev.py
```

Evaluate news transcripts:
```bash
# Evaluate transcripts based on ev-index.txt criteria
python ev-run.py
# Output: evaluation_results.csv
```

Test API connectivity:
```bash
python apitest.py
# or
python test_openai.py
```

## System Dependencies (Windows)

This project requires the following system-level dependencies:

```bash
winget install --id Gyan.FFmpeg -e
winget install --id OpenJS.NodeJS.LTS -e
```