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
| `llm-ev.py` | LLM-based sentiment analysis on collected comments using OpenAI API |
| `apitest.py` | OpenAI API key testing |
| `llm-tst.py` | LLM API functionality testing |
| `test_download.py` | Download functionality testing |
| `test_openai.py` | OpenAI API integration testing |
| `requirements-uv.txt` | Project dependencies and libraries |

## 🔄 Workflow

1. **Scrape Comments** → Collect YouTube comments using `data_scrape.py`
2. **Extract Scripts** → Convert video audio to text using `stt.py` (OpenAI Whisper API)
   - If interrupted by bot verification, resume with `stt_resume.py`
3. **Store** → Save data to cloud database
4. **Analyze** → Process sentiment using `llm-ev.py` (OpenAI API)
5. **Evaluate** → Assess news fairness via LLM analysis

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
```

Execute sentiment analysis:
```bash
python llm-ev.py
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