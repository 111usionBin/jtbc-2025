
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
| `data_scrape.py` | Web scraping for comments and news scripts |
| `requirements-uv.txt` | Project dependencies and libraries |
| `llm-ev.py` | LLM-based sentiment and fairness evaluation |

## 🔄 Workflow

1. **Scrape** → Collect YouTube comments and news scripts
2. **Store** → Save data to cloud database
3. **Analyze** → Process sentiment using LLM
4. **Evaluate** → Assess news fairness via LLM API

## 🚀 Getting Started

Install dependencies:
```bash
pip install -r requirements-uv.txt
```

Run data collection:
```bash
python data_scrape.py
```

Execute evaluation:
```bash
python llm-ev.py
```
