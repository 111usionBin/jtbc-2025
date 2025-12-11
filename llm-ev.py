import os
import json
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from openai import OpenAI

load_dotenv()
OPENAI_API_KEY = os.getenv("openai_api_key")
DB_URL = os.getenv("SUPABASE_CONNECTION_STRING")

client = OpenAI(api_key=OPENAI_API_KEY)
print(os.getenv("openai_api_key"))

SYSTEM_PROMPT = (
    "You are a strict JSON generator. For each input text, return:\n"
    '{"sentiment": -1.0_to_1.0, "fairness": 0.0_to_1.0, "notes": "very brief reason"}\n'
    "Sentiment: -1 very negative, 0 neutral, +1 very positive.\n"
    "Fairness: 0 unfair/biased, 1 fully fair/neutral."
)

def fetch_data(conn):
    # Comments
    comments = pd.read_sql(
        "SELECT text, published_at::date AS dt FROM comments WHERE text IS NOT NULL",
        conn,
    )
    # Transcripts
    transcripts = pd.read_sql(
        "SELECT transcript AS text, published_at::date AS dt FROM videos WHERE transcript IS NOT NULL",
        conn,
    )
    return pd.concat([comments, transcripts], ignore_index=True).dropna()

def analyze_batch(texts):
    joined = "\n---\n".join(texts)
    prompt = (
        "For each text separated by --- output a JSON array in order. "
        "Each element follows the schema above.\n"
        f"{joined}"
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": SYSTEM_PROMPT},
                  {"role": "user", "content": prompt}],
        temperature=0
    )
    content = resp.choices[0].message.content.strip()
    try:
        data = json.loads(content)
        if isinstance(data, dict):  # if single object, wrap
            data = [data]
        return data
    except Exception:
        return [{"sentiment": 0, "fairness": 0.5, "notes": "parse_error"} for _ in texts]

def score_dataframe(df, batch_size=10):
    sentiments, fairnesses, notes = [], [], []
    for i in tqdm(range(0, len(df), batch_size)):
        batch = df.iloc[i:i+batch_size]
        results = analyze_batch(batch["text"].tolist())
        for res in results:
            sentiments.append(res.get("sentiment", 0))
            fairnesses.append(res.get("fairness", 0.5))
            notes.append(res.get("notes", ""))
    df = df.iloc[:len(sentiments)].copy()
    df["sentiment"] = sentiments
    df["fairness"] = fairnesses
    df["notes"] = notes
    return df

def aggregate_timeseries(scored_df):
    return (
        scored_df.groupby("dt")
        .agg(sentiment_avg=("sentiment", "mean"),
             fairness_avg=("fairness", "mean"),
             n=("text", "count"))
        .reset_index()
        .sort_values("dt")
    )

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS llm_scores (
                id SERIAL PRIMARY KEY,
                dt DATE NOT NULL,
                text TEXT NOT NULL,
                sentiment DOUBLE PRECISION,
                fairness DOUBLE PRECISION,
                notes TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
    conn.commit()

def insert_scores(conn, scored_df, batch_size=500):
    rows = list(
        scored_df[["dt", "text", "sentiment", "fairness", "notes"]].itertuples(index=False, name=None)
    )
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            execute_values(
                cur,
                "INSERT INTO llm_scores (dt, text, sentiment, fairness, notes) VALUES %s",
                rows[i:i+batch_size],
            )
    conn.commit()

def main():
    if not OPENAI_API_KEY or not DB_URL:
        raise RuntimeError("Missing openai_api_key or SUPABASE_CONNECTION_STRING in .env")
    with psycopg2.connect(DB_URL) as conn:
        create_table(conn)
        df = fetch_data(conn)
        if df.empty:
            print("No data found.")
            return
        scored = score_dataframe(df)
        insert_scores(conn, scored)
        ts = aggregate_timeseries(scored)
        print(ts.head(10))

if __name__ == "__main__":
    main()
