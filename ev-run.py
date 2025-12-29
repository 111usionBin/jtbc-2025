import os
import csv
import json
from openai import OpenAI
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_URL = os.getenv("SUPABASE_CONNECTION_STRING")

missing_env = [
    name for name, value in (
        ("OPENAI_API_KEY", OPENAI_API_KEY),
        ("SUPABASE_CONNECTION_STRING", DB_URL),
    ) if not value
]
if missing_env:
    raise EnvironmentError(f"Missing environment variables: {', '.join(missing_env)}")

# 클라이언트 초기화
openai_client = OpenAI(api_key=OPENAI_API_KEY)

COLUMN_HEADERS = [
    "번호","S","O1","O2","O3","O4","O5","C1","F1","F2","F3","F4","F5","C2","C3","C4","C5",
    "C6","E1","E2","E3","E4","E5","CR1","CR2","CR3","CR4","CR5","SV1","SV2","SV3",
    "SU1","SU2","SU3","D1","D2","제목","특이사항","출연자","영상ID"
]
SCORE_HEADERS = [h for h in COLUMN_HEADERS if h not in {"번호","S","제목","특이사항","출연자","영상ID"}]

def read_evaluation_criteria(file_path="ev-index.txt"):
    """평가 기준 파일 읽기"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def get_transcripts_from_supabase():
    """PostgreSQL에서 transcript 가져오기"""
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    id,
                    transcript AS content,
                    title,
                    video_id,
                    published_at AS air_date
                FROM videos 
                WHERE transcript IS NOT NULL
            """)
            return cur.fetchall()

def _parse_iso_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None

def _next_article_number(date_key, counters):
    counters[date_key] += 1
    return f"뉴-L-{date_key}-{counters[date_key]:02d}"

def _build_prompt(criteria, transcript_text):
    score_lines = "\n".join([f"- {key}: 0 혹은 1 혹은 9" for key in SCORE_HEADERS])
    return f"""
아래는 평가 기준입니다.

{criteria}

위 기준을 사용해서 대본을 평가하세요.
필수 규칙:
1. 각 지표(O1~D2)는 0, 1, 9 중 하나만 사용합니다.
2. S 열에는 대본이 속한 카테고리명을 한국어로 작성합니다.
3. JSON만 출력합니다.

채워야 할 지표:
{score_lines}

JSON 스키마 예시:
{{
  "category": "카테고리명",
  "scores": {{"O1": 0, "...": 1}},
  "notes": "특이사항 요약 (없으면 빈 문자열)",
  "participants": "출연자 정보 (없으면 빈 문자열)"
}}

대본:
{transcript_text}
"""

def evaluate_transcript(transcript_text, criteria):
    prompt = _build_prompt(criteria, transcript_text)
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "당신은 JTBC 뉴스룸 심사위원으로 모든 기준을 0/1/9로만 평가합니다."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )
    try:
        return json.loads(response.choices[0].message.content)
    except (json.JSONDecodeError, AttributeError, IndexError):
        return {"category": "", "scores": {}, "notes": "", "participants": ""}

def _normalize_score(value):
    try:
        val = int(value)
    except (TypeError, ValueError):
        return ""
    return val if val in {0, 1, 9} else ""

def build_row(transcript, evaluation, article_number):
    scores = evaluation.get("scores", {})
    row = []
    for header in COLUMN_HEADERS:
        if header == "번호":
            row.append(article_number)
        elif header == "S":
            row.append(evaluation.get("category", ""))
        elif header in SCORE_HEADERS:
            row.append(_normalize_score(scores.get(header)))
        elif header == "제목":
            row.append(transcript.get("title", ""))
        elif header == "특이사항":
            row.append(evaluation.get("notes") or transcript.get("notes", ""))
        elif header == "출연자":
            row.append(evaluation.get("participants") or transcript.get("participants", ""))
        elif header == "영상ID":
            row.append(transcript.get("video_id", ""))
    return row

def save_to_csv(rows, output_file="evaluation_results.csv"):
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(COLUMN_HEADERS)
        writer.writerows(rows)

def main():
    criteria = read_evaluation_criteria()
    transcripts = get_transcripts_from_supabase()
    transcripts.sort(key=lambda t: _parse_iso_datetime(t.get("air_date") or t.get("created_at")) or datetime.now())

    results = []
    daily_counters = defaultdict(int)

    for transcript in transcripts:
        print(f"평가 중: ID {transcript['id']}")
        evaluation = evaluate_transcript(transcript.get('content', ''), criteria)

        date_obj = _parse_iso_datetime(transcript.get("air_date") or transcript.get("created_at")) or datetime.now()
        date_key = date_obj.strftime("%y%m%d")
        article_number = _next_article_number(date_key, daily_counters)

        results.append(build_row(transcript, evaluation, article_number))

    save_to_csv(results)
    print(f"평가 완료! 총 {len(transcripts)}개 transcript 평가됨")

if __name__ == "__main__":
    main()