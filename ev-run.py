import os
import csv
from openai import OpenAI
from supabase import create_client, Client
from datetime import datetime

# 환경 변수에서 API 키 가져오기
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# 클라이언트 초기화
openai_client = OpenAI(api_key=OPENAI_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def read_evaluation_criteria(file_path="ev-index.txt"):
    """평가 기준 파일 읽기"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def get_transcripts_from_supabase():
    """Supabase에서 transcript 가져오기"""
    response = supabase.table('transcripts').select('*').execute()
    return response.data

def evaluate_transcript(transcript_text, criteria):
    """OpenAI API를 사용하여 transcript 평가"""
    prompt = f"""
다음 평가 기준을 사용하여 제공된 대본을 평가해주세요:

{criteria}

대본:
{transcript_text}

각 기준에 대해 점수(1-10)와 이유를 JSON 형식으로 제공해주세요.
형식: {{"기준명": {{"점수": 점수, "이유": "이유"}}}}
"""
    
    response = openai_client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "당신은 대본을 평가하는 전문가입니다."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7
    )
    
    return response.choices[0].message.content

def save_to_csv(results, output_file="evaluation_results.csv"):
    """평가 결과를 CSV 파일로 저장"""
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', '기준', '점수', '이유', '평가시간'])
        
        for result in results:
            writer.writerow(result)

def main():
    # 평가 기준 읽기
    criteria = read_evaluation_criteria()
    
    # Supabase에서 transcript 가져오기
    transcripts = get_transcripts_from_supabase()
    
    results = []
    
    for transcript in transcripts:
        print(f"평가 중: ID {transcript['id']}")
        
        # GPT로 평가
        evaluation = evaluate_transcript(transcript['content'], criteria)
        
        # 결과 저장
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        results.append([
            transcript['id'],
            '종합평가',
            evaluation,
            '',
            timestamp
        ])
    
    # CSV 저장
    save_to_csv(results)
    print(f"평가 완료! 총 {len(transcripts)}개 transcript 평가됨")

if __name__ == "__main__":
    main()