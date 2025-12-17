import os
import yt_dlp
from openai import OpenAI
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# .env 파일 로드
load_dotenv()

# 환경 변수 설정
SUPABASE_CONNECTION_STRING = os.getenv("SUPABASE_CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# OpenAI 클라이언트 초기화
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def get_db_connection():
    """PostgreSQL 데이터베이스 연결을 반환합니다."""
    return psycopg2.connect(SUPABASE_CONNECTION_STRING)

def download_audio(video_id: str, output_path: str) -> str:
    """유튜브 영상의 오디오를 다운로드합니다."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    
    ydl_opts = {
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': output_path,
        'quiet': True,
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])
    
    return f"{output_path}.mp3"

def transcribe_audio(audio_path: str) -> str:
    """OpenAI Whisper API를 사용하여 오디오를 텍스트로 변환합니다."""
    with open(audio_path, "rb") as audio_file:
        transcript = openai_client.audio.transcriptions.create(
            model="whisper-1",
            file=audio_file,
            language="ko"
        )
    return transcript.text

def get_videos_without_transcript(table_name: str = "videos"):
    """대본이 없는 영상 목록을 가져옵니다."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT id, video_id FROM {table_name} WHERE transcript IS NULL")
            return cur.fetchall()
    finally:
        conn.close()

def update_transcript(video_id: str, transcript: str, table_name: str = "videos"):
    """영상의 대본을 업데이트합니다."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {table_name} SET transcript = %s WHERE video_id = %s",
                (transcript, video_id)
            )
            conn.commit()
    finally:
        conn.close()

def main():
    """메인 실행 함수"""
    # 대본이 없는 영상 목록 조회
    videos = get_videos_without_transcript()
    
    if not videos:
        print("대본이 필요한 영상이 없습니다.")
        return
    
    print(f"총 {len(videos)}개의 영상 대본을 추출합니다.")
    
    # 임시 디렉토리 생성
    with tempfile.TemporaryDirectory() as temp_dir:
        for idx, video in enumerate(videos, 1):
            video_id = video['video_id']
            print(f"[{idx}/{len(videos)}] 영상 {video_id} 처리 중...")
            
            try:
                # 오디오 다운로드
                audio_path = os.path.join(temp_dir, video_id)
                downloaded_file = download_audio(video_id, audio_path)
                print(f"  - 오디오 다운로드 완료")
                
                # STT 수행
                transcript = transcribe_audio(downloaded_file)
                print(f"  - 대본 추출 완료 (길이: {len(transcript)} 자)")
                
                # Supabase 업데이트
                update_transcript(video_id, transcript)
                print(f"  - DB 업데이트 완료")
                
                # 임시 파일 삭제
                if os.path.exists(downloaded_file):
                    os.remove(downloaded_file)
                
            except Exception as e:
                print(f"  - 오류 발생: {str(e)}")
                continue
    
    print("모든 영상 처리 완료!")

if __name__ == "__main__":
    main()
