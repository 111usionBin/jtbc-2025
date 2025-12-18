import os
import yt_dlp
from openai import OpenAI
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import random

# .env 파일 로드
load_dotenv()

# 환경 변수 설정
SUPABASE_CONNECTION_STRING = os.getenv("SUPABASE_CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# 네트워크/다운로드 튜닝용 환경 변수
YTDLP_PROXY = os.getenv("YTDLP_PROXY")  # 예: http://127.0.0.1:7890
YTDLP_COOKIEFILE = os.getenv("YTDLP_COOKIEFILE")  # 예: c:\path\to\cookies.txt
YTDLP_SLEEP_MIN = int(os.getenv("YTDLP_SLEEP_MIN", "1"))
YTDLP_SLEEP_MAX = int(os.getenv("YTDLP_SLEEP_MAX", "3"))
YTDLP_MAX_ATTEMPTS = int(os.getenv("YTDLP_MAX_ATTEMPTS", "5"))
YTDLP_BACKOFF_BASE = float(os.getenv("YTDLP_BACKOFF_BASE", "2"))

# OpenAI 클라이언트 초기화
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def get_db_connection():
    """PostgreSQL 데이터베이스 연결을 반환합니다."""
    return psycopg2.connect(SUPABASE_CONNECTION_STRING)

def build_ydl_opts(output_path: str) -> dict:
    """yt-dlp 옵션을 구성합니다."""
    opts = {
        'format': 'bestaudio[ext=m4a]/bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'outtmpl': output_path,
        'quiet': True,
        'noplaylist': True,
        'socket_timeout': 30,
        'retries': 10,
        'fragment_retries': 10,
        'concurrent_fragment_downloads': 1,
        'sleep_interval': YTDLP_SLEEP_MIN,
        'max_sleep_interval': YTDLP_SLEEP_MAX,
        'geo_bypass': True,
        'http_headers': {
            # 일부 네트워크/차단 회피용 UA 지정
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
        },
        'extractor_args': {
            'youtube': {
                # 플레이어 클라이언트 변경으로 차단 회피 시도
                'player_client': ['android', 'web']
            }
        },
    }
    if YTDLP_PROXY:
        opts['proxy'] = YTDLP_PROXY
    if YTDLP_COOKIEFILE and Path(YTDLP_COOKIEFILE).exists():
        opts['cookiefile'] = YTDLP_COOKIEFILE
    return opts

def download_audio(video_id: str, output_path: str) -> str:
    """유튜브 영상의 오디오를 다운로드합니다."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = build_ydl_opts(output_path)

    last_err = None
    for attempt in range(1, YTDLP_MAX_ATTEMPTS + 1):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            return f"{output_path}.mp3"
        except Exception as e:
            last_err = e
            delay = YTDLP_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
            print(f"  - 다운로드 재시도 {attempt}/{YTDLP_MAX_ATTEMPTS} 예정, 대기 {delay:.1f}s: {e}")
            time.sleep(delay)

    raise RuntimeError(f"오디오 다운로드 실패 ({video_id}): {last_err}")

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
                # 다음 요청 전에 잠시 대기하여 차단/레이트리밋을 피합니다.
                time.sleep(random.uniform(YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX))
                continue

            # 각 영상 사이에도 짧게 대기
            time.sleep(random.uniform(YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX))

    print("모든 영상 처리 완료!")

if __name__ == "__main__":
    main()
