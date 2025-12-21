"""
YouTube 영상 다운로드 테스트 스크립트
OpenAI API와 독립적으로 영상 다운로드 기능만 테스트합니다.
"""
import os
import tempfile
import time
import random
from pathlib import Path
import yt_dlp
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# .env 파일 로드
script_env = Path(__file__).with_name(".env")
if script_env.exists():
    load_dotenv(dotenv_path=script_env)
else:
    load_dotenv()

# Supabase 연결 문자열
SUPABASE_CONNECTION_STRING = os.getenv("SUPABASE_CONNECTION_STRING")

# 네트워크/다운로드 튜닝용 환경 변수
YTDLP_PROXY = os.getenv("YTDLP_PROXY")
YTDLP_COOKIEFILE = os.getenv("YTDLP_COOKIEFILE")
YTDLP_SLEEP_MIN = int(os.getenv("YTDLP_SLEEP_MIN", "1"))
YTDLP_SLEEP_MAX = int(os.getenv("YTDLP_SLEEP_MAX", "3"))
YTDLP_MAX_ATTEMPTS = int(os.getenv("YTDLP_MAX_ATTEMPTS", "5"))
YTDLP_BACKOFF_BASE = float(os.getenv("YTDLP_BACKOFF_BASE", "2"))

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
        'quiet': False,  # 디버그를 위해 출력 활성화
        'noplaylist': True,
        'socket_timeout': 30,
        'retries': 10,
        'fragment_retries': 10,
        'concurrent_fragment_downloads': 1,
        'sleep_interval': YTDLP_SLEEP_MIN,
        'max_sleep_interval': YTDLP_SLEEP_MAX,
        'geo_bypass': True,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
        },
        'extractor_args': {
            'youtube': {
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
            print(f"  다운로드 시도 {attempt}/{YTDLP_MAX_ATTEMPTS}...")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            mp3_path = f"{output_path}.mp3"
            if os.path.exists(mp3_path):
                file_size = os.path.getsize(mp3_path) / (1024 * 1024)  # MB
                print(f"  ✅ 다운로드 성공! 파일 크기: {file_size:.2f} MB")
                return mp3_path
            else:
                print(f"  ⚠️ 다운로드는 완료되었으나 파일을 찾을 수 없음: {mp3_path}")
                return mp3_path
        except Exception as e:
            last_err = e
            delay = YTDLP_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
            print(f"  ❌ 다운로드 실패 (시도 {attempt}/{YTDLP_MAX_ATTEMPTS}): {e}")
            if attempt < YTDLP_MAX_ATTEMPTS:
                print(f"  {delay:.1f}초 대기 후 재시도...")
                time.sleep(delay)

    raise RuntimeError(f"오디오 다운로드 실패 ({video_id}): {last_err}")

def get_videos_without_transcript(table_name: str = "videos", limit: int = 3):
    """대본이 없는 영상 목록을 가져옵니다."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT id, video_id FROM {table_name} WHERE transcript IS NULL LIMIT {limit}")
            return cur.fetchall()
    finally:
        conn.close()

def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("YouTube 영상 다운로드 테스트")
    print("=" * 60)
    print()
    
    # DB 연결 테스트
    print("📊 데이터베이스 연결 테스트...")
    try:
        videos = get_videos_without_transcript(limit=3)
        print(f"✅ DB 연결 성공! 대본이 없는 영상 {len(videos)}개 발견")
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
        return
    
    if not videos:
        print("\n⚠️ 대본이 필요한 영상이 없습니다.")
        return
    
    print(f"\n처리할 영상 목록:")
    for idx, video in enumerate(videos, 1):
        print(f"  {idx}. video_id: {video['video_id']}")
    
    print(f"\n{'=' * 60}")
    print("영상 다운로드 시작")
    print("=" * 60)
    
    # 임시 디렉토리 생성
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"임시 디렉토리: {temp_dir}\n")
        
        success_count = 0
        fail_count = 0
        
        for idx, video in enumerate(videos, 1):
            video_id = video['video_id']
            print(f"\n[{idx}/{len(videos)}] 영상 {video_id} 다운로드 중...")
            print(f"URL: https://www.youtube.com/watch?v={video_id}")
            
            try:
                # 오디오 다운로드
                audio_path = os.path.join(temp_dir, video_id)
                downloaded_file = download_audio(video_id, audio_path)
                
                # 파일 정보 출력
                if os.path.exists(downloaded_file):
                    print(f"  📁 저장 경로: {downloaded_file}")
                    success_count += 1
                else:
                    print(f"  ⚠️ 파일이 생성되지 않았습니다.")
                    fail_count += 1
                
            except Exception as e:
                print(f"  ❌ 오류 발생: {str(e)}")
                fail_count += 1
                continue
            
            # 다음 영상 전 짧은 대기
            if idx < len(videos):
                wait_time = random.uniform(YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX)
                print(f"  ⏳ {wait_time:.1f}초 대기...")
                time.sleep(wait_time)
    
    print(f"\n{'=' * 60}")
    print("테스트 완료!")
    print("=" * 60)
    print(f"✅ 성공: {success_count}개")
    print(f"❌ 실패: {fail_count}개")
    print()
    print("💡 다운로드가 정상적으로 작동한다면, OpenAI API 키 문제만 해결하면 됩니다.")

if __name__ == "__main__":
    main()
