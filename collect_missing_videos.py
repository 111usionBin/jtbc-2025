# """
# 누락된 영상 수집 스크립트 (2025-04-13 ~ 2025-07-10)
# DB에서 해당 기간의 대본이 없는 영상을 가져와 대본을 추출합니다.
# """
import os
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import tempfile
import time
import random
import httpx
import yt_dlp
from datetime import datetime, timedelta

# 날짜 범위 설정
START_DATE = "2025-04-13"  # 시작 날짜 (포함)
END_DATE = "2025-07-11"    # 종료 날짜 (2025-07-10 포함을 위해 +1일)

# .env 파일 로드
script_env = Path(__file__).with_name(".env")
repo_root_env = Path(__file__).resolve().parents[1] / ".env"
loaded_env_path = None
if script_env.exists():
    load_dotenv(dotenv_path=script_env)
    loaded_env_path = script_env
elif repo_root_env.exists():
    load_dotenv(dotenv_path=repo_root_env)
    loaded_env_path = repo_root_env
else:
    load_dotenv()
    loaded_env_path = "default search"

# 환경 변수 설정
SUPABASE_CONNECTION_STRING = os.getenv("SUPABASE_CONNECTION_STRING")
_raw_key = os.getenv("OPENAI_API_KEY") or ""
_key = _raw_key.strip()
if (_key.startswith('"') and _key.endswith('"')) or (_key.startswith("'") and _key.endswith("'")) or (_key.startswith("`") and _key.endswith("`")):
    _key = _key[1:-1].strip()
OPENAI_API_KEY = _key

OPENAI_BASE_URL = (os.getenv("OPENAI_BASE_URL") or "").strip()
OPENAI_ORG_ID = (os.getenv("OPENAI_ORG_ID") or "").strip()
OPENAI_PROJECT_ID = (os.getenv("OPENAI_PROJECT_ID") or "").strip()
OPENAI_PROXY = (os.getenv("OPENAI_PROXY") or "").strip()

# 네트워크/다운로드 튜닝
YTDLP_PROXY = os.getenv("YTDLP_PROXY")
YTDLP_COOKIEFILE = os.getenv("YTDLP_COOKIEFILE")
YTDLP_SLEEP_MIN = int(os.getenv("YTDLP_SLEEP_MIN", "5"))
YTDLP_SLEEP_MAX = int(os.getenv("YTDLP_SLEEP_MAX", "10"))
YTDLP_MAX_ATTEMPTS = int(os.getenv("YTDLP_MAX_ATTEMPTS", "5"))
YTDLP_BACKOFF_BASE = float(os.getenv("YTDLP_BACKOFF_BASE", "2"))

def _mask_key(k: str) -> str:
    if not k:
        return "None"
    return f"{k[:6]}...{k[-4:]}"

def _check_key_format(k: str):
    if not k:
        raise SystemExit("OPENAI_API_KEY 가 비어있습니다.")
    if not (k.startswith("sk-") or k.startswith("sk_proj-") or k.startswith("sk-proj-")):
        raise SystemExit("OPENAI_API_KEY 포맷이 올바르지 않습니다.")
    for bad in ['"', "'", "`", " "]:
        if bad in k:
            raise SystemExit("OPENAI_API_KEY 값에 내부 따옴표/공백이 포함되어 있습니다.")

_check_key_format(OPENAI_API_KEY)

USE_HTTPX_TRUST_ENV = False
if OPENAI_PROXY:
    os.environ["HTTPS_PROXY"] = OPENAI_PROXY
    os.environ["HTTP_PROXY"] = OPENAI_PROXY
    USE_HTTPX_TRUST_ENV = True

try:
    http_client = httpx.Client(timeout=6300.0, trust_env=USE_HTTPX_TRUST_ENV)
except TypeError:
    http_client = httpx.Client(timeout=6300.0)

openai_client = OpenAI(
    api_key=OPENAI_API_KEY,
    base_url=OPENAI_BASE_URL or None,
    organization=OPENAI_ORG_ID or None,
    project=OPENAI_PROJECT_ID or None,
    http_client=http_client,
)

def validate_openai_credentials():
    """키 유효성을 빠르게 점검합니다."""
    try:
        base = OPENAI_BASE_URL or "https://api.openai.com/v1 (default)"
        proxy = OPENAI_PROXY or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "none"
        openai_client.models.list()
        print(f"OpenAI 키 확인 완료: {_mask_key(OPENAI_API_KEY)} | base_url={base} | proxy={proxy}")
    except Exception as e:
        msg = str(e)
        status_code = getattr(e, "status_code", None)
        if status_code == 401 or "invalid_api_key" in msg or "status': 401" in msg:
            print(f"OpenAI 인증 실패(401). 키를 확인하세요.")
            raise SystemExit(1)
        raise

def get_db_connection():
    """PostgreSQL 데이터베이스 연결을 반환합니다."""
    return psycopg2.connect(SUPABASE_CONNECTION_STRING)

def get_videos_without_transcript_in_range(start_date: str, end_date: str, table_name: str = "videos"):
    """특정 날짜 범위에서 대본이 없는 영상 목록을 가져옵니다."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, video_id, title, published_at 
                FROM {table_name} 
                WHERE transcript IS NULL 
                AND published_at >= %s 
                AND published_at < %s
                ORDER BY published_at
                """,
                (start_date, end_date)
            )
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

def build_ydl_opts(output_path: str) -> dict:
    """yt-dlp 옵션을 구성합니다."""
    opts = {
        'format': 'bestaudio[ext=m4a]/bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '32',
        }],
        'postprocessor_args': [
            '-ar', '8000',
            '-ac', '1',
        ],
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate',
        },
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'web'],
                'skip': ['hls', 'dash'],
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
            err_msg = str(e).lower()
            
            if 'bot' in err_msg or 'captcha' in err_msg or '429' in err_msg or 'too many requests' in err_msg:
                print(f"  - ⚠️ 봇 차단 감지! 대기 시간을 늘립니다...")
                delay = YTDLP_BACKOFF_BASE * (2 ** (attempt - 1)) * 2 + random.uniform(5, 15)
            else:
                delay = YTDLP_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 1.0)
            
            print(f"  - 다운로드 재시도 {attempt}/{YTDLP_MAX_ATTEMPTS} 예정, 대기 {delay:.1f}s: {e}")
            time.sleep(delay)

    raise RuntimeError(f"오디오 다운로드 실패 ({video_id}): {last_err}")

def split_audio_file(audio_path: str, chunk_duration_minutes: int = 10) -> list:
    """오디오 파일을 여러 청크로 분할합니다."""
    try:
        from pydub import AudioSegment
    except ImportError:
        raise RuntimeError("pydub 패키지가 필요합니다. 설치: pip install pydub")
    
    audio = AudioSegment.from_mp3(audio_path)
    chunk_length_ms = chunk_duration_minutes * 60 * 1000
    
    chunks = []
    for i in range(0, len(audio), chunk_length_ms):
        chunk = audio[i:i + chunk_length_ms]
        chunk_path = f"{audio_path}_chunk_{i//chunk_length_ms}.mp3"
        chunk.export(chunk_path, format="mp3", bitrate="32k", parameters=["-ar", "8000", "-ac", "1"])
        chunks.append(chunk_path)
    
    return chunks

def transcribe_audio(audio_path: str) -> str:
    """OpenAI Whisper API를 사용하여 오디오를 텍스트로 변환합니다."""
    file_size_mb = os.path.getsize(audio_path) / (1024 * 1024)
    
    if file_size_mb > 25:
        print(f"  - 파일 크기({file_size_mb:.1f}MB)가 25MB 초과, 자동 분할 처리 중...")
        chunk_files = split_audio_file(audio_path, chunk_duration_minutes=10)
        print(f"  - {len(chunk_files)}개 청크로 분할 완료")
        
        transcripts = []
        for idx, chunk_path in enumerate(chunk_files, 1):
            try:
                chunk_size_mb = os.path.getsize(chunk_path) / (1024 * 1024)
                print(f"  - 청크 {idx}/{len(chunk_files)} 처리 중 ({chunk_size_mb:.1f}MB)...")
                
                with open(chunk_path, "rb") as audio_file:
                    transcript = openai_client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file,
                        language="ko"
                    )
                    transcripts.append(transcript.text)
            finally:
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)
            
            if idx < len(chunk_files):
                time.sleep(1)
        
        print(f"  - 모든 청크 처리 완료, 텍스트 결합 중...")
        return " ".join(transcripts)
    
    print(f"  - 파일 크기: {file_size_mb:.1f}MB (직접 처리)")
    with open(audio_path, "rb") as audio_file:
        try:
            transcript = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language="ko"
            )
        except Exception as e:
            msg = str(e)
            if "invalid_api_key" in msg or "status': 401" in msg:
                raise RuntimeError("OpenAI 401: API 키가 올바르지 않습니다.")
            raise
    return transcript.text

def main():
    """메인 실행 함수"""
    print(f"\n{'='*60}")
    print(f"📅 누락된 영상 대본 추출: {START_DATE} ~ {END_DATE[:10]}")
    print(f"⏱️ 요청 간 대기 시간: {YTDLP_SLEEP_MIN}~{YTDLP_SLEEP_MAX}초")
    if YTDLP_COOKIEFILE and Path(YTDLP_COOKIEFILE).exists():
        print(f"🍪 쿠키 파일 사용: {YTDLP_COOKIEFILE}")
    else:
        print(f"⚠️ 쿠키 파일 미사용 - 봇 차단 위험이 높습니다!")
        print(f"   해결: .env에 YTDLP_COOKIEFILE 경로 추가")
    print(f"{'='*60}\n")
    
    validate_openai_credentials()
    
    # DB에서 해당 기간의 대본 없는 영상 가져오기
    videos = get_videos_without_transcript_in_range(START_DATE, END_DATE)
    
    if not videos:
        print(f"대본이 필요한 영상이 없습니다. ({START_DATE} ~ {END_DATE[:10]})")
        return
    
    print(f"총 {len(videos)}개의 영상 대본을 추출합니다.")
    print(f"첫 영상: {videos[0]['video_id']} ({videos[0]['published_at']})")
    print(f"마지막 영상: {videos[-1]['video_id']} ({videos[-1]['published_at']})\n")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for idx, video in enumerate(videos, 1):
            video_id = video['video_id']
            title = video.get('title', '')[:50]
            published_at = video.get('published_at', 'N/A')
            print(f"\n[{idx}/{len(videos)}] {video_id} ({published_at})")
            if title:
                print(f"  제목: {title}...")
            
            try:
                audio_path = os.path.join(temp_dir, video_id)
                downloaded_file = download_audio(video_id, audio_path)
                print(f"  - ✅ 오디오 다운로드 완료")
                
                transcript = transcribe_audio(downloaded_file)
                print(f"  - ✅ 대본 추출 완료 (길이: {len(transcript)} 자)")
                
                update_transcript(video_id, transcript)
                print(f"  - ✅ DB 업데이트 완료")
                
                if os.path.exists(downloaded_file):
                    os.remove(downloaded_file)
                
            except Exception as e:
                print(f"  - ❌ 오류 발생: {str(e)}")
                if "401" in str(e) or "invalid_api_key" in str(e):
                    print("  - 인증 오류로 작업을 중단합니다.")
                    break
                
                if 'bot' in str(e).lower() or 'captcha' in str(e).lower():
                    wait_time = random.uniform(30, 60)
                    print(f"  - ⚠️ 봇 차단 감지! {wait_time:.0f}초 대기 후 다음 영상으로...")
                    time.sleep(wait_time)
                else:
                    time.sleep(random.uniform(YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX))
                continue

            wait_time = random.uniform(YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX)
            print(f"  - ⏱️ {wait_time:.1f}초 대기 중...")
            time.sleep(wait_time)

    print("\n✅ 모든 영상 처리 완료!")

if __name__ == "__main__":
    main()
