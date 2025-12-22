import os
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import tempfile
import time
import random
import httpx  # 신규: OpenAI 클라이언트에 프록시/환경제어 적용
import yt_dlp

# .env 파일 로드 (스크립트 폴더 -> 레포 루트 -> 기본 검색)
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
    load_dotenv()  # fallback: dotenv 기본 검색
    loaded_env_path = "default search"

# 환경 변수 설정 (앞뒤 공백/따옴표 제거; 내부 공백/따옴표는 오류)
SUPABASE_CONNECTION_STRING = os.getenv("SUPABASE_CONNECTION_STRING")
_raw_key = os.getenv("OPENAI_API_KEY") or ""
_key = _raw_key.strip()
# 제거 가능한 둘러싼 따옴표/backtick
if (_key.startswith('"') and _key.endswith('"')) or (_key.startswith("'") and _key.endswith("'")) or (_key.startswith("`") and _key.endswith("`")):
    _key = _key[1:-1].strip()
OPENAI_API_KEY = _key

# OpenAI 관련 추가 환경변수 (없으면 빈 문자열)
OPENAI_BASE_URL = (os.getenv("OPENAI_BASE_URL") or "").strip()
OPENAI_ORG_ID = (os.getenv("OPENAI_ORG_ID") or "").strip()
OPENAI_PROJECT_ID = (os.getenv("OPENAI_PROJECT_ID") or "").strip()
OPENAI_PROXY = (os.getenv("OPENAI_PROXY") or "").strip()

# 네트워크/다운로드 튜닝용 환경 변수
YTDLP_PROXY = os.getenv("YTDLP_PROXY")  # 예: http://127.0.0.1:7890
YTDLP_COOKIEFILE = os.getenv("YTDLP_COOKIEFILE")  # 예: c:\path\to\cookies.txt
YTDLP_SLEEP_MIN = int(os.getenv("YTDLP_SLEEP_MIN", "1"))
YTDLP_SLEEP_MAX = int(os.getenv("YTDLP_SLEEP_MAX", "3"))
YTDLP_MAX_ATTEMPTS = int(os.getenv("YTDLP_MAX_ATTEMPTS", "5"))
YTDLP_BACKOFF_BASE = float(os.getenv("YTDLP_BACKOFF_BASE", "2"))

def _mask_key(k: str) -> str:
    if not k:
        return "None"
    return f"{k[:6]}...{k[-4:]}"

def _check_key_format(k: str):
    # 빈값/포맷/내부 공백·따옴표 체크
    if not k:
        raise SystemExit("OPENAI_API_KEY 가 비어있습니다. .env 위치/변수명을 확인하세요.")
    if not (k.startswith("sk-") or k.startswith("sk_proj-") or k.startswith("sk-proj-")):
        raise SystemExit("OPENAI_API_KEY 포맷이 올바르지 않습니다. sk- 또는 sk-proj- 로 시작하는 키를 사용하세요.")
    # 내부 공백/따옴표는 허용하지 않음 (둘러싼 따옴표는 위에서 제거됨)
    for bad in ['"', "'", "`", " "]:
        if bad in k:
            raise SystemExit("OPENAI_API_KEY 값에 내부 따옴표/공백이 포함되어 있습니다. .env에서 제거 후 다시 실행하세요.")

_check_key_format(OPENAI_API_KEY)

# sk-proj- 키는 프로젝트 ID가 필수
if OPENAI_API_KEY.startswith("sk-proj-") and not OPENAI_PROJECT_ID:
    print("경고: sk-proj- 키를 사용 중이지만 OPENAI_PROJECT_ID가 설정되지 않았습니다.")
    print("OpenAI 대시보드에서 프로젝트 ID를 확인하여 .env에 추가하세요.")
    print("예: OPENAI_PROJECT_ID=proj_xxxxxxxxxxxx\n")

# OpenAI 클라이언트 초기화 (환경 프록시 무시 기본, 명시적 OPENAI_PROXY가 있을 때만 사용)
USE_HTTPX_TRUST_ENV = False
if OPENAI_PROXY:
    # 명시적 프록시가 설정된 경우 환경변수로 전달하고 trust_env 활성화
    os.environ["HTTPS_PROXY"] = OPENAI_PROXY
    os.environ["HTTP_PROXY"] = OPENAI_PROXY
    USE_HTTPX_TRUST_ENV = True

# httpx 버전에 따라 trust_env 지원 여부 처리
# 긴 영상(1시간 이상)의 STT 처리를 위해 타임아웃을 6300초(1시간 45분)로 설정
try:
    http_client = httpx.Client(timeout=6300.0, trust_env=USE_HTTPX_TRUST_ENV)
except TypeError:
    # 일부 오래된/특정 버전은 trust_env 인자를 지원하지 않음
    http_client = httpx.Client(timeout=6300.0)

openai_client = OpenAI(
    api_key=OPENAI_API_KEY,
    base_url=OPENAI_BASE_URL or None,
    organization=OPENAI_ORG_ID or None,
    project=OPENAI_PROJECT_ID or None,
    http_client=http_client,
)

def validate_openai_credentials():
    """키 유효성을 빠르게 점검합니다. 잘못된 키면 즉시 종료."""
    try:
        base = OPENAI_BASE_URL or "https://api.openai.com/v1 (default)"
        proxy = OPENAI_PROXY or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or "none"
        # 로드된 .env 경로 정보가 있으면 표시
        try:
            loaded_info = loaded_env_path if isinstance(loaded_env_path, str) else str(loaded_env_path)
        except NameError:
            loaded_info = "unknown"
        # 간단한 호출로 인증 검증
        openai_client.models.list()
        print(f"OpenAI 키 확인 완료: {_mask_key(OPENAI_API_KEY)} | base_url={base} | proxy={proxy} | trust_env={'on' if USE_HTTPX_TRUST_ENV else 'off'} | .env={loaded_info}")
    except Exception as e:
        msg = str(e)
        # 가능한 경우 예외에서 상태코드 확인
        status_code = getattr(e, "status_code", None)
        if status_code is None and hasattr(e, "response") and getattr(e.response, "status_code", None):
            status_code = getattr(e.response, "status_code")
        if status_code == 401 or "invalid_api_key" in msg or "status': 401" in msg or "Incorrect API key provided" in msg or "HTTP status code: 401" in msg:
            print(f"OpenAI 인증 실패(401). 현재 키: {_mask_key(OPENAI_API_KEY)} | base_url={OPENAI_BASE_URL or 'default'} | proxy={OPENAI_PROXY or 'none'} | trust_env={'on' if USE_HTTPX_TRUST_ENV else 'off'}")
            print(f"\n=== 401 오류 해결 체크리스트 ===")
            print(f"1. .env 파일이 올바른 위치에 있는지 확인: {loaded_env_path}")
            print(f"2. .env에서 OPENAI_API_KEY 값 확인 (위 출력된 전체 키 확인)")
            print(f"3. OpenAI 대시보드(https://platform.openai.com/api-keys)에서:")
            print(f"   - 키가 활성 상태인지 확인")
            print(f"   - 사용 한도(Usage limits)가 설정되어 있는지 확인")
            print(f"   - 결제 정보가 등록되어 있는지 확인")
            if OPENAI_API_KEY.startswith("sk-proj-"):
                print(f"4. 프로젝트 키(sk-proj-)를 사용 중이므로 OPENAI_PROJECT_ID 필수:")
                print(f"   현재 값: {OPENAI_PROJECT_ID or '(설정 안 됨)'}")
                print(f"   대시보드 > Settings > General에서 Project ID 확인")
            print(f"5. 시스템 프록시 설정 제거 후 재시도")
            print(f"6. 다른 네트워크(모바일 핫스팟 등)에서 시도")
            print(f"================================\n")
            print("- .env 파일 경로/로딩 확인 (스크립트 폴더의 .env 사용)")
            print("- 키에 공백/줄바꿈/따옴표가 포함되어 있지 않은지 확인")
            print("- 필요 시 OPENAI_PROXY를 설정, 없다면 시스템 프록시 제거")
            print("- 최신 openai/httpx로 업데이트 권장 (pip install -U openai httpx)")
            raise SystemExit(1)
        raise

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
            'preferredquality': '32',  # STT 전용으로 32kbps로 설정 (최대 압축)
        }],
        'postprocessor_args': [
            '-ar', '8000',   # 샘플레이트 8kHz로 낮춤 (음성 인식에 충분, 파일 크기 최소화)
            '-ac', '1',      # 모노로 변환 (STT에는 스테레오 불필요)
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

def split_audio_file(audio_path: str, chunk_duration_minutes: int = 10) -> list:
    """오디오 파일을 여러 청크로 분할합니다. (pydub 사용)"""
    try:
        from pydub import AudioSegment
    except ImportError:
        raise RuntimeError("pydub 패키지가 필요합니다. 설치: pip install pydub")
    
    audio = AudioSegment.from_mp3(audio_path)
    chunk_length_ms = chunk_duration_minutes * 60 * 1000  # 분을 밀리초로 변환
    
    chunks = []
    for i in range(0, len(audio), chunk_length_ms):
        chunk = audio[i:i + chunk_length_ms]
        chunk_path = f"{audio_path}_chunk_{i//chunk_length_ms}.mp3"
        chunk.export(chunk_path, format="mp3", bitrate="32k", parameters=["-ar", "8000", "-ac", "1"])
        chunks.append(chunk_path)
    
    return chunks

def transcribe_audio(audio_path: str) -> str:
    """OpenAI Whisper API를 사용하여 오디오를 텍스트로 변환합니다."""
    # Whisper API는 최대 25MB 파일만 지원
    file_size_mb = os.path.getsize(audio_path) / (1024 * 1024)
    
    # 25MB 초과 시 자동으로 분할 처리
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
                # 청크 파일 삭제
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)
            
            # 청크 간 짧은 대기
            if idx < len(chunk_files):
                time.sleep(1)
        
        print(f"  - 모든 청크 처리 완료, 텍스트 결합 중...")
        return " ".join(transcripts)
    
    # 25MB 이하는 일반 처리
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
            if "invalid_api_key" in msg or "status': 401" in msg or "Incorrect API key provided" in msg or "HTTP status code: 401" in msg:
                raise RuntimeError("OpenAI 401: API 키가 올바르지 않거나 프록시로 인해 손상되었습니다.")
            raise
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
    # OpenAI 인증을 먼저 검증하여 대량 처리 전에 즉시 실패
    validate_openai_credentials()

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
                # 키 오류면 추가 시도 의미 없으므로 중단
                if "401" in str(e) or "invalid_api_key" in str(e):
                    print("  - 인증 오류로 작업을 중단합니다.")
                    break
                # 다음 요청 전에 잠시 대기하여 차단/레이트리밋을 피합니다.
                time.sleep(random.uniform(YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX))
                continue

            # 각 영상 사이에도 짧게 대기
            time.sleep(random.uniform(YTDLP_SLEEP_MIN, YTDLP_SLEEP_MAX))

    print("모든 영상 처리 완료!")

if __name__ == "__main__":
    main()
