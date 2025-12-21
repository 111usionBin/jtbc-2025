"""OpenAI API 키 테스트 스크립트"""
import os
from dotenv import load_dotenv
from pathlib import Path

# .env 파일 로드
script_env = Path(__file__).with_name(".env")
if script_env.exists():
    load_dotenv(dotenv_path=script_env)
else:
    load_dotenv()

# 환경 변수 로드
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_PROJECT_ID = os.getenv("OPENAI_PROJECT_ID", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "").strip()

print(f"API Key (앞 10자): {OPENAI_API_KEY[:10]}...")
print(f"Project ID: {OPENAI_PROJECT_ID}")
print(f"Base URL: {OPENAI_BASE_URL or 'default'}")
print()

# 테스트 1: 기본 OpenAI 클라이언트
print("=== 테스트 1: 기본 설정으로 연결 ===")
try:
    from openai import OpenAI
    client = OpenAI(
        api_key=OPENAI_API_KEY,
        project=OPENAI_PROJECT_ID or None,
    )
    response = client.models.list()
    print(f"✅ 성공! 사용 가능한 모델 수: {len(response.data)}")
    print(f"첫 번째 모델: {response.data[0].id if response.data else 'N/A'}")
except Exception as e:
    print(f"❌ 실패: {e}")

print()

# 테스트 2: 명시적 base_url 없이
print("=== 테스트 2: base_url 없이 연결 ===")
try:
    from openai import OpenAI
    client = OpenAI(
        api_key=OPENAI_API_KEY,
        project=OPENAI_PROJECT_ID or None,
    )
    response = client.models.list()
    print(f"✅ 성공! 사용 가능한 모델 수: {len(response.data)}")
except Exception as e:
    print(f"❌ 실패: {e}")

print()

# 테스트 3: 간단한 채팅 완성 테스트 (비용 발생 가능)
print("=== 테스트 3: 간단한 API 호출 (1센트 미만) ===")
try:
    from openai import OpenAI
    client = OpenAI(
        api_key=OPENAI_API_KEY,
        project=OPENAI_PROJECT_ID or None,
    )
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": "Say 'test'"}],
        max_tokens=5
    )
    print(f"✅ 성공! 응답: {response.choices[0].message.content}")
except Exception as e:
    print(f"❌ 실패: {e}")

print()
print("=== 체크리스트 ===")
print("1. OpenAI 대시보드 (https://platform.openai.com/settings/organization/billing) 에서 결제 정보 확인")
print("2. Usage limits (https://platform.openai.com/settings/organization/limits) 확인")
print("3. API keys (https://platform.openai.com/api-keys) 에서:")
print("   - 키 상태가 'Active'인지 확인")
print("   - Permissions가 'All'로 설정되어 있는지 확인 (Restricted가 아닌)")
print("   - 프로젝트 키라면 해당 프로젝트에 권한이 있는지 확인")
print("4. 신규 계정의 경우 $5 최소 충전이 필요할 수 있음")
