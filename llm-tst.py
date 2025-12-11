from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
import os

key = os.getenv("OPENAI_API_KEY")
print("KEY LEN =", len(key) if key else None)

client = OpenAI(api_key=key)

resp = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "한 단어로만 대답해: OK"}],
)

print(resp.choices[0].message.content)
