from openai import OpenAI
import os

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

resp = client.responses.create(
    model="gpt-4.1-mini",
    input="안녕, 테스트야"
)

print(resp.output_text)
