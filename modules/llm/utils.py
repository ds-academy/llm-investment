import json
from datetime import datetime


LLAMA_PROMPT = """

"""

GPT_PROMPT = """

"""


def format_recent_chat_history(chat_history, n=5):
    # 시간순으로 정렬 (타임스탬프가 문자열인 경우를 대비해 datetime 객체로 변환)
    sorted_history = sorted(
        chat_history,
        key=lambda x: (
            datetime.fromisoformat(x["timestamp"])
            if isinstance(x["timestamp"], str)
            else x["timestamp"]
        ),
    )

    # 최근 N개 선택
    recent_history = sorted_history[-n:]

    # ChatGPT API 형식으로 변환
    formatted_messages = []
    for entry in recent_history:
        if entry["speaker"] == "user":
            role = "user"
        elif entry["speaker"] in ["llama", "gpt"]:
            role = "assistant"
        else:
            print(f"Warning: Unknown speaker type: {entry['speaker']}")
            continue  # 알 수 없는 speaker는 무시

        formatted_messages.append({"role": role, "content": entry["message"]})

    return formatted_messages
