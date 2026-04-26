import os

MODEL_NAME       = "azure/openai/gpt-5-mini-2025-08-07-gs"
TARGET_BASE_URL  = "https://api.platform.a15t.com/v1"
DEFAULT_API_KEY  = "sk-gapk-LCvZ5c5rpB9mGJrfhxf1mN2eO-gzwHra"
DEFAULT_MAX_WORKERS = 50

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # type: ignore


def init_client(api_key: str = "", use_llm: bool = True):
    """LLM 클라이언트 초기화. (client, model_name) 반환 — 실패 시 (None, model_name)."""
    if not use_llm:
        return None, MODEL_NAME

    if OpenAI is None:
        print("[Error] openai 라이브러리가 없습니다. pip install openai")
        return None, MODEL_NAME

    key = api_key or DEFAULT_API_KEY or os.getenv("OPENAI_API_KEY", "")
    if not key:
        print("[Warning] API Key가 설정되지 않아 LLM 기능이 비활성화됩니다.")
        return None, MODEL_NAME

    try:
        client = OpenAI(api_key=key, base_url=TARGET_BASE_URL)
        print(f"[Info] LLM 클라이언트 초기화 완료 (model={MODEL_NAME})")
        return client, MODEL_NAME
    except Exception as e:
        print(f"[Error] LLM 초기화 실패: {e}")
        return None, MODEL_NAME
