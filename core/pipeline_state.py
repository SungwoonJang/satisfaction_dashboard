"""
파이프라인 실행 상태 레지스트리.

이 모듈은 Python 프로세스 내에서 한 번만 임포트되므로,
Streamlit rerun 사이에도 REGISTRY 딕셔너리가 초기화되지 않는다.

키 구조 per pipeline_id:
    "state"       : "running" | "paused" | "stopped" | "done" | "error"
    "step"        : str   — 현재 단계명
    "current"     : int   — 현재 진행 수
    "total"       : int   — 전체 수
    "message"     : str   — 표시 메시지
    "stop_event"  : threading.Event
    "pause_event" : threading.Event  (set=실행중, clear=일시정지)
    "result"      : dict | None      — 완료 후 결과
    "error"       : str | None       — 에러 메시지
    "meta"        : dict             — parse_filename_meta() 반환값
"""

REGISTRY: dict = {}
