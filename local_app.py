"""
챗봇 만족도 분석 — 로컬 전용 Streamlit 앱
실행: 05_run_local_analysis.bat (localhost:8502)

[동작]
1. 엑셀 파일 업로드
2. 로컬에서 LLM 분석 실행 (기존 UI/진행상태 동일)
3. 분석 완료 시 S3 자동 업로드
4. EC2 대시보드(app.py)에서 새로고침하면 결과 확인 가능
"""

import os
import threading
import uuid

import streamlit as st
import pandas as pd

from core.evaluator import (
    run_pipeline,
    normalize_schema,
    parse_filename_meta,
    load_all_history,
    save_to_history,
    is_already_analyzed,
)
from core.llm_client import DEFAULT_API_KEY, DEFAULT_MAX_WORKERS
from core.categories import SESSION_COL, TIMESTAMP_COL
from core.pipeline_state import REGISTRY as _PIPELINE_REGISTRY


# ── 페이지 설정 ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="만족도 분석 (로컬)",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    "<style>[data-testid='stToolbar'] { display: none !important; }</style>",
    unsafe_allow_html=True,
)

# ── 세션 상태 초기화 ─────────────────────────────────────────────────────────
if "pipeline_id" not in st.session_state:
    st.session_state["pipeline_id"] = None

if "uploader_key" not in st.session_state:
    st.session_state["uploader_key"] = 0

if "completed_slug" not in st.session_state:
    st.session_state["completed_slug"] = None


# ── 파이프라인 제어 ──────────────────────────────────────────────────────────

def _start_pipeline(df: pd.DataFrame, meta: dict, use_llm: bool,
                    api_key: str, max_workers: int):
    pid = str(uuid.uuid4())
    stop_event  = threading.Event()
    pause_event = threading.Event()
    pause_event.set()

    _PIPELINE_REGISTRY[pid] = {
        "state":       "running",
        "step":        "utterance",
        "current":     0,
        "total":       1,
        "message":     "파이프라인 시작 중...",
        "stop_event":  stop_event,
        "pause_event": pause_event,
        "result":      None,
        "error":       None,
        "meta":        meta,
    }
    st.session_state["pipeline_id"] = pid

    def _worker():
        reg = _PIPELINE_REGISTRY[pid]

        def _on_progress(step: str, current: int, total: int, message: str = ""):
            reg["step"]    = step
            reg["current"] = current
            reg["total"]   = max(total, 1)
            reg["message"] = message

        try:
            result = run_pipeline(
                df,
                use_llm=use_llm,
                api_key=api_key,
                max_workers=max_workers,
                progress_callback=_on_progress,
                stop_event=stop_event,
                pause_event=pause_event,
            )
            if result is None:
                reg["state"] = "stopped"
            else:
                reg["result"] = result
                reg["state"]  = "done"
        except Exception as e:
            reg["error"] = str(e)
            reg["state"] = "error"

    threading.Thread(target=_worker, daemon=True).start()
    return pid


def _pause_pipeline(pid: str):
    reg = _PIPELINE_REGISTRY.get(pid)
    if reg and reg["state"] == "running":
        reg["pause_event"].clear()
        reg["state"] = "paused"


def _resume_pipeline(pid: str):
    reg = _PIPELINE_REGISTRY.get(pid)
    if reg and reg["state"] == "paused":
        reg["pause_event"].set()
        reg["state"] = "running"


def _stop_pipeline(pid: str):
    reg = _PIPELINE_REGISTRY.get(pid)
    if reg and reg["state"] in ("running", "paused"):
        reg["pause_event"].set()
        reg["stop_event"].set()
        reg["state"] = "stopped"


# ── 진행률 계산 ─────────────────────────────────────────────────────────────

_STEP_LABELS  = {
    "utterance": "1단계: 발화 분석",
    "sessions":  "2단계: 세션 평가 (LLM)",
    "synthesis": "3단계: LLM 종합 분석",
    "done":      "완료",
}
_STEP_WEIGHTS = {"utterance": 0.05, "sessions": 0.85, "synthesis": 0.10}


def _compute_overall_progress(reg: dict) -> float:
    step    = reg.get("step", "utterance")
    current = reg.get("current", 0)
    total   = max(reg.get("total", 1), 1)
    if step == "utterance":
        return (current / total) * _STEP_WEIGHTS["utterance"]
    if step == "sessions":
        return _STEP_WEIGHTS["utterance"] + (current / total) * _STEP_WEIGHTS["sessions"]
    if step == "synthesis":
        return _STEP_WEIGHTS["utterance"] + _STEP_WEIGHTS["sessions"] + (current / total) * _STEP_WEIGHTS["synthesis"]
    return 1.0


# ── 파이프라인 모니터 (0.5초 갱신) ──────────────────────────────────────────

@st.fragment(run_every="0.5s")
def _pipeline_monitor_fragment():
    pid = st.session_state.get("pipeline_id")
    if not pid:
        return

    p_reg = _PIPELINE_REGISTRY.get(pid, {})
    state = p_reg.get("state")

    # ── 실행 중 / 일시정지 ───────────────────────────────────────────────────
    if state in ("running", "paused"):
        overall    = _compute_overall_progress(p_reg)
        step_label = _STEP_LABELS.get(p_reg.get("step", ""), "")
        pct_text   = f"{int(overall * 100)}%"
        bar_text   = (
            f"⏸ 일시정지 — {step_label} ({pct_text})"
            if state == "paused"
            else f"🔄 {step_label} ({pct_text})"
        )

        st.markdown("#### 분석 진행 현황")
        st.progress(min(overall, 1.0), text=bar_text)
        if p_reg.get("message"):
            st.caption(p_reg["message"])

        col_pause, col_reset = st.columns([1, 1])
        with col_pause:
            if state == "running":
                if st.button("⏸ 일시정지", key=f"btn_pause_{pid}", use_container_width=True):
                    _pause_pipeline(pid)
                    st.rerun()
            else:
                if st.button("▶ 재개", key=f"btn_resume_{pid}",
                             use_container_width=True, type="primary"):
                    _resume_pipeline(pid)
                    st.rerun()
        with col_reset:
            if st.button("🔄 초기화 (파일 교체)", key=f"btn_reset_{pid}",
                         use_container_width=True):
                _stop_pipeline(pid)
                st.session_state["uploader_key"] += 1
                st.rerun(scope="app")

    # ── 완료 ────────────────────────────────────────────────────────────────
    elif state == "done":
        result = p_reg.get("result")
        p_meta = p_reg.get("meta", {})

        if result is not None and not p_reg.get("saved"):
            # S3 자동 업로드
            try:
                save_to_history(result, p_meta)
                p_reg["s3_ok"] = True
            except Exception as e:
                p_reg["s3_error"] = str(e)

            p_reg["saved"]  = True
            p_reg["result"] = None  # DataFrame 메모리 해제
            st.session_state["completed_slug"] = p_meta.get("slug", "")
            st.session_state["uploader_key"]   = st.session_state.get("uploader_key", 0) + 1
            st.rerun(scope="app")

        date_label = p_meta.get("date_label") or p_meta.get("slug", "")
        st.success(f"분석 완료! ({date_label})", icon="✅")

        if p_reg.get("s3_ok"):
            st.info("S3 업로드 완료 — EC2 대시보드에서 새로고침하면 결과를 확인할 수 있습니다.")
        elif p_reg.get("s3_error"):
            st.warning(f"S3 업로드 실패: {p_reg['s3_error']}")

        if st.button("새 파일 분석하기", key="btn_done_reset", type="primary"):
            _PIPELINE_REGISTRY.pop(pid, None)
            st.session_state["pipeline_id"] = None
            st.session_state["uploader_key"] = st.session_state.get("uploader_key", 0) + 1
            st.rerun(scope="app")

    # ── 오류 ────────────────────────────────────────────────────────────────
    elif state == "error":
        st.error(f"분석 중 오류 발생: {p_reg.get('error', '')}")
        if st.button("닫기", key="btn_error_close"):
            _PIPELINE_REGISTRY.pop(pid, None)
            st.session_state["pipeline_id"] = None
            st.rerun(scope="app")

    # ── 중단됨 ───────────────────────────────────────────────────────────────
    elif state == "stopped":
        st.info("분석이 중단되었습니다. 새 파일을 업로드하여 다시 시작하세요.")
        _PIPELINE_REGISTRY.pop(pid, None)
        st.session_state["pipeline_id"] = None


# ── 사이드바 ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🔬 만족도 분석")
    st.caption("로컬 전용 — 분석 후 S3 자동 업로드")
    st.divider()
    st.markdown("**EC2 대시보드**")
    st.markdown("http://52.65.104.159:8501/")
    st.divider()
    st.caption("분석 완료 후 EC2 대시보드에서 새로고침하면 결과를 확인할 수 있습니다.")


# ── 히스토리 로드 (중복 판단용) ──────────────────────────────────────────────
all_history = load_all_history()
has_history = len(all_history) > 0

_cur_pid = st.session_state.get("pipeline_id")
if _cur_pid and _PIPELINE_REGISTRY.get(_cur_pid, {}).get("state") == "stopped":
    _PIPELINE_REGISTRY.pop(_cur_pid, None)
    st.session_state["pipeline_id"] = None


# ── 메인 화면 ────────────────────────────────────────────────────────────────
st.title("🔬 챗봇 만족도 분석 (로컬)")

st.subheader("파일 업로드 / 분석")
st.markdown(
    "주간 챗봇 대화 파일을 업로드하면 만족도 분석을 시작합니다.  \n"
    "**분석 완료 후 S3에 자동으로 업로드됩니다.**"
)

with st.expander("파일명 규칙 안내", expanded=not has_history):
    st.markdown("""
| 파일명 예시 | 기간 표시 |
|-------------|-----------|
| `chatbot_0413-0424.xlsx` | `04.13~04.24` |
| `skt_log_0421-0427.xlsx` | `04.21~04.27` |
| `data_week17.xlsx`       | 파일명 그대로 |

- **동일 날짜 범위** 재업로드 → 기존 S3 데이터 **덮어쓰기**
- **다른 날짜 범위** 업로드 → **새 주차**로 별도 저장
    """)

st.divider()

uploaded_file = st.file_uploader(
    "주간 대화 파일 업로드 (.xlsx / .csv)",
    type=["xlsx", "csv"],
    key=f"uploader_{st.session_state['uploader_key']}",
)

upload_meta   = None
upload_slug   = None
upload_label  = None
is_duplicate  = False
existing_slug = None
run_btn       = False

if uploaded_file is not None:
    upload_meta  = parse_filename_meta(uploaded_file.name)
    upload_slug  = upload_meta["slug"]
    upload_label = upload_meta["date_label"] or upload_slug
    upload_start = upload_meta.get("start", "")
    upload_end   = upload_meta.get("end", "")

    week_slugs = [r["slug"] for r in all_history] if has_history else []

    if upload_start and upload_end and has_history:
        _matching = next(
            (r for r in all_history
             if r.get("start") == upload_start and r.get("end") == upload_end),
            None,
        )
        is_duplicate  = _matching is not None
        existing_slug = _matching["slug"] if _matching else upload_slug
    else:
        is_duplicate  = upload_slug in week_slugs
        existing_slug = upload_slug

    _effective_slug = existing_slug if is_duplicate else upload_slug

    if _effective_slug == st.session_state.get("completed_slug"):
        st.success("분석 및 S3 업로드가 완료되었습니다.", icon="✅")
    elif is_duplicate:
        st.warning(
            f"**'{upload_label}'** 데이터가 S3에 이미 존재합니다.\n\n"
            "아래 버튼을 누르면 기존 데이터를 **덮어씁니다.**",
            icon="⚠️",
        )
        run_btn = st.button("🔄 재분석하여 업데이트", type="primary")
    else:
        st.info(f"**'{upload_label}'** 신규 분석을 시작합니다.")
        run_btn = st.button("▶ 분석 시작", type="primary")

st.divider()

# ── LLM 설정 ─────────────────────────────────────────────────────────────────
st.markdown("**LLM 설정**")
col1, col2, col3 = st.columns([1, 2, 1])
with col1:
    use_llm = st.toggle("LLM 분석 사용", value=True, key="use_llm_toggle")
with col2:
    api_key_input = st.text_input(
        "API Key (비어 있으면 기본값 사용)",
        type="password",
        value="",
        key="api_key_input",
    )
with col3:
    max_workers = st.slider(
        "병렬 처리 수",
        min_value=10, max_value=100,
        value=DEFAULT_MAX_WORKERS, step=10,
        key="max_workers_slider",
    )

# ── 파이프라인 시작 ───────────────────────────────────────────────────────────
if uploaded_file is not None and run_btn:
    if is_already_analyzed(upload_slug) and not is_duplicate:
        st.toast("이미 분석된 결과가 S3에 있습니다.", icon="ℹ️")
    else:
        existing_pid = st.session_state.get("pipeline_id")
        if existing_pid and _PIPELINE_REGISTRY.get(existing_pid, {}).get("state") in ("running", "paused"):
            _stop_pipeline(existing_pid)

        ext = os.path.splitext(uploaded_file.name)[1].lower()
        try:
            df_raw = pd.read_csv(uploaded_file) if ext == ".csv" else pd.read_excel(uploaded_file)
            df_raw = normalize_schema(df_raw)
            df_raw = df_raw.sort_values([SESSION_COL, TIMESTAMP_COL]).reset_index(drop=True)
        except Exception as e:
            st.error(f"파일 로딩 실패: {e}")
            st.stop()

        api_key        = api_key_input.strip() or DEFAULT_API_KEY
        effective_meta = upload_meta.copy()
        if is_duplicate and existing_slug:
            effective_meta["slug"] = existing_slug

        _start_pipeline(df_raw, effective_meta, use_llm, api_key, max_workers)
        st.rerun()

# ── 진행 모니터 ───────────────────────────────────────────────────────────────
if st.session_state.get("pipeline_id"):
    st.divider()
    _pipeline_monitor_fragment()
