"""
챗봇 만족도 대시보드 — Streamlit 진입점
실행: streamlit run app.py  (또는 02_run_dashboard.bat 더블클릭)

[데이터 흐름]
- 분석 결과는 S3(jsw-aws-s3-work/satisfaction_history/{slug}.json + {slug}.pkl)에 영구 저장됨
- 앱 재실행 시 디스크에서 자동 로드 → 파일 재업로드 불필요
- 동일 파일명 재업로드 → 기존 데이터 덮어쓰기(업데이트)
- 다른 파일명 업로드 → 새 주차로 별도 저장

[탭 구조]
- 📁 파일 업로드 / 분석: 파일 업로드, LLM 설정, 분석 진행 현황 모니터링
- 📊 주간 현황 / 📋 도메인 분석 / 💡 인사이트: 분석 결과 대시보드
  → 분석 진행 중에도 대시보드 탭에 자유롭게 접근 가능
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
    load_history_by_slug,
    save_to_history,
    is_already_analyzed,
)
from core.llm_client import DEFAULT_API_KEY, DEFAULT_MAX_WORKERS
from core.categories import SESSION_COL, TIMESTAMP_COL
from core.pipeline_state import REGISTRY as _PIPELINE_REGISTRY
from views.tab_overview import render_overview
from views.tab_domain import render_domain
from views.tab_insight import render_insight

# ── 필수 디렉토리 보장 ──────────────────────────────────────────────────────
os.makedirs("data/uploads", exist_ok=True)
os.makedirs("input",        exist_ok=True)
os.makedirs("output",       exist_ok=True)


# ── 페이지 설정 ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="챗봇 만족도 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Streamlit 기본 툴바(Stop / Deploy / ⋮ 메뉴) 숨김
st.markdown(
    "<style>[data-testid='stToolbar'] { display: none !important; }</style>",
    unsafe_allow_html=True,
)

# ── 세션 상태 초기화 ─────────────────────────────────────────────────────────
if "result_cache" not in st.session_state:
    st.session_state["result_cache"] = {}

if "pipeline_id" not in st.session_state:
    st.session_state["pipeline_id"] = None

if "uploader_key" not in st.session_state:
    st.session_state["uploader_key"] = 0

if "last_analyzed_slug" not in st.session_state:
    st.session_state["last_analyzed_slug"] = None

if "completed_slug" not in st.session_state:
    st.session_state["completed_slug"] = None


# ── 헬퍼: 결과 캐시 ─────────────────────────────────────────────────────────

def _get_result(slug: str) -> dict | None:
    """slug에 해당하는 결과를 메모리 캐시 → 디스크 순으로 로드."""
    if slug in st.session_state["result_cache"]:
        return st.session_state["result_cache"][slug]
    result = load_history_by_slug(slug)
    if result:
        st.session_state["result_cache"][slug] = result
    return result


def _save_and_cache(result: dict, meta: dict):
    """분석 결과를 디스크에 저장하고 메모리 캐시도 갱신."""
    save_to_history(result, meta)
    st.session_state["result_cache"][meta["slug"]] = result


# ── 파이프라인 제어 함수 ────────────────────────────────────────────────────

def _start_pipeline(df: pd.DataFrame, meta: dict, use_llm: bool,
                    api_key: str, max_workers: int):
    """백그라운드 스레드에서 run_pipeline()을 실행한다."""
    pid = str(uuid.uuid4())
    stop_event  = threading.Event()
    pause_event = threading.Event()
    pause_event.set()  # set 상태 = 실행 중

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
        "use_llm":     use_llm,
        "api_key":     api_key,
        "max_workers": max_workers,
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

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
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
        reg["pause_event"].set()   # pause 상태면 루프가 stop_event를 볼 수 있도록
        reg["stop_event"].set()
        reg["state"] = "stopped"


# ── 진행률 계산 ─────────────────────────────────────────────────────────────

_STEP_LABELS = {
    "utterance": "1단계: 발화 분석",
    "sessions":  "2단계: 세션 평가 (LLM)",
    "synthesis": "3단계: LLM 종합 분석",
    "done":      "완료",
}

_STEP_WEIGHTS = {"utterance": 0.05, "sessions": 0.85, "synthesis": 0.10}


def _compute_overall_progress(reg: dict) -> float:
    """전체 진행률(0.0~1.0)을 단계 가중치로 계산한다."""
    step    = reg.get("step", "utterance")
    current = reg.get("current", 0)
    total   = max(reg.get("total", 1), 1)

    if step == "utterance":
        return (current / total) * _STEP_WEIGHTS["utterance"]
    if step == "sessions":
        base = _STEP_WEIGHTS["utterance"]
        return base + (current / total) * _STEP_WEIGHTS["sessions"]
    if step == "synthesis":
        base = _STEP_WEIGHTS["utterance"] + _STEP_WEIGHTS["sessions"]
        return base + (current / total) * _STEP_WEIGHTS["synthesis"]
    return 1.0


# ── 파이프라인 진행 모니터 (fragment — 0.5초마다 자동 갱신) ─────────────────
# @st.fragment(run_every="0.5s") 는 이 함수만 0.5초 간격으로 재실행한다.
# 다른 탭은 영향을 받지 않으므로 분석 중에도 대시보드 탭을 자유롭게 이용 가능.

@st.fragment(run_every="0.5s")
def _pipeline_monitor_fragment():
    """분석 진행 현황을 0.5초 간격으로 갱신하는 격리된 fragment."""
    pid = st.session_state.get("pipeline_id")
    if not pid:
        return

    p_reg = _PIPELINE_REGISTRY.get(pid, {})
    state = p_reg.get("state")

    # ── 실행 중 / 일시정지 ───────────────────────────────────────────────────
    if state in ("running", "paused"):
        step    = p_reg.get("step", "utterance")
        message = p_reg.get("message", "")
        overall = _compute_overall_progress(p_reg)

        step_label = _STEP_LABELS.get(step, step)
        pct_text   = f"{int(overall * 100)}%"
        bar_text   = (
            f"⏸ 일시정지 — {step_label} ({pct_text})"
            if state == "paused"
            else f"🔄 {step_label} ({pct_text})"
        )

        st.markdown("#### 분석 진행 현황")
        st.progress(min(overall, 1.0), text=bar_text)
        if message:
            st.caption(message)

        col_pause, col_reset = st.columns([1, 1])
        with col_pause:
            if state == "running":
                if st.button("⏸ 일시정지", key=f"btn_pause_{pid}", use_container_width=True):
                    _pause_pipeline(pid)
                    st.rerun()  # fragment만 재실행
            else:
                if st.button("▶ 재개", key=f"btn_resume_{pid}",
                             use_container_width=True, type="primary"):
                    _resume_pipeline(pid)
                    st.rerun()  # fragment만 재실행

        with col_reset:
            if st.button("🔄 초기화 (파일 교체)", key=f"btn_reset_{pid}",
                         use_container_width=True):
                _stop_pipeline(pid)
                st.session_state["uploader_key"] += 1
                st.rerun(scope="app")  # 파일 업로더 위젯 초기화를 위해 전체 앱 재실행

    # ── 완료 ────────────────────────────────────────────────────────────────
    elif state == "done":
        result = p_reg.get("result")
        p_meta = p_reg.get("meta", {})

        if result is not None and not p_reg.get("saved"):
            result["slug"]       = p_meta.get("slug", "")
            result["date_label"] = p_meta.get("date_label", "")
            _save_and_cache(result, p_meta)
            st.session_state["last_analyzed_slug"] = p_meta.get("slug", "")
            st.session_state["completed_slug"]      = p_meta.get("slug", "")
            p_reg["saved"] = True
            # 사이드바 주차 목록 갱신을 위해 전체 앱 1회 재실행
            st.rerun(scope="app")

        date_label = p_meta.get("date_label") or p_meta.get("slug", "")
        st.success(f"분석이 완료되었습니다! ({date_label})", icon="✅")
        st.info(
            "왼쪽 사이드바에서 해당 주차를 선택하면 "
            "**[📊 주간 현황]**, **[📋 도메인 분석]**, **[💡 인사이트]** 탭에서 결과를 확인할 수 있습니다."
        )
        if st.button("새 파일 분석하기 (초기화)", key="btn_done_reset", type="primary"):
            _PIPELINE_REGISTRY.pop(pid, None)
            st.session_state["pipeline_id"] = None
            st.session_state["uploader_key"] = st.session_state.get("uploader_key", 0) + 1
            st.rerun(scope="app")

    # ── 오류 ────────────────────────────────────────────────────────────────
    elif state == "error":
        st.error(f"분석 중 오류가 발생했습니다: {p_reg.get('error', '')}")
        if st.button("닫기", key="btn_error_close"):
            _PIPELINE_REGISTRY.pop(pid, None)
            st.session_state["pipeline_id"] = None
            st.rerun(scope="app")

    # ── 중단됨 ───────────────────────────────────────────────────────────────
    elif state == "stopped":
        st.info("분석이 중단되었습니다. 새 파일을 업로드하여 다시 시작하세요.")
        _PIPELINE_REGISTRY.pop(pid, None)
        st.session_state["pipeline_id"] = None


# ── 파일 업로드 / 분석 탭 콘텐츠 ─────────────────────────────────────────────

def _render_upload_section(all_history: list, has_history: bool):
    """파일 업로드 UI, LLM 설정, 파이프라인 시작 로직을 렌더링한다."""
    st.subheader("새 파일 분석 / 업데이트")
    st.markdown(
        "주간 챗봇 대화 파일을 업로드하면 만족도 분석을 시작합니다.  \n"
        "**분석이 진행 중에도 다른 탭에서 이전 결과를 자유롭게 조회할 수 있습니다.**"
    )

    with st.expander("파일명 규칙 안내", expanded=not has_history):
        st.markdown("""
| 파일명 예시 | 기간 표시 |
|-------------|-----------|
| `chatbot_0413-0424.xlsx` | `04.13~04.24` |
| `skt_log_0421-0427.xlsx` | `04.21~04.27` |
| `data_week17.xlsx`       | 파일명 그대로 |

- **동일 파일명** 재업로드 → 기존 분석 데이터 **업데이트**
- **다른 파일명** 업로드 → **새 주차** 데이터로 별도 저장
- 앱을 재실행해도 분석 이력은 S3(`jsw-aws-s3-work/satisfaction_history/`)에 영구 보존됩니다
        """)

    st.divider()

    uploaded_file = st.file_uploader(
        "주간 대화 파일 업로드 (.xlsx / .csv)",
        type=["xlsx", "csv"],
        help="동일 파일명 재업로드 시 기존 데이터를 업데이트합니다.",
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

        # 날짜 기반 중복 판단 — 파일명 접두사·slug 형식 차이에 무관하게 동작
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

        # 우선순위: 완료 표시 > 중복 경고 > 신규 안내
        if _effective_slug == st.session_state.get("completed_slug"):
            st.success("데이터 분석 및 업데이트가 완료되었어요.", icon="✅")
        elif is_duplicate:
            st.warning(
                f"**'{upload_label}'** 데이터가 이미 존재합니다.\n\n"
                "아래 버튼을 누르면 기존 데이터를 **덮어씁니다.**",
                icon="⚠️",
            )
            run_btn = st.button("🔄 재분석하여 업데이트", type="primary")
        else:
            st.info(f"**'{upload_label}'** 신규 분석을 시작합니다.")
            run_btn = st.button("▶ 분석 시작", type="primary")

    st.divider()

    # ── LLM 설정 ─────────────────────────────────────────────────────────────
    st.markdown("**LLM 설정**")
    col_llm1, col_llm2, col_llm3 = st.columns([1, 2, 1])
    with col_llm1:
        use_llm = st.toggle("LLM 분석 사용", value=True, key="use_llm_toggle")
    with col_llm2:
        api_key_input = st.text_input(
            "API Key (비어 있으면 기본값 사용)",
            type="password",
            value="",
            key="api_key_input",
        )
    with col_llm3:
        max_workers = st.slider(
            "병렬 처리 수",
            min_value=10, max_value=100,
            value=DEFAULT_MAX_WORKERS, step=10,
            key="max_workers_slider",
        )

    # ── 파이프라인 시작 ───────────────────────────────────────────────────────
    if uploaded_file is not None and run_btn:
        if is_already_analyzed(upload_slug) and not is_duplicate:
            st.toast("이미 분석된 결과가 있습니다. 캐시에서 로드합니다.", icon="ℹ️")
            return

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
            return

        api_key = api_key_input.strip() or DEFAULT_API_KEY
        effective_meta = upload_meta.copy()
        if is_duplicate and existing_slug:
            effective_meta["slug"] = existing_slug
        _start_pipeline(df_raw, effective_meta, use_llm, api_key, max_workers)
        st.rerun()


# ── 히스토리 로드 및 stopped 상태 정리 ────────────────────────────────────
all_history = load_all_history()
has_history = len(all_history) > 0

_cur_pid = st.session_state.get("pipeline_id")
if _cur_pid:
    _cur_state = _PIPELINE_REGISTRY.get(_cur_pid, {}).get("state")
    if _cur_state == "stopped":
        _PIPELINE_REGISTRY.pop(_cur_pid, None)
        st.session_state["pipeline_id"] = None


# ── 사이드바 (주차 선택) ─────────────────────────────────────────────────────
selected_slug  = None
selected_label = "—"

with st.sidebar:
    st.title("📊 챗봇 만족도")
    st.caption("SKT 챗봇 고객 대화 만족도 분석")
    st.divider()

    if has_history:
        st.markdown("**분석 주차 선택**")
        week_slugs  = [r["slug"]                        for r in all_history]
        week_labels = [r.get("date_label") or r["slug"] for r in all_history]

        default_idx = len(week_slugs) - 1
        if st.session_state.get("last_analyzed_slug") in week_slugs:
            default_idx = week_slugs.index(st.session_state["last_analyzed_slug"])

        sel_idx = st.selectbox(
            "조회할 주차를 선택하세요",
            options=range(len(week_slugs)),
            format_func=lambda i: week_labels[i],
            index=default_idx,
            key="week_selector",
        )
        selected_slug  = week_slugs[sel_idx]
        selected_label = week_labels[sel_idx]

        rec = all_history[sel_idx]
        if rec.get("uploaded_at"):
            uploaded_dt = rec["uploaded_at"][:16].replace("T", " ")
            st.caption(f"분석일시: {uploaded_dt}")
        st.divider()

    st.caption(
        "파일 업로드 및 분석 설정은  \n"
        "**[📁 파일 업로드 / 분석]** 탭을 이용하세요."
    )


# ── active_result 결정 (선택된 주차 기반) ────────────────────────────────────
active_result = None
active_slug   = None
active_label  = "—"

if selected_slug:
    active_result = _get_result(selected_slug)
    active_slug   = selected_slug
    active_label  = selected_label


# ── 대시보드 헤더 ─────────────────────────────────────────────────────────────
st.title("📊 챗봇 만족도 대시보드")

# 분석 진행 중 알림 배너 (어느 탭에서든 상단에 표시)
_active_pid   = st.session_state.get("pipeline_id")
_active_state = _PIPELINE_REGISTRY.get(_active_pid, {}).get("state") if _active_pid else None
if _active_state in ("running", "paused"):
    _icon = "⏸" if _active_state == "paused" else "🔄"
    st.info(
        f"{_icon} 파일 분석이 진행 중입니다. "
        "**[📁 파일 업로드 / 분석]** 탭에서 진행 현황을 확인하세요.",
        icon="ℹ️",
    )


# ── 4개 탭 레이아웃 (항상 렌더링) ────────────────────────────────────────────
tab_file, tab1, tab2, tab3 = st.tabs([
    "📁 파일 업로드 / 분석",
    "📊 주간 현황",
    "📋 도메인 분석",
    "💡 인사이트",
])

# ── 탭 0: 파일 업로드 / 분석 ─────────────────────────────────────────────────
with tab_file:
    _render_upload_section(all_history, has_history)

    if st.session_state.get("pipeline_id"):
        st.divider()
        _pipeline_monitor_fragment()

# ── 탭 1: 주간 현황 ──────────────────────────────────────────────────────────
with tab1:
    if active_result is not None:
        st.caption(
            f"분석 기간: **{active_label}** &nbsp;|&nbsp; "
            f"저장 위치: `s3://jsw-aws-s3-work/satisfaction_history/{active_slug}.json`"
        )
        render_overview(active_result)
    elif not has_history:
        st.info(
            "아직 분석된 데이터가 없습니다.  \n"
            "**[📁 파일 업로드 / 분석]** 탭에서 파일을 업로드하고 분석을 시작하세요."
        )
    else:
        st.warning("선택한 주차의 데이터를 불러올 수 없습니다. (pkl 파일 누락 — 재분석 필요)")

# ── 탭 2: 도메인 분석 ─────────────────────────────────────────────────────────
with tab2:
    if active_result is not None:
        st.caption(
            f"분석 기간: **{active_label}** &nbsp;|&nbsp; "
            f"저장 위치: `s3://jsw-aws-s3-work/satisfaction_history/{active_slug}.json`"
        )
        render_domain(active_result)
    elif not has_history:
        st.info(
            "아직 분석된 데이터가 없습니다.  \n"
            "**[📁 파일 업로드 / 분석]** 탭에서 파일을 업로드하고 분석을 시작하세요."
        )
    else:
        st.warning("선택한 주차의 데이터를 불러올 수 없습니다. (pkl 파일 누락 — 재분석 필요)")

# ── 탭 3: 인사이트 ───────────────────────────────────────────────────────────
with tab3:
    if active_result is not None:
        st.caption(
            f"분석 기간: **{active_label}** &nbsp;|&nbsp; "
            f"저장 위치: `s3://jsw-aws-s3-work/satisfaction_history/{active_slug}.json`"
        )
        render_insight(active_result, active_slug)
    elif not has_history:
        st.info(
            "아직 분석된 데이터가 없습니다.  \n"
            "**[📁 파일 업로드 / 분석]** 탭에서 파일을 업로드하고 분석을 시작하세요."
        )
    else:
        st.warning("선택한 주차의 데이터를 불러올 수 없습니다. (pkl 파일 누락 — 재분석 필요)")
