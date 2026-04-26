"""
챗봇 만족도 대시보드 — EC2 전용 (S3 조회 전용)
실행: streamlit run app.py

[데이터 흐름]
- 로컬 PC에서 local_app.py로 분석 완료 → S3 자동 저장
- 이 앱은 S3에서 결과를 읽어 대시보드로 표시하기만 함

[탭 구조]
- 📊 주간 현황 / 📋 도메인 분석 / 💡 인사이트
"""

import streamlit as st

from core.evaluator import load_all_history, load_history_by_slug
from views.tab_overview import render_overview
from views.tab_domain import render_domain
from views.tab_insight import render_insight


# ── 페이지 설정 ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="챗봇 만족도 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    "<style>[data-testid='stToolbar'] { display: none !important; }</style>",
    unsafe_allow_html=True,
)

# ── 세션 상태 초기화 ─────────────────────────────────────────────────────────
if "result_cache" not in st.session_state:
    st.session_state["result_cache"] = {}

if "last_selected_slug" not in st.session_state:
    st.session_state["last_selected_slug"] = None


# ── 헬퍼: 결과 로드 (캐시 → S3) ─────────────────────────────────────────────

def _get_result(slug: str) -> dict | None:
    if slug in st.session_state["result_cache"]:
        return st.session_state["result_cache"][slug]
    result = load_history_by_slug(slug)
    if result:
        st.session_state["result_cache"][slug] = result
    return result


# ── S3 히스토리 로드 ──────────────────────────────────────────────────────────
all_history = load_all_history()
has_history = len(all_history) > 0


# ── 사이드바 ─────────────────────────────────────────────────────────────────
selected_slug  = None
selected_label = "—"

with st.sidebar:
    st.title("📊 챗봇 만족도")
    st.caption("SKT 챗봇 고객 대화 만족도 분석")
    st.divider()

    if st.button("🔄 새로고침", use_container_width=True, help="S3에서 최신 데이터를 다시 불러옵니다."):
        st.session_state["result_cache"] = {}
        st.rerun()

    st.divider()

    if has_history:
        st.markdown("**분석 주차 선택**")
        week_slugs  = [r["slug"]                        for r in all_history]
        week_labels = [r.get("date_label") or r["slug"] for r in all_history]

        default_idx = len(week_slugs) - 1
        if st.session_state.get("last_selected_slug") in week_slugs:
            default_idx = week_slugs.index(st.session_state["last_selected_slug"])

        sel_idx = st.selectbox(
            "조회할 주차를 선택하세요",
            options=range(len(week_slugs)),
            format_func=lambda i: week_labels[i],
            index=default_idx,
            key="week_selector",
        )
        selected_slug  = week_slugs[sel_idx]
        selected_label = week_labels[sel_idx]
        st.session_state["last_selected_slug"] = selected_slug

        rec = all_history[sel_idx]
        if rec.get("uploaded_at"):
            uploaded_dt = rec["uploaded_at"][:16].replace("T", " ")
            st.caption(f"분석일시: {uploaded_dt}")
        st.divider()

    st.caption("분석은 로컬 PC에서 `local_app.py`를 실행하세요.")


# ── active_result 결정 ────────────────────────────────────────────────────────
active_result = None
active_slug   = None
active_label  = "—"

if selected_slug:
    active_result = _get_result(selected_slug)
    active_slug   = selected_slug
    active_label  = selected_label


# ── 대시보드 헤더 ─────────────────────────────────────────────────────────────
st.title("📊 챗봇 만족도 대시보드")

_no_data_msg = "아직 분석된 데이터가 없습니다.  \n로컬 PC에서 `local_app.py`를 실행하여 분석을 시작하세요."
_no_pkl_msg  = "선택한 주차의 데이터를 불러올 수 없습니다. (pkl 파일 누락 — 로컬에서 재분석 필요)"


# ── 3개 탭 ───────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 주간 현황", "📋 도메인 분석", "💡 인사이트"])

with tab1:
    if active_result is not None:
        st.caption(f"분석 기간: **{active_label}** &nbsp;|&nbsp; S3: `satisfaction_history/{active_slug}.json`")
        render_overview(active_result)
    elif not has_history:
        st.info(_no_data_msg)
    else:
        st.warning(_no_pkl_msg)

with tab2:
    if active_result is not None:
        st.caption(f"분석 기간: **{active_label}** &nbsp;|&nbsp; S3: `satisfaction_history/{active_slug}.json`")
        render_domain(active_result)
    elif not has_history:
        st.info(_no_data_msg)
    else:
        st.warning(_no_pkl_msg)

with tab3:
    if active_result is not None:
        st.caption(f"분석 기간: **{active_label}** &nbsp;|&nbsp; S3: `satisfaction_history/{active_slug}.json`")
        render_insight(active_result, active_slug)
    elif not has_history:
        st.info(_no_data_msg)
    else:
        st.warning(_no_pkl_msg)
