"""Tab 2 — 도메인 분석: 만족/불만족 도메인 순위 + 대화 주제 + 대화 10턴 탐색."""
import streamlit as st
from core.evaluator import build_domain_ranking


_SENDER_LABEL = {
    "user": "👤 고객", "u": "👤 고객", "고객": "👤 고객", "손님": "👤 고객",
}
_USER_STYLE  = "background:#E3F2FD;padding:8px 12px;margin:3px 0;border-radius:8px;border-left:4px solid #2196F3;"
_AGENT_STYLE = "background:#F5F5F5;padding:8px 12px;margin:3px 0;border-radius:8px;border-left:4px solid #9E9E9E;"


def _render_turns(turns: list[dict]):
    for turn in turns:
        sender_key = turn["sender"].lower()
        is_user    = sender_key in _SENDER_LABEL
        label      = _SENDER_LABEL.get(sender_key, "🤖 챗봇")
        style      = _USER_STYLE if is_user else _AGENT_STYLE
        text       = turn["text"].replace("<", "&lt;").replace(">", "&gt;")
        st.markdown(f'<div style="{style}"><b>{label}</b>: {text}</div>', unsafe_allow_html=True)


def render_domain(result: dict):
    # 만족 / 불만족 토글
    label = st.radio(
        "분석 유형 선택",
        ["불만족", "만족"],
        horizontal=True,
        index=0,
    )

    session_eval_df = result.get("session_eval_df")
    detail_df       = result.get("detail_df")

    if session_eval_df is None or detail_df is None:
        st.warning("도메인 분석에 필요한 데이터가 없습니다. (캐시 pkl 파일 누락 — 재분석 필요)")
        return

    with st.spinner("도메인 순위 계산 중..."):
        rankings = build_domain_ranking(session_eval_df, detail_df, label)

    if not rankings:
        st.info(f"{label} 세션이 없습니다.")
        return

    st.caption(f"총 {len(rankings)}개 도메인 — 건수 내림차순")
    st.divider()

    for i, item in enumerate(rankings):
        col_rank, col_info = st.columns([1, 9])
        with col_rank:
            st.markdown(
                f'<div style="font-size:2rem;font-weight:bold;color:{"#F44336" if label=="불만족" else "#4CAF50"};'
                f'text-align:center;padding-top:8px;">{i+1}</div>',
                unsafe_allow_html=True,
            )
        with col_info:
            st.markdown(
                f"**{item['category_main']}** &nbsp;›&nbsp; **{item['category_sub']}** &nbsp;&nbsp;"
                f'<span style="color:gray;">({item["count"]}건)</span>',
                unsafe_allow_html=True,
            )

            # 대화 주제 (최대 3개)
            if item["topics"]:
                st.markdown("대화 주제:")
                for t in item["topics"]:
                    st.markdown(f"&nbsp;&nbsp;&nbsp;• {t}")

            # 대표 대화 익스팬더
            with st.expander("대표 대화 보기 (최대 10턴)"):
                if item["turns"]:
                    _render_turns(item["turns"])
                else:
                    st.write("대화 데이터가 없습니다.")

        st.divider()
