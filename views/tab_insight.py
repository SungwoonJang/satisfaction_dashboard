"""Tab 3 — 인사이트: Weekly 분석 + Why 분석 + Hot Topic / Pain Point."""
import re
import streamlit as st
from core.evaluator import load_all_history


# ── LLM 종합 분석 텍스트 → HTML 포맷터 ────────────────────────────────────────

def _split_plan_sentences(html: str) -> str:
    """실행 계획 블록 내 문장 경계(. 한글/대문자)를 불릿으로 분리한다."""
    marker = '<b>실행 계획</b><br>'
    idx = html.find(marker)
    if idx == -1:
        return html
    end_of_section = html.find('<br><br>', idx + len(marker))
    if end_of_section == -1:
        end_of_section = len(html)
    before  = html[:idx + len(marker)]
    section = html[idx + len(marker):end_of_section]
    after   = html[end_of_section:]
    # 마침표 + 공백 + 한글/대문자 → 마침표 + 불릿
    section = re.sub(r'\. +([가-힣A-Z])', r'.<br>&bull; \1', section)
    return before + section + after


def _format_synthesis(text: str) -> str:
    """LLM이 생성한 자유형식 텍스트를 단락 구분된 HTML로 변환한다."""
    if not text or not text.strip():
        return "LLM 분석 결과 없음 (LLM 미연동)"

    # 1. HTML 특수문자 이스케이프
    t = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # 2. 개행 → HTML 줄바꿈 (섹션 헤더 치환 이전에 실행)
    t = t.replace("\n\n", "<br><br>").replace("\n", "<br>")

    # 3. 주요 섹션 헤더 → 굵은 줄바꿈 블록
    # "채널별 실행 계획"을 먼저 치환한 뒤 standalone "실행 계획"을 처리해 이중 매칭 방지
    section_headers = [
        (r'종합평가\s*[:\.]?\s*',                                           "종합평가"),
        (r'채널별\s*실행\s*계획\s*[:\.]?\s*',                                "채널별 실행 계획"),
        (r'실행\s*계획\s*[:\.]?\s*',                                         "실행 계획"),
        (r'개선\s*제안\s*[\(（]?(?:[왜어][–\-]?어떻게)?[\)）]?\s*[:\.]?\s*',  "개선 제안"),
        (r'마케팅\s*메시지\s*[:\.]?\s*',                                     "마케팅 메시지"),
    ]
    for pattern, label in section_headers:
        t = re.sub(pattern, f'<br><br><b>{label}</b><br>', t)

    # 4. 첫째/둘째/셋째 → 불릿 항목
    ordinals = [
        (r'첫\s*째\s*[,.]?\s*', "첫째"),
        (r'둘\s*째\s*[,.]?\s*', "둘째"),
        (r'셋\s*째\s*[,.]?\s*', "셋째"),
    ]
    for pattern, label in ordinals:
        t = re.sub(pattern, f'<br>&bull; <b>{label},</b> ', t)

    # 5. (1) (2) (3) 형태 번호 목록 → 불릿
    t = re.sub(r'\(([0-9]+)\)\s*', r'<br>&bull; (\1) ', t)

    # 6. 채널 아이템: 채널명 + 구분자(-, –, —, :) → 불릿 헤더 + 서브 설명
    # em dash(—) 포함, 더 구체적인 패턴(긴 것)을 앞에 두어 짧은 패턴과 겹치지 않도록 순서 유지
    _CHANNEL_KEYWORDS = [
        r'배너\s*[\(（][^）\)]*[\)）]',
        r'배너',
        r'푸시',
        r'SNS',
        r'인앱\s*/\s*챗\s*시작\s*팝업',
        r'인앱',
        r'오프라인\s*/\s*매장',
        r'오프라인',
        r'매장',
        r'KPI',
        r'테스트\s*[·‧]\s*운영',
        r'개인정보[·‧해외연동가-힣]*',
    ]
    for kw in _CHANNEL_KEYWORDS:
        t = re.sub(
            rf'(<br>|(?<![가-힣]))({kw})\s*[-–—:]\s*',
            r'\1<br>&bull; <b>\2</b><br>- ',
            t,
        )

    # 7. * 항목 형태 → 불릿 (LLM이 * 를 직접 생성한 경우)
    t = re.sub(r'(?:<br>)+\s*\*\s+', '<br>&bull; ', t)

    # 8. 실행 계획 블록 내 문장 분리 → 불릿
    t = _split_plan_sentences(t)

    # 9. 3개 이상 연속 <br> → 2개로 축소
    t = re.sub(r'(<br>\s*){3,}', '<br><br>', t)

    # 10. 앞쪽 남는 <br> 제거
    t = re.sub(r'^(<br>\s*)+', '', t).strip()
    return t


def _render_synthesis_box(html_content: str):
    """포맷된 HTML 내용을 진한 검정 텍스트의 스타일 박스로 렌더링한다."""
    st.markdown(
        f'<div style="'
        f'background:#F9F9F9;border:1px solid #E0E0E0;border-radius:8px;'
        f'padding:16px 18px;color:#111111;font-size:0.9rem;line-height:1.75;'
        f'overflow-y:auto;">'
        f'{html_content}'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_insight(result: dict, current_slug: str):
    history = load_all_history()
    history_sorted = sorted(history, key=lambda r: r.get("slug", ""))

    # 이전 주차 데이터 탐색
    current_idx = next((i for i, r in enumerate(history_sorted) if r["slug"] == current_slug), -1)
    prev = history_sorted[current_idx - 1] if current_idx > 0 else None

    # ── Section 1: Weekly 분석 ───────────────────────────────────────────────
    st.subheader("📅 Weekly 분석")

    sat   = result.get("satisfied", 0)
    diss  = result.get("dissatisfied", 0)
    total = result.get("total", 0) or 1

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**이번 기간 요약**")
        winner      = "만족" if sat >= diss else "불만족"
        winner_clr  = "#4CAF50" if winner == "만족" else "#F44336"
        st.markdown(
            f'<div style="border-radius:8px;padding:12px;background:#F9F9F9;">'
            f'이번 기간은 <span style="color:{winner_clr};font-weight:bold;">{winner}</span>이 더 많았습니다.<br><br>'
            f'😊 만족 <b>{sat}건</b> ({sat/total*100:.1f}%)&nbsp;&nbsp;'
            f'😤 불만족 <b>{diss}건</b> ({diss/total*100:.1f}%)&nbsp;&nbsp;'
            f'😐 중립 <b>{result.get("neutral",0)}건</b>'
            f'</div>',
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown("**지난주 대비 변화**")
        if prev:
            sat_diff  = sat  - prev.get("satisfied", 0)
            diss_diff = diss - prev.get("dissatisfied", 0)
            prev_lbl  = prev.get("date_label") or prev["slug"]

            def _delta(v):
                sign  = "+" if v >= 0 else ""
                color = "#4CAF50" if v <= 0 else "#F44336"  # 불만족은 감소가 좋음
                return f'<span style="color:{color};font-weight:bold;">{sign}{v}건</span>'

            st.markdown(
                f'<div style="border-radius:8px;padding:12px;background:#F9F9F9;">'
                f'이전 기간 <b>{prev_lbl}</b> 대비<br><br>'
                f'😊 만족 {_delta(sat_diff)}&nbsp;&nbsp;'
                f'😤 불만족 {_delta(diss_diff)}'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("비교할 이전 데이터가 없습니다. (현재 첫 분석)")

    st.divider()

    # ── Section 2: Why 분석 ──────────────────────────────────────────────────
    st.markdown('<h3 style="color:#111111;">🔍 Why 분석</h3>', unsafe_allow_html=True)

    col3, col4 = st.columns(2)

    with col3:
        st.markdown(
            '<p style="font-weight:bold;color:#111111;margin-bottom:6px;">고객들은 왜 칭찬했나?</p>',
            unsafe_allow_html=True,
        )
        sat_result = result.get("sat_llm_result") or {}
        syn_sat    = sat_result.get("synthesis", "")
        _render_synthesis_box(_format_synthesis(syn_sat))

    with col4:
        st.markdown(
            '<p style="font-weight:bold;color:#111111;margin-bottom:6px;">고객들은 왜 불만을 표현했나?</p>',
            unsafe_allow_html=True,
        )
        syn_diss = result.get("diss_synthesis", "")
        _render_synthesis_box(_format_synthesis(syn_diss))

    st.divider()

    # ── Section 3: Hot Topic & Pain Point ───────────────────────────────────
    st.subheader("🚀 홍보 및 개선책")

    col5, col6 = st.columns(2)

    with col5:
        st.markdown("**🔥 Hot Topic — 홍보 추천**")
        sat_result = result.get("sat_llm_result") or {}
        mkt_msg    = sat_result.get("marketing_message", "")
        if mkt_msg:
            st.success(f"💬 추천 마케팅 메시지\n\n**{mkt_msg}**")

        top10_sat = result.get("top10_satisfied")
        if top10_sat is not None and not top10_sat.empty:
            st.markdown("**만족 상위 서비스:**")
            cnt_col = "만족 건수" if "만족 건수" in top10_sat.columns else top10_sat.columns[2]
            for _, row in top10_sat.head(5).iterrows():
                st.markdown(f"- {row['대분류']} › **{row['중분류']}** ({row[cnt_col]}건)")
        else:
            st.info("만족 데이터가 없습니다.")

    with col6:
        st.markdown("**🔧 Pain Point — 개선 우선순위**")
        top10_diss = result.get("top10_dissatisfied")
        if top10_diss is not None and not top10_diss.empty:
            st.markdown("**불만족 상위 서비스:**")
            cnt_col = "불만족 건수" if "불만족 건수" in top10_diss.columns else top10_diss.columns[2]
            for _, row in top10_diss.head(5).iterrows():
                st.markdown(f"- {row['대분류']} › **{row['중분류']}** ({row[cnt_col]}건)")
        else:
            st.info("불만족 데이터가 없습니다.")

    st.divider()

    # ── Section 4: 이전 주차 히스토리 타임라인 ────────────────────────────────
    if len(history_sorted) > 1:
        st.subheader("📆 분석 히스토리")
        cols = st.columns(min(len(history_sorted), 6))
        for idx, rec in enumerate(history_sorted[-6:]):  # 최근 6주만 표시
            lbl   = rec.get("date_label") or rec["slug"]
            t     = rec.get("total", 1) or 1
            s_pct = round(rec.get("satisfied", 0) / t * 100, 1)
            d_pct = round(rec.get("dissatisfied", 0) / t * 100, 1)
            is_cur = rec["slug"] == current_slug
            border = "3px solid #2196F3" if is_cur else "1px solid #ddd"
            cols[idx].markdown(
                f'<div style="border:{border};border-radius:8px;padding:8px;text-align:center;">'
                f'<b>{lbl}</b><br>'
                f'<span style="color:#4CAF50;">😊 {s_pct}%</span><br>'
                f'<span style="color:#F44336;">😤 {d_pct}%</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
