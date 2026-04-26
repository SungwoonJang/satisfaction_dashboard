"""Tab 1 — 주간 현황: KPI 카드 + 주간 추이 그래프 + 대분류별 Bar 차트."""
import streamlit as st
import plotly.graph_objects as go
from core.evaluator import load_all_history


def render_overview(result: dict):
    # ── KPI 카드 ────────────────────────────────────────────────────────────
    total = result.get("total", 0) or 1  # 0 나누기 방지
    sat   = result.get("satisfied", 0)
    diss  = result.get("dissatisfied", 0)
    neu   = result.get("neutral", 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📊 전체 세션",  f"{result.get('total', 0):,}건")
    c2.metric("😊 만족",       f"{sat:,}건",  f"{sat/total*100:.1f}%")
    c3.metric("😤 불만족",     f"{diss:,}건", f"{diss/total*100:.1f}%")
    c4.metric("😐 중립",       f"{neu:,}건",  f"{neu/total*100:.1f}%")

    st.divider()

    # ── 주간 추이 라인 차트 ─────────────────────────────────────────────────
    history = load_all_history()
    if history:
        st.subheader("📈 주간 만족도 추이")
        x      = [r.get("date_label") or r["slug"] for r in history]
        sat_y  = [r.get("satisfied", 0)    for r in history]
        diss_y = [r.get("dissatisfied", 0) for r in history]
        neu_y  = [r.get("neutral", 0)      for r in history]
        tot_y  = [r.get("total", 1) or 1   for r in history]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x, y=sat_y, mode="lines+markers", name="만족",
            line=dict(color="#4CAF50", width=2), marker=dict(size=8),
            customdata=[[s, round(s/t*100,1)] for s, t in zip(sat_y, tot_y)],
            hovertemplate="<b>%{x}</b><br>만족: %{customdata[0]}건 (%{customdata[1]}%)<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=x, y=diss_y, mode="lines+markers", name="불만족",
            line=dict(color="#F44336", width=2), marker=dict(size=8),
            customdata=[[d, round(d/t*100,1)] for d, t in zip(diss_y, tot_y)],
            hovertemplate="<b>%{x}</b><br>불만족: %{customdata[0]}건 (%{customdata[1]}%)<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=x, y=neu_y, mode="lines+markers", name="중립",
            line=dict(color="#9E9E9E", width=2), marker=dict(size=8),
            customdata=[[n, round(n/t*100,1)] for n, t in zip(neu_y, tot_y)],
            hovertemplate="<b>%{x}</b><br>중립: %{customdata[0]}건 (%{customdata[1]}%)<extra></extra>",
        ))
        fig.update_layout(
            hovermode="x unified",
            xaxis_title="기간",
            yaxis_title="세션 수",
            height=420,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.2,
                xanchor="center",
                x=0.5,
            ),
            margin=dict(t=20, b=90),
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("아직 저장된 이전 주차 데이터가 없습니다. 분석 완료 후 주간 추이가 표시됩니다.")

    st.divider()

    # ── 대분류별 만족도 Bar 차트 ─────────────────────────────────────────────
    st.subheader("📊 대분류별 만족도")
    ms = result.get("main_category_stats")
    if ms is not None and not ms.empty:
        plot_df = ms[ms["대분류"] != "전체합계"].copy()

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            name="만족", x=plot_df["대분류"], y=plot_df["만족"],
            marker_color="#4CAF50",
            hovertemplate="%{x}<br>만족: %{y}건<extra></extra>",
        ))
        fig2.add_trace(go.Bar(
            name="불만족", x=plot_df["대분류"], y=plot_df["불만족"],
            marker_color="#F44336",
            hovertemplate="%{x}<br>불만족: %{y}건<extra></extra>",
        ))
        fig2.add_trace(go.Bar(
            name="중립", x=plot_df["대분류"], y=plot_df["중립"],
            marker_color="#9E9E9E",
            hovertemplate="%{x}<br>중립: %{y}건<extra></extra>",
        ))
        fig2.update_layout(
            barmode="group",
            xaxis_title="대분류",
            yaxis_title="세션 수",
            height=420,
            # 범례를 차트 하단 중앙으로 배치 → 우상단 Plotly 툴바와 겹침 방지
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.2,
                xanchor="center",
                x=0.5,
            ),
            margin=dict(t=20, b=90),   # 하단 여백 확보
        )
        st.plotly_chart(fig2, width='stretch')

        # 수치 테이블
        with st.expander("대분류별 상세 수치 보기"):
            st.dataframe(plot_df[["대분류","만족","불만족","중립","전체합계","만족 비율(%)","불만족 비율(%)"]],
                         hide_index=True, width='stretch')
    else:
        st.info("대분류 통계 데이터가 없습니다.")
