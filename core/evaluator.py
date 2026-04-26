"""
핵심 처리 로직 — 발화 분석, 세션 평가, 도메인 분류, history 관리.
웹(app.py)과 CLI(chatbot_satisfaction_eval.py) 양쪽에서 import해서 사용한다.
"""

import os
import re
import json
import pickle
import hashlib
import threading
import datetime
import time
import concurrent.futures
from pathlib import Path
from typing import Optional

import pandas as pd
import boto3
from botocore.exceptions import ClientError
import io

# ── S3 설정 (분석 결과 이력 저장소) ──────────────────────────────────────────
_S3_BUCKET = "jsw-aws-s3-work"
_S3_PREFIX = "satisfaction_history"


def _s3():
    """boto3 S3 클라이언트 (EC2 IAM Role 또는 환경변수 자동 인식)."""
    return boto3.client("s3", region_name="ap-northeast-2")


def _s3_key(filename: str) -> str:
    """S3 오브젝트 키 생성."""
    return f"{_S3_PREFIX}/{filename}"


# ──────────────────────────────────────────────────────────────────────────────
# LLM 토큰 방어 상수
# ──────────────────────────────────────────────────────────────────────────────
_MAX_SESSION_TEXT_CHARS = 2_000   # 세션당 LLM 입력 최대 글자 수 (초과분 절삭)
_MIN_USER_TURNS_FOR_LLM = 2       # 사용자 발화 수 최소 기준 (미만이면 LLM 스킵 → 중립)

from core.categories import (
    SESSION_COL, TIMESTAMP_COL, USER_ID_COL, SENDER_COL, TEXT_COL,
    PROFANITY_PATTERN, NEGATIVE_PATTERN, GRATITUDE_PATTERN,
    COUNSELOR_PATTERN, AGENT_LIMIT_PATTERN,
    EXCLUDED_GRATITUDE_TERMS, ALLOWED_CATEGORY_MAIN, CATEGORY_SUB_MAP,
)
from core.llm_client import init_client, MODEL_NAME as _DEFAULT_MODEL_NAME


# ──────────────────────────────────────────────────────────────────────────────
# 파일명 파싱
# ──────────────────────────────────────────────────────────────────────────────

def parse_filename_meta(filename: str) -> dict:
    """
    파일명에서 slug(저장 키)와 날짜 레이블을 추출한다.
    날짜 패턴(MMDD-MMDD)이 있으면 slug를 날짜 부분으로만 설정하여,
    파일명 접두사가 달라도 같은 날짜이면 동일 데이터로 간주한다.
    예) "chatbot_0413-0424.xlsx" → slug="0413-0424", date_label="04.13~04.24"
        "report_0413-0424.xlsx" → slug="0413-0424"  (동일 slug → 덮어쓰기)
    날짜 패턴이 없으면 파일명 전체(stem)를 slug로 사용한다.
    """
    stem = Path(filename).stem  # 확장자 제거
    m = re.search(r"(\d{4})-(\d{4})", stem)
    if m:
        start, end = m.group(1), m.group(2)
        date_label = f"{start[:2]}.{start[2:]}~{end[:2]}.{end[2:]}"
        slug = f"{start}-{end}"   # 날짜 부분만 slug로 사용
    else:
        start, end, date_label = "", "", ""
        slug = stem               # 날짜 패턴 없으면 파일명 전체 사용
    return {"slug": slug, "date_label": date_label, "start": start, "end": end}


# ──────────────────────────────────────────────────────────────────────────────
# 데이터 전처리
# ──────────────────────────────────────────────────────────────────────────────

def normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """sender_type/utterance 컬럼 형태 or user_message/agent_answer 형태를 통일한다."""
    if SENDER_COL in df.columns and TEXT_COL in df.columns:
        return df.drop(columns=["평가결과", "평가이유"], errors="ignore")

    if "user_message" in df.columns and "agent_answer" in df.columns:
        rows = []
        for _, row in df.iterrows():
            base = row.to_dict()
            for col in ["평가결과", "평가이유", "user_message", "agent_answer"]:
                base.pop(col, None)
            meta = base.copy()

            if pd.notna(row.get("user_message")) and str(row.get("user_message")).strip():
                d = meta.copy()
                d[SENDER_COL] = "user"
                d[TEXT_COL] = str(row["user_message"])
                rows.append(d)

            if pd.notna(row.get("agent_answer")) and str(row.get("agent_answer")).strip():
                d = meta.copy()
                d[SENDER_COL] = "agent"
                d[TEXT_COL] = str(row["agent_answer"])
                rows.append(d)

        if not rows:
            raise ValueError("유효한 대화 데이터가 없습니다.")
        new_df = pd.DataFrame(rows)
        if TIMESTAMP_COL not in new_df.columns:
            raise ValueError(f"'{TIMESTAMP_COL}' 컬럼이 필요합니다.")
        return new_df

    raise ValueError("지원하지 않는 파일 형식입니다. (sender_type/utterance 또는 user_message/agent_answer 필요)")


def analyze_utterance(text: str) -> dict:
    if not isinstance(text, str):
        text = "" if pd.isna(text) else str(text)

    temp = text
    for term in EXCLUDED_GRATITUDE_TERMS:
        temp = temp.replace(term, " ")

    return {
        "has_profanity":  bool(PROFANITY_PATTERN.search(text)),
        "has_negative":   bool(NEGATIVE_PATTERN.search(text)),
        "has_gratitude":  bool(GRATITUDE_PATTERN.search(temp)),
        "asks_counselor": bool(COUNSELOR_PATTERN.search(text)),
        "agent_limit":    bool(AGENT_LIMIT_PATTERN.search(text)),
    }


def validate_and_fix_categories(category_main: str, category_sub: str) -> tuple[str, str]:
    """LLM 출력 대분류/중분류를 허용 목록과 대조해 교정한다."""
    if category_main not in ALLOWED_CATEGORY_MAIN:
        fixed = "기타"
        for allowed in ALLOWED_CATEGORY_MAIN:
            prefix = allowed.split("(")[0]
            if prefix and category_main.startswith(prefix):
                fixed = allowed
                break
        category_main = fixed

    allowed_subs = CATEGORY_SUB_MAP.get(category_main, ["기타"])
    if category_sub not in allowed_subs:
        category_sub = "기타"

    return category_main, category_sub


# ──────────────────────────────────────────────────────────────────────────────
# LLM 평가
# ──────────────────────────────────────────────────────────────────────────────

_CATEGORY_MAPPING_PROMPT = """
[대분류 및 중분류 매핑 가이드]
[
{대분류: CS0(데이터/리필)
{중분류: 데이터사용량/잔여량, 음성잔여량, 데이터선물하기, 리필쿠폰}},
{대분류: CS1(요금제)
{중분류: 요금제혜택, 요금제추천, 요금제검색, 요금제변경, 요금제조회}},
{대분류: CS2(납부/청구/할부)
{중분류: 미납문의, 납부문의, 요금문의, 약정/할인반환금, 기기할부}},
{대분류: CS3(부가서비스/결합)
{중분류: 부가서비스가입, 부가서비스해지, 부가서비스변경, 부가서비스조회, 결합상태조회, 결합가입, 결합해지, 결합변경}},
{대분류: CS4(로밍)
{중분류: 로밍요금제, 로밍이용내역, 로밍해지, 로밍데이터, 로밍사용이슈}},
{대분류: CS5(기기구매/개통)
[분류 조건: 반드시 아래 항목 중 하나가 대화에 명확히 나타나야 합니다]
- 아이폰 또는 갤럭시(삼성) 스마트폰 구매 문의
- T다이렉트 요금제 가입·문의
- T다이렉트 할인 적용 문의
- USIM 요금제 가입 / USIM 기기변경
- eSIM 신규가입 / eSIM 기기변경
[위 조건에 해당하지 않으면 CS5가 아닌 다른 대분류 또는 기타로 분류할 것]
{중분류: 기기구매, 개통, 기기변경, USIM개통, USIM해지, eSIM가입, 주문/배송문의}},
{대분류: CS6(T멤버십/구독)
{중분류: T멤버십혜택, T멤버십조회, T멤버십변경, T멤버십해지, 구독상태조회, 구독신청, 구독해지}},
{대분류: 기타
{중분류: FAQ, 기타}},
]
"""

_SYSTEM_PROMPT_TEMPLATE = f"""You are an expert in analyzing telecommunications customer service logs to classify customer satisfaction and service domains.

[운영 지침]
- 대화 내용을 빠짐없이 철저히 검토한 후 평가하세요.
- 분류 결과가 통계 집계에 직접 사용되므로 오류 없이 정확하게 출력해야 합니다.

[분류 절차]
1. topic_summary: 첫 Agent 인트로 발화 무시, 고객 실제 질문 기준 30자 이내 한 문장 요약
2. category_main / category_sub: 매핑 가이드에서 선택
3. label / reason: 대화 전체 감정 흐름 검토

{_CATEGORY_MAPPING_PROMPT}

[대분류·중분류 출력 규칙]
허용 category_main: CS0(데이터/리필) | CS1(요금제) | CS2(납부/청구/할부) | CS3(부가서비스/결합) | CS4(로밍) | CS5(기기구매/개통) | CS6(T멤버십/구독) | 기타
category_sub는 선택한 대분류 블록의 중분류만 글자 단위로 동일하게 복사하세요.

[평가 기준 — 반드시 엄격하게 적용]

1. 만족: 고객이 긍정·감사를 **직접 언어로 표현**한 경우만 해당
   - 해당 예시: "감사합니다", "도움이 됐어요", "해결됐어요 감사해요", "수고하세요"
   - 비해당: "네", "알겠어요" 등 단답형, 감정 표현 없이 종료

2. 불만족: 고객이 불만·불평·짜증·분노를 **직접적인 언어로 표현**한 경우만 해당
   - 해당 예시: 욕설, "말이 다르잖아", "뭐라는 거야", "짜증나", "화나", "이게 뭐야", "말이 안 되잖아"
   - 비해당: 요청한 업무가 완료되지 않았더라도 고객이 감정을 표현하지 않은 경우

   ⚠️ 핵심 원칙: **업무 미처리 ≠ 불만족**
   챗봇이 "안 된다"고 안내했더라도 고객이 감정적 불만 표현 없이 대화를 종료하면 반드시 '중립'으로 분류하세요.

   [판단 예시 — 중립]
   user: 리필 쿠폰 충전해줘
   agent: 충전할 수 있는 쿠폰이 없어요.
   → 업무 미완료지만 고객이 감정을 표현하지 않았으므로 → 중립

   [판단 예시 — 불만족]
   user: 리필 쿠폰 충전해줘
   agent: 충전할 수 있는 쿠폰이 없어요.
   user: 아까는 있다고 했잖아. 뭐라는 거야
   agent: 불편을 드려 죄송해요.
   → 고객이 "뭐라는 거야"로 직접 불만을 표현했으므로 → 불만족

3. 중립: 감정 표현 없는 모든 경우 (업무 완료·미완료 여부와 무관)
   단순 정보 문의, 안내 수신 후 조용히 종료, 단답형 응답("네", "확인했어요") 등

[출력 형식 — 반드시 JSON]
{{"topic_summary": "30자 이내", "label": "만족/불만족/중립", "reason": "1줄", "category_main": "<원문>", "category_sub": "<원문>"}}
"""


class _SessionLLMCache:
    """
    파이프라인 1회 실행 내에서 동일한 대화 텍스트에 대한 LLM 중복 호출을 방지한다.
    - 동일 대화 텍스트(MD5 해시 기준)는 첫 호출 결과를 재사용
    - ThreadPoolExecutor 병렬 환경에서 안전하게 동작 (Lock 사용)
    """
    def __init__(self):
        self._store: dict[str, dict] = {}
        self._lock  = threading.Lock()
        self.hits   = 0
        self.misses = 0

    def _key(self, text: str) -> str:
        return hashlib.md5(text.encode("utf-8", errors="replace")).hexdigest()

    def get(self, text: str) -> Optional[dict]:
        k = self._key(text)
        with self._lock:
            val = self._store.get(k)
            if val is not None:
                self.hits += 1
            else:
                self.misses += 1
            return val

    def set(self, text: str, value: dict) -> None:
        k = self._key(text)
        with self._lock:
            self._store[k] = value


def evaluate_session_by_llm(session_text: str, client, model_name: str) -> dict:
    if client is None:
        return {
            "topic_summary": "", "label": "중립",
            "reason": "LLM 미연동", "category_main": "기타", "category_sub": "기타",
        }

    user_prompt = (
        f"--- 대화 내용 ---\n{session_text}\n-----------------\n"
        "위 대화의 만족도(label)를 분류하고, 도메인은 매핑 가이드 문자열 그대로 출력하세요."
    )

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT_TEMPLATE},
                {"role": "user",   "content": user_prompt},
            ],
            response_format={"type": "json_object"},
        )
        data = json.loads(response.choices[0].message.content.strip())
        fixed_main, fixed_sub = validate_and_fix_categories(
            data.get("category_main", "기타"),
            data.get("category_sub", "기타"),
        )
        return {
            "topic_summary": data.get("topic_summary", ""),
            "label":         data.get("label", "중립"),
            "reason":        f"[LLM] {data.get('reason', '')}",
            "category_main": fixed_main,
            "category_sub":  fixed_sub,
        }
    except Exception as e:
        return {
            "topic_summary": "", "label": "중립",
            "reason": f"LLM 에러: {e}", "category_main": "기타", "category_sub": "기타",
        }


def evaluate_session(
    session_data,
    client,
    model_name: str,
    llm_cache: Optional[_SessionLLMCache] = None,
) -> dict:
    session_id, session_df = session_data

    user_mask = session_df[SENDER_COL].astype(str).str.lower().isin(["user", "u", "고객", "손님"])
    user_rows = session_df[user_mask]

    has_profanity  = user_rows["has_profanity"].any()
    has_negative   = user_rows["has_negative"].any()
    has_gratitude  = user_rows["has_gratitude"].any()   # noqa: F841
    asks_counselor = user_rows["asks_counselor"].any()
    agent_limit    = user_rows["agent_limit"].any()

    last = user_rows.sort_values(TIMESTAMP_COL).tail(1)
    last_gratitude = bool(last["has_gratitude"].any()) if not last.empty else False
    last_negative  = bool(last["has_negative"].any())  if not last.empty else False

    rule_label  = None
    rule_reason = ""

    if last_gratitude and not has_negative and not has_profanity:
        rule_label  = "만족"
        rule_reason = "(Rule) 마지막 발화에서 명확한 감사·긍정 표현 감지"
    elif has_profanity or (has_negative and asks_counselor) or (has_negative and agent_limit) or last_negative:
        rule_label  = "불만족"
        rule_reason = "(Rule) 욕설 또는 해결되지 않은 불만족 징후 감지"

    # ── 방어 1: 사용자 발화 수 최소 기준 미달 → LLM 스킵 ─────────────────────
    user_turn_count = int(user_rows.shape[0])
    if user_turn_count < _MIN_USER_TURNS_FOR_LLM:
        return {
            "session_id":     session_id,
            "session_label":  rule_label or "중립",
            "session_reason": rule_reason or f"(Skip) 사용자 발화 {user_turn_count}턴 → LLM 스킵",
            "topic_summary":  "",
            "category_main":  "기타",
            "category_sub":   "기타",
        }

    # LLM 입력 구성: 첫 User 발화 이전 Agent 인트로 제거
    sorted_sess = session_df.sort_values(TIMESTAMP_COL)
    user_idx    = sorted_sess[sorted_sess[SENDER_COL].astype(str).str.lower().isin(["user","u","고객","손님"])].index
    trimmed     = sorted_sess.loc[user_idx[0]:] if not user_idx.empty else sorted_sess
    session_text = "\n".join(
        f"{r.get(SENDER_COL,'')}: {r.get(TEXT_COL,'')}" for _, r in trimmed.iterrows()
    )

    # ── 방어 2: 세션 텍스트 최대 길이 절삭 (토큰 낭비 방지) ──────────────────
    if len(session_text) > _MAX_SESSION_TEXT_CHARS:
        session_text = session_text[:_MAX_SESSION_TEXT_CHARS] + "\n...(이하 생략)"

    if client is None:
        return {
            "session_id":     session_id,
            "session_label":  rule_label or "중립",
            "session_reason": rule_reason or "LLM 미연동",
            "topic_summary":  "",
            "category_main":  "기타",
            "category_sub":   "기타",
        }

    # ── 방어 3: 동일 대화 텍스트 중복 LLM 호출 방지 (해시 캐시) ──────────────
    if llm_cache:
        cached = llm_cache.get(session_text)
        if cached is not None:
            return {
                "session_id":     session_id,
                "session_label":  rule_label or cached["label"],
                "session_reason": rule_reason or f"[LLM/캐시] {cached['reason']}",
                "topic_summary":  cached["topic_summary"],
                "category_main":  cached["category_main"],
                "category_sub":   cached["category_sub"],
            }

    llm = evaluate_session_by_llm(session_text, client, model_name)

    if llm_cache:
        llm_cache.set(session_text, llm)

    return {
        "session_id":     session_id,
        "session_label":  rule_label or llm["label"],
        "session_reason": rule_reason or llm["reason"],
        "topic_summary":  llm["topic_summary"],
        "category_main":  llm["category_main"],
        "category_sub":   llm["category_sub"],
    }


# ──────────────────────────────────────────────────────────────────────────────
# 세션 분석 헬퍼
# ──────────────────────────────────────────────────────────────────────────────

def _count_chars_ko(s: str) -> int:
    return len(s) if isinstance(s, str) else 0


def _safe_truncate(text: str, max_chars: int = 1500) -> str:
    """max_chars 초과 시 마지막 완성 문장(마침표) 위치에서 잘라낸다."""
    if not text or len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    last_end = max(cut.rfind(". "), cut.rfind(".\n"), cut.rfind("\u3002"))
    if last_end > max_chars // 2:
        return cut[:last_end + 1]
    return cut


def format_session_dialogue(detail_df: pd.DataFrame, session_id, max_chars: int = 900) -> str:
    """세션 전체 발화를 하나의 문자열로 직렬화 (Top10 테이블 대표 대화용)."""
    if detail_df.empty or SESSION_COL not in detail_df.columns:
        return ""
    sub   = detail_df[detail_df[SESSION_COL] == session_id].sort_values(TIMESTAMP_COL)
    lines = [f"{r.get(SENDER_COL,'')}: {r.get(TEXT_COL,'')}" for _, r in sub.iterrows()]
    text  = "\n".join(lines)
    return text[:max_chars - 3] + "..." if len(text) > max_chars else text


def extract_key_turns(
    detail_df: pd.DataFrame,
    session_id,
    trigger_type: str = "dissatisfied",
    max_turns: int = 10,
) -> list[dict]:
    """
    만족/불만족 트리거 발화 기준 최대 max_turns 턴 추출.
    - dissatisfied: 마지막 욕설/부정어/상담원 요청 발화까지
    - satisfied   : 마지막 감사 표현 발화까지
    """
    sub = detail_df[detail_df[SESSION_COL] == session_id].sort_values(TIMESTAMP_COL).reset_index(drop=True)
    if sub.empty:
        return []

    trigger_idx = None
    if trigger_type == "dissatisfied":
        for i in range(len(sub) - 1, -1, -1):
            row = sub.iloc[i]
            if row.get("has_profanity") or row.get("has_negative") or row.get("asks_counselor"):
                trigger_idx = i
                break
    else:
        for i in range(len(sub) - 1, -1, -1):
            if sub.iloc[i].get("has_gratitude"):
                trigger_idx = i
                break

    if trigger_idx is None:
        sub_slice = sub.tail(max_turns)
    else:
        start = max(0, trigger_idx - max_turns + 1)
        sub_slice = sub.iloc[start: trigger_idx + 1]

    return [
        {"sender": str(r.get(SENDER_COL, "")), "text": str(r.get(TEXT_COL, ""))}
        for _, r in sub_slice.iterrows()
    ]


def get_top3_topics_per_domain(
    session_eval_df: pd.DataFrame,
    label: str,
    category_main: str,
    category_sub: str,
) -> list[str]:
    """특정 label·도메인의 topic_summary 상위 3개 (중복 제거)."""
    mask = (
        (session_eval_df["session_label"] == label) &
        (session_eval_df["category_main"] == category_main) &
        (session_eval_df["category_sub"]  == category_sub) &
        session_eval_df["topic_summary"].notna() &
        (session_eval_df["topic_summary"] != "")
    )
    return session_eval_df[mask]["topic_summary"].drop_duplicates().head(3).tolist()


def build_domain_ranking(
    session_eval_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    label: str,
) -> list[dict]:
    """label(만족/불만족) 기준 도메인 순위 목록 구성 (건수 내림차순)."""
    sub = session_eval_df[session_eval_df["session_label"] == label]
    if sub.empty:
        return []

    trigger = "dissatisfied" if label == "불만족" else "satisfied"

    grouped = (
        sub.groupby(["category_main", "category_sub"])
        .agg(count=("session_id", "count"), sessions=("session_id", list))
        .reset_index()
        .sort_values("count", ascending=False)
    )

    ranking = []
    for _, row in grouped.iterrows():
        topics = get_top3_topics_per_domain(
            session_eval_df, label, row["category_main"], row["category_sub"]
        )
        turns = extract_key_turns(detail_df, row["sessions"][0], trigger_type=trigger)
        ranking.append({
            "category_main": row["category_main"],
            "category_sub":  row["category_sub"],
            "count":         int(row["count"]),
            "topics":        topics,
            "turns":         turns,
        })
    return ranking


def build_top10_category_table(
    session_eval_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    session_label: str,
    count_col_name: str,
) -> pd.DataFrame:
    """중분류 Top10: 대분류·중분류·건수·평가이유·대표 대화."""
    sub = session_eval_df[session_eval_df["session_label"] == session_label]
    if sub.empty:
        return pd.DataFrame(columns=["대분류", "중분류", count_col_name, "평가이유", "대표 대화 사례"])

    rows = []
    for cat_sub, cnt in sub["category_sub"].value_counts().head(10).items():
        part     = sub[sub["category_sub"] == cat_sub]
        mode     = part["category_main"].mode()
        cat_main = mode.iloc[0] if len(mode) else part["category_main"].iloc[0]
        sid      = part["session_id"].iloc[0]
        reason   = part["session_reason"].iloc[0] if "session_reason" in part.columns else ""
        rows.append({
            "대분류":          cat_main,
            "중분류":          cat_sub,
            count_col_name:   int(cnt),
            "평가이유":        reason,
            "대표 대화 사례":  format_session_dialogue(detail_df, sid),
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# LLM 종합 분석
# ──────────────────────────────────────────────────────────────────────────────

def _build_top10_context(table_df: pd.DataFrame, kind: str, count_col: str) -> str:
    lines = [f"[{kind} Top10 표]"]
    for i, (_, row) in enumerate(table_df.iterrows(), 1):
        reason = f"\n  평가이유: {str(row.get('평가이유',''))[:300]}" if row.get("평가이유") else ""
        lines.append(
            f"\n[{i}위] 대분류: {row['대분류']} | 중분류: {row['중분류']} | 건수: {row[count_col]}"
            f"\n  대표대화: {str(row.get('대표 대화 사례',''))[:600]}{reason}"
        )
    return "\n".join(lines)


def llm_synthesize_dissatisfaction_top10(table_df: pd.DataFrame, client, model_name: str) -> str:
    if client is None or table_df.empty:
        return (
            "[LLM 미연동] 불만족 Top10 종합평가를 생성하려면 USE_LLM 연동 후 실행하세요. "
            "운영에서는 중분류별 재학습·답변 템플릿·에스컬레이션 기준을 점검하고, "
            "주간 단위 로그 리뷰 및 개선 과제 백로그 관리를 병행하시기 바랍니다."
        )

    ctx = _build_top10_context(table_df, "불만족", "불만족 건수")
    prompt = f"""{ctx}

[작성 요구사항]
1. 주요 불만족 패턴 분석 (Top10 기반, 3가지 이상)
2. 챗봇 서비스 개선점 (3가지 이상, 왜·어떻게 구체 기술)
3. 실행 계획: FAQ/시나리오 보강, 상담원 연결 조건 개선 등

형식: JSON 아닌 평문, 800자 이상 1200자 이하, 반드시 완성된 문장으로 마침표(.)를 찍어 마무리"""

    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "당신은 챗봇 품질 개선 컨설턴트입니다. 반드시 800자 이상 1200자 이하 평문으로 작성합니다. 마지막 문장은 반드시 마침표(.)로 완성하여 끝냅니다. 문장이 중간에 끊기거나 잘리지 않도록 주의합니다."},
                {"role": "user",   "content": prompt},
            ],
        )
        text = (resp.choices[0].message.content or "").strip()
        if _count_chars_ko(text) < 500:
            text += (
                " 추가로 불만족 유형별 상담 로그를 주간 단위로 리뷰하고 개선 과제를 스프린트 백로그에 반영하여 "
                "지속적인 품질 개선 사이클을 구축해야 합니다."
            )
        return _safe_truncate(text)
    except Exception as e:
        return f"[LLM 오류] {e}"


def llm_synthesize_satisfaction_top10(table_df: pd.DataFrame, client, model_name: str) -> dict:
    if client is None or table_df.empty:
        return {
            "marketing_message": "AI상담, 빠르고 정확하게",
            "synthesis": (
                "[LLM 미연동] 만족 Top10 기반 마케팅 문구·종합평가는 USE_LLM 연동 후 생성됩니다. "
                "만족도가 높은 유형을 중심으로 성공 패턴을 분석하고 이를 다른 영역으로 확산하는 전략을 권장합니다."
            ),
        }

    ctx    = _build_top10_context(table_df, "만족", "만족 건수")
    schema = ('{"marketing_message": "30자 이상 50자 이내", '
              '"synthesis": "800자 이상 1200자 이하, 완성된 문장으로 마침표 마무리, 만족 패턴·마케팅 선정 이유·실행 계획"}')
    prompt = f"""{ctx}

[작성 요구사항]
1) marketing_message: 고객 마케팅 메시지 30글자 이상 50글자 이내
2) synthesis: 800자 이상 1200자 이하 종합평가, 반드시 마침표(.)로 끝낼 것
   - 주요 만족 패턴 분석 (3가지 이상)
   - 마케팅 메시지 선정 이유
   - 채널별 실행 계획 (배너·푸시·SNS 등)

반드시 JSON만 출력: {schema}"""

    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "당신은 마케팅 기획자입니다. JSON만 출력합니다. synthesis 필드는 800자 이상 1200자 이하로 작성하며 반드시 완성된 문장으로 마침표(.)를 찍어 마무리합니다. 문장이 중간에 끊기지 않도록 주의합니다."},
                {"role": "user",   "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        data = json.loads((resp.choices[0].message.content or "").strip())
        msg  = str(data.get("marketing_message", "")).strip()[:50]
        syn  = str(data.get("synthesis", "")).strip()
        if _count_chars_ko(syn) < 500:
            syn += " 만족도 높은 중분류를 벤치마크 기준으로 다른 영역에 확산하고 캠페인 반응을 주간 점검하는 실행 계획을 권장합니다."
        return {"marketing_message": msg, "synthesis": _safe_truncate(syn)}
    except Exception as e:
        return {"marketing_message": "AI상담, 빠르고 정확하게", "synthesis": f"[LLM 오류] {e}"}


# ──────────────────────────────────────────────────────────────────────────────
# History 관리
# ──────────────────────────────────────────────────────────────────────────────

def load_all_history(history_dir: str = "data/history") -> list[dict]:
    """전체 주차 JSON 로드 — S3에서 읽기 (slug 기준 오름차순 정렬)."""
    client = _s3()
    records = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_S3_BUCKET, Prefix=f"{_S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                body = client.get_object(Bucket=_S3_BUCKET, Key=key)["Body"].read()
                records.append(json.loads(body.decode("utf-8")))
            except Exception:
                pass
    return sorted(records, key=lambda r: r.get("slug", ""))


def load_history_by_slug(slug: str, history_dir: str = "data/history") -> Optional[dict]:
    """
    특정 slug의 분석 결과 로드 — S3에서 읽기.
    - JSON(통계) + pkl(DataFrames) 모두 존재해야 완전한 결과 반환.
    - JSON만 있으면 통계만 반환 (Tab2 도메인 뷰는 사용 불가).
    """
    client = _s3()
    try:
        body = client.get_object(Bucket=_S3_BUCKET, Key=_s3_key(f"{slug}.json"))["Body"].read()
        result = json.loads(body.decode("utf-8"))
    except ClientError:
        return None

    try:
        pkl_body = client.get_object(Bucket=_S3_BUCKET, Key=_s3_key(f"{slug}.pkl"))["Body"].read()
        result.update(pickle.loads(pkl_body))
    except ClientError:
        pass

    return result


def save_to_history(result: dict, meta: dict, history_dir: str = "data/history"):
    """
    분석 결과를 S3에 저장.
    - {slug}.json : 경량 통계 (그래프·비교용)
    - {slug}.pkl  : DataFrame 전체 (Tab2 도메인 뷰용)
    동일 slug 재저장 시 덮어쓰기.
    """
    client = _s3()
    slug = meta["slug"]

    # 대분류 통계 dict 변환
    cat_stats: dict = {}
    ms = result.get("main_category_stats")
    if ms is not None and not ms.empty:
        for _, row in ms[ms["대분류"] != "전체합계"].iterrows():
            cat_stats[row["대분류"]] = {
                "만족":   int(row.get("만족", 0)),
                "불만족": int(row.get("불만족", 0)),
                "중립":   int(row.get("중립", 0)),
            }

    stats = {
        "slug":         slug,
        "date_label":   meta.get("date_label", ""),
        "start":        meta.get("start", ""),
        "end":          meta.get("end", ""),
        "uploaded_at":  datetime.datetime.now().isoformat(),
        "satisfied":    result.get("satisfied", 0),
        "dissatisfied": result.get("dissatisfied", 0),
        "neutral":      result.get("neutral", 0),
        "total":        result.get("total", 0),
        "category_stats":  cat_stats,
        "diss_synthesis":  result.get("diss_synthesis", ""),
        "sat_llm_result":  result.get("sat_llm_result", {}),
    }
    json_bytes = json.dumps(stats, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        Bucket=_S3_BUCKET,
        Key=_s3_key(f"{slug}.json"),
        Body=json_bytes,
        ContentType="application/json",
    )

    # DataFrames pickle 저장
    pkl_data = {
        k: result[k]
        for k in ["detail_df", "session_eval_df", "main_category_stats", "top10_satisfied", "top10_dissatisfied"]
        if k in result
    }
    client.put_object(
        Bucket=_S3_BUCKET,
        Key=_s3_key(f"{slug}.pkl"),
        Body=pickle.dumps(pkl_data),
        ContentType="application/octet-stream",
    )


def is_already_analyzed(slug: str, history_dir: str = "data/history") -> bool:
    """
    slug에 해당하는 분석 결과가 S3에 완전히 존재하는지 확인한다.
    (JSON + pkl 둘 다 있어야 True)
    app.py / CLI에서 LLM 재호출 전에 먼저 체크하여 불필요한 분석을 방지한다.
    """
    client = _s3()

    def _exists(key: str) -> bool:
        try:
            client.head_object(Bucket=_S3_BUCKET, Key=key)
            return True
        except ClientError:
            return False

    return _exists(_s3_key(f"{slug}.json")) and _exists(_s3_key(f"{slug}.pkl"))


# ──────────────────────────────────────────────────────────────────────────────
# 메인 파이프라인
# ──────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    df: pd.DataFrame,
    use_llm: bool = True,
    api_key: str = "",
    max_workers: int = 50,
    progress_callback=None,
    stop_event: Optional[threading.Event] = None,
    pause_event: Optional[threading.Event] = None,
) -> Optional[dict]:
    """
    정규화된 DataFrame을 받아 만족도 평가 결과 dict를 반환한다.
    엑셀 저장·화면 출력 책임은 호출자(app.py / chatbot_satisfaction_eval.py)에게 있다.
    stop_event가 설정되면 조기 종료하고 None을 반환한다.
    pause_event가 clear 상태이면 resume될 때까지 대기한다.

    progress_callback 시그니처:
        callback(step: str, current: int, total: int, message: str)
    """
    def _cb(step: str, current: int, total: int, message: str = ""):
        if progress_callback:
            progress_callback(step, current, total, message)

    client, model_name = init_client(api_key=api_key, use_llm=use_llm)

    # 1) 발화 단위 분석
    _cb("utterance", 0, len(df), "발화 분석 중...")
    utter_results = df[TEXT_COL].apply(analyze_utterance)
    for key in ["has_profanity", "has_negative", "has_gratitude", "asks_counselor", "agent_limit"]:
        df[key] = utter_results.map(lambda x, k=key: x[k])
    _cb("utterance", len(df), len(df), "발화 분석 완료")

    if stop_event and stop_event.is_set():
        return None

    # 2) 세션 평가 (병렬)
    session_groups  = list(df.groupby(SESSION_COL))
    total_sessions  = len(session_groups)
    session_results: dict = {}

    _cb("sessions", 0, total_sessions, f"세션 평가 준비 중... (총 {total_sessions}개)")

    llm_cache = _SessionLLMCache() if client is not None else None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(evaluate_session, sd, client, model_name, llm_cache): sd[0]
            for sd in session_groups
        }
        for future in concurrent.futures.as_completed(futures):
            # pause: resume될 때까지 대기 (stop이 오면 탈출)
            while pause_event and not pause_event.is_set():
                if stop_event and stop_event.is_set():
                    break
                time.sleep(0.2)
            # stop: 남은 pending 작업 취소 후 조기 종료
            if stop_event and stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                return None
            sid = futures[future]
            try:
                session_results[sid] = future.result()
            except Exception as e:
                session_results[sid] = {
                    "session_label":  "ERROR",
                    "session_reason": str(e),
                    "topic_summary":  "",
                    "category_main":  "기타",
                    "category_sub":   "기타",
                }
            _cb("sessions", len(session_results), total_sessions,
                f"세션 평가 중 ({len(session_results)}/{total_sessions})")

    if stop_event and stop_event.is_set():
        return None

    # 3) DataFrame에 매핑
    for col, key in [("평가결과","session_label"), ("평가이유","session_reason"),
                     ("대화요약","topic_summary"), ("대분류","category_main"), ("중분류","category_sub")]:
        default = "기타" if key in ("category_main","category_sub") else ""
        df[col] = df[SESSION_COL].map(lambda s, k=key, d=default: session_results.get(s, {}).get(k, d))

    # 4) detail_df 구성 (has_* 포함 → extract_key_turns에서 사용)
    base_cols = [SESSION_COL, TIMESTAMP_COL, SENDER_COL, TEXT_COL]
    if USER_ID_COL in df.columns:
        base_cols.insert(2, USER_ID_COL)
    flag_cols = ["has_profanity", "has_negative", "has_gratitude", "asks_counselor", "agent_limit"]
    eval_cols = ["대화요약", "대분류", "중분류", "평가결과", "평가이유"]
    detail_df = df[base_cols + flag_cols + eval_cols].copy()

    # 5) session_eval_df
    session_eval_df = pd.DataFrame([
        {
            "session_id":     sid,
            "session_label":  r.get("session_label", "ERROR"),
            "session_reason": r.get("session_reason", ""),
            "topic_summary":  r.get("topic_summary", ""),
            "category_main":  r.get("category_main", "기타"),
            "category_sub":   r.get("category_sub", "기타"),
        }
        for sid, r in session_results.items()
    ])

    # 6) 대분류 통계
    main_stats = pd.crosstab(
        session_eval_df["category_main"],
        session_eval_df["session_label"],
        margins=True, margins_name="전체합계",
    )
    for col in ["만족", "불만족", "중립"]:
        if col not in main_stats.columns:
            main_stats[col] = 0
    main_stats["만족 비율(%)"]  = (main_stats["만족"]  / main_stats["전체합계"] * 100).round(1)
    main_stats["불만족 비율(%)"] = (main_stats["불만족"] / main_stats["전체합계"] * 100).round(1)
    main_stats = main_stats[["만족","불만족","중립","전체합계","만족 비율(%)","불만족 비율(%)"]].reset_index()
    main_stats.rename(columns={"category_main": "대분류"}, inplace=True)

    # 7) Top10 테이블
    top10_diss = build_top10_category_table(session_eval_df, detail_df, "불만족", "불만족 건수")
    top10_sat  = build_top10_category_table(session_eval_df, detail_df, "만족",   "만족 건수")

    # 8) LLM 종합 분석
    _cb("synthesis", 0, 2, "LLM 종합 분석 준비 중...")
    if stop_event and stop_event.is_set():
        return None

    _cb("synthesis", 1, 2, "불만족 Top10 종합 분석 중...")
    diss_synthesis = llm_synthesize_dissatisfaction_top10(top10_diss, client, model_name)

    if stop_event and stop_event.is_set():
        return None

    _cb("synthesis", 2, 2, "만족 Top10 종합 분석 중...")
    sat_llm_result = llm_synthesize_satisfaction_top10(top10_sat, client, model_name)

    _cb("done", 2, 2, "분석 완료!")

    label_counts = session_eval_df["session_label"].value_counts()

    return {
        "satisfied":            int(label_counts.get("만족", 0)),
        "dissatisfied":         int(label_counts.get("불만족", 0)),
        "neutral":              int(label_counts.get("중립", 0)),
        "total":                len(session_eval_df),
        "detail_df":            detail_df,
        "session_eval_df":      session_eval_df,
        "main_category_stats":  main_stats,
        "top10_satisfied":      top10_sat,
        "top10_dissatisfied":   top10_diss,
        "diss_synthesis":       diss_synthesis,
        "sat_llm_result":       sat_llm_result,
    }
