import re

# 컬럼명
SESSION_COL   = "session ID"
TIMESTAMP_COL = "발화 시작일시"
USER_ID_COL   = "서비스 관리 번호"
SENDER_COL    = "sender_type"
TEXT_COL      = "utterance"

# 정규표현식
PROFANITY_PATTERN  = re.compile(r"(씨\s*발|시\s*발|ㅅ\s*ㅂ|병\s*신|개\s*새끼|꺼\s*져)", re.IGNORECASE)
NEGATIVE_PATTERN   = re.compile(r"(짜증|화나|답답|최악|불만|실망|엉망|에휴)", re.IGNORECASE)
GRATITUDE_PATTERN  = re.compile(r"(감사|고마워|고맙|덕분에|수고)", re.IGNORECASE)
COUNSELOR_PATTERN  = re.compile(r"(상담원|상담사|사람이랑|직원 연결)", re.IGNORECASE)
AGENT_LIMIT_PATTERN = re.compile(r"(아직 제가 답변드릴 수 없는|전문 상담원에게)", re.IGNORECASE)

EXCLUDED_GRATITUDE_TERMS = ["고객감사제", "고객감사 패키지", "T멤버십 고객감사"]

ALLOWED_CATEGORY_MAIN = [
    "CS0(데이터/리필)",
    "CS1(요금제)",
    "CS2(납부/청구/할부)",
    "CS3(부가서비스/결합)",
    "CS4(로밍)",
    "CS5(기기구매/개통)",
    "CS6(T멤버십/구독)",
    "기타",
]

CATEGORY_SUB_MAP: dict[str, list[str]] = {
    "CS0(데이터/리필)": ["데이터사용량/잔여량", "음성잔여량", "데이터선물하기", "리필쿠폰"],
    "CS1(요금제)": ["요금제혜택", "요금제추천", "요금제검색", "요금제변경", "요금제조회"],
    "CS2(납부/청구/할부)": ["미납문의", "납부문의", "요금문의", "약정/할인반환금", "기기할부"],
    "CS3(부가서비스/결합)": [
        "부가서비스가입", "부가서비스해지", "부가서비스변경", "부가서비스조회",
        "결합상태조회", "결합가입", "결합해지", "결합변경",
    ],
    "CS4(로밍)": ["로밍요금제", "로밍이용내역", "로밍해지", "로밍데이터", "로밍사용이슈"],
    "CS5(기기구매/개통)": ["기기구매", "개통", "기기변경", "USIM개통", "USIM해지", "eSIM가입", "주문/배송문의"],
    "CS6(T멤버십/구독)": [
        "T멤버십혜택", "T멤버십조회", "T멤버십변경", "T멤버십해지",
        "구독상태조회", "구독신청", "구독해지",
    ],
    "기타": ["FAQ", "기타"],
}
