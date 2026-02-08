import re

# 한국 종목 약어/별명 → 정식명
KR_ALIASES: dict[str, str] = {
    "삼전": "삼성전자",
    "삼디": "삼성SDI",
    "하닉": "SK하이닉스",
    "엘솔": "LG에너지솔루션",
    "엘에솔": "LG에너지솔루션",
    "카카": "카카오",
    "네버": "NAVER",
    "에프엠": "에코프로비엠",
    "에코비": "에코프로비엠",
    "포홀": "포스코홀딩스",
    "포퓨": "포스코퓨처엠",
    "한에어": "한화에어로스페이스",
    "한에로": "한화에어로스페이스",
    "현차": "현대차",
    "기차": "기아",
    "셀트": "셀트리온",
    "한미반": "한미반도체",
    "리노": "리노공업",
    "두산밥": "두산밥캣",
}

# 미국 종목 한글 표기 → 티커
US_KO_ALIASES: dict[str, str] = {
    "엔비디아": "NVDA",
    "테슬라": "TSLA",
    "애플": "AAPL",
    "마이크로소프트": "MSFT",
    "구글": "GOOGL",
    "알파벳": "GOOGL",
    "아마존": "AMZN",
    "메타": "META",
    "페이스북": "META",
    "브로드컴": "AVGO",
    "마이크론": "MU",
    "팔란티어": "PLTR",
    "코인베이스": "COIN",
    "슈퍼마이크로": "SMCI",
    "아이온큐": "IONQ",
    "리게티": "RGTI",
    "디웨이브": "QBTS",
    "사운드하운드": "SOUN",
    "크라우드스트라이크": "CRWD",
    "스노우플레이크": "SNOW",
    "세일즈포스": "CRM",
    "서비스나우": "NOW",
    "어도비": "ADBE",
    "넷플릭스": "NFLX",
    "우버": "UBER",
    "로블록스": "RBLX",
    "유니티": "U",
    "에이엠디": "AMD",
    "인텔": "INTC",
    "퀄컴": "QCOM",
    "암": "ARM",
    "마벨": "MRVL",
    "시놉시스": "SNPS",
    "케이던스": "CDNS",
    "ASML": "ASML",
    "램리서치": "LRCX",
    "어플라이드": "AMAT",
    "도큐사인": "DOCU",
}


def normalize_stock_name(raw: str) -> str:
    """기본 정규화: 공백 정리, 괄호 내용 제거."""
    raw = raw.strip()
    raw = re.sub(r"\s+", " ", raw)
    raw = re.sub(r"\(.*?\)", "", raw).strip()
    return raw


def resolve_kr_alias(name: str) -> str | None:
    """한국 종목 약어를 정식명으로 변환. 없으면 None."""
    return KR_ALIASES.get(name)


def resolve_us_ko_alias(name: str) -> str | None:
    """한글로 쓴 미국 종목명을 티커로 변환. 없으면 None."""
    return US_KO_ALIASES.get(name)


def is_likely_us_ticker(text: str) -> bool:
    """대문자 1~5자 영문 → 미국 티커일 가능성."""
    return bool(re.match(r"^[A-Z]{1,5}$", text))


def extract_potential_tickers(text: str) -> list[str]:
    """텍스트에서 미국 티커 후보 추출 ($NVDA 또는 단독 대문자)."""
    # $NVDA 패턴
    dollar_tickers = re.findall(r"\$([A-Z]{1,5})\b", text)
    # 단독 대문자 단어 (2자 이상)
    standalone = re.findall(r"\b([A-Z]{2,5})\b", text)
    # 일반적인 영단어 제외
    noise = {
        "THE", "AND", "FOR", "BUT", "NOT", "ARE", "WAS", "HAS", "HAD",
        "HBM", "AI", "ETF", "IPO", "CEO", "CFO", "GDP", "CPI", "PPI",
        "FOMC", "FED", "SEC", "USD", "KRW", "JPY", "EUR", "API", "EPS",
        "PER", "PBR", "ROE", "ROA", "BPS", "SMA", "RSI", "MACD",
    }
    standalone = [t for t in standalone if t not in noise]
    return list(dict.fromkeys(dollar_tickers + standalone))  # dedupe, preserve order
