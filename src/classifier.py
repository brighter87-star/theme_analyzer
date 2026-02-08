import json
import logging
from pathlib import Path

import anthropic
import yaml

from config.settings import Settings
from db.repository import Repository
from utils.industry_resolver import resolve_industries
from utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# 고정 섹터 코드 - 코드에서 groupby/필터에 사용
VALID_SECTORS = [
    "semiconductor", "ai", "energy", "battery", "bio",
    "defense", "auto", "robot", "media", "shipbuilding",
    "finance", "software", "telecom", "consumer", "materials",
    "construction", "quantum", "cybersecurity", "blockchain", "other",
]

# Claude가 한글/GICS 섹터를 반환할 때 영문 고정 코드로 매핑
SECTOR_KO_TO_EN = {
    # 한글
    "반도체": "semiconductor", "정보기술": "software", "소프트웨어": "software",
    "에너지": "energy", "배터리": "battery", "바이오": "bio", "헬스케어": "bio",
    "방산": "defense", "방위": "defense", "자동차": "auto", "운송": "auto",
    "로봇": "robot", "미디어": "media", "조선": "shipbuilding",
    "금융": "finance", "통신": "telecom", "소비재": "consumer", "유통": "consumer",
    "소재": "materials", "화학": "materials", "자재": "materials",
    "건설": "construction", "양자": "quantum", "보안": "cybersecurity",
    "블록체인": "blockchain", "암호화폐": "blockchain", "유틸리티": "energy",
    "기타": "other", "자산운용": "finance", "보험": "finance",
    "부동산": "construction", "기업인수합병": "finance", "산업": "materials",
    "전력": "energy", "항공": "defense", "우주": "defense", "게임": "media",
    "엔터": "media", "제약": "bio", "의료": "bio",
    # GICS 영문 → 고정 코드
    "Communication Services": "media", "Industrials": "materials",
    "Consumer Discretionary": "consumer", "Consumer Staples": "consumer",
    "Health Care": "bio", "Information Technology": "software",
    "Real Estate": "construction", "Utilities": "energy",
    "Financials": "finance", "Materials": "materials",
    "Energy": "energy",
}

# 미리 정의된 테마 목록 (객관식)
PREDEFINED_THEMES_KR = {
    "semiconductor": [
        "HBM메모리", "AI반도체", "반도체장비", "반도체소재", "시스템반도체", "파운드리",
    ],
    "ai": ["AI소프트웨어", "AI데이터센터"],
    "energy": ["전력기기/변압기", "원전/SMR", "태양광/신재생", "수소/연료전지"],
    "battery": ["2차전지/배터리", "배터리소재"],
    "bio": ["바이오/신약", "의료기기/의료AI", "헬스케어"],
    "defense": ["방산", "우주항공", "드론/UAV"],
    "auto": ["전기차/자율주행", "자동차부품"],
    "robot": ["로봇/자동화"],
    "media": ["게임", "엔터/미디어", "광고", "K-콘텐츠"],
    "shipbuilding": ["조선/해운"],
    "finance": ["금융/보험", "리츠/부동산", "증권"],
    "software": ["클라우드/SaaS", "플랫폼"],
    "telecom": ["통신/5G", "네트워크장비"],
    "consumer": ["화장품/뷰티", "K-푸드/식음료", "유통/소비재", "여행/레저", "담배"],
    "materials": ["화학/소재", "디스플레이", "전자부품", "철강/비철금속"],
    "construction": ["건설/인프라"],
    "quantum": ["양자컴퓨팅"],
    "cybersecurity": ["사이버보안"],
    "blockchain": ["블록체인/암호화폐"],
}

PREDEFINED_THEMES_US = {
    "semiconductor": [
        "AI칩/GPU", "반도체장비", "반도체패키징", "메모리",
    ],
    "ai": ["AI인프라/클라우드", "AI소프트웨어/에이전트"],
    "energy": ["청정에너지/전력", "원전/SMR/우라늄", "석유/가스"],
    "battery": ["배터리/리튬"],
    "bio": ["바이오테크/제약", "의료서비스", "대마초/Cannabis"],
    "defense": ["방산/우주항공", "드론/UAV", "스페이스"],
    "auto": ["전기차/EV", "자율주행"],
    "robot": ["로봇/자동화"],
    "media": ["디지털미디어/스트리밍", "소셜미디어"],
    "finance": ["핀테크", "금융/보험"],
    "software": ["SaaS/소프트웨어", "전자상거래", "사이버보안"],
    "telecom": ["네트워크/통신"],
    "consumer": ["소비재/유통", "식음료", "럭셔리/의류"],
    "materials": ["산업재/소재", "광물/희토류"],
    "blockchain": ["블록체인/암호화폐", "스테이블코인/DeFi"],
    "quantum": ["양자컴퓨팅"],
}


def _build_theme_guide(market: str) -> str:
    """미리 정의된 테마 목록을 프롬프트용 문자열로 변환."""
    themes = PREDEFINED_THEMES_KR if market == "KR" else PREDEFINED_THEMES_US
    lines = []
    for sector, theme_list in themes.items():
        for t in theme_list:
            lines.append(f"  - {t} (sector: {sector})")
    return "\n".join(lines)


CLASSIFICATION_PROMPT = """당신은 주식 테마 분류 전문가입니다.
시장: {market_label}

아래 종목들을 **산업 테마**별로 분류해주세요.

## 테마 목록 (반드시 이 중에서 선택, 해당 없으면 가장 가까운 것 선택):
{theme_guide}
{existing_themes_section}

## 오늘 언급된 종목들:
{stock_list}

## 핵심 규칙:
1. 테마는 반드시 위 목록에서 선택. 목록에 없는 새 테마는 정말 필요한 경우에만 최소한으로 생성
2. **금지 테마**: "신고가", "외국인매매", "기관매매", "실적발표", "수급" 등 시장 이벤트/매매 동향은 테마가 아님. 이런 맥락의 종목도 반드시 산업 테마로 분류
3. sector는 반드시 영문 고정 코드: semiconductor, ai, energy, battery, bio, defense, auto, robot, media, shipbuilding, finance, software, telecom, consumer, materials, construction, quantum, cybersecurity, blockchain, other
4. 각 종목은 가장 적합한 1개 테마에만 배정 (정말 두 테마에 걸치는 경우만 2개)
5. reason은 15자 이내, 해당 종목의 사업 내용 기반
6. ticker가 빈 종목은 제외
7. **industry 힌트가 있으면 반드시 참고**: 종목의 실제 업종(industry)이 제공된 경우, 메시지 맥락보다 실제 업종을 우선하여 sector와 테마를 결정. 예: industry가 "Semiconductor Equipment"이면 "AI칩/GPU"가 아닌 "반도체장비"로 분류

반드시 아래 JSON 형식으로만 응답하세요:
{{
  "테마이름": [
    {{"name": "종목명", "ticker": "티커", "sector": "섹터코드", "reason": "분류 이유"}},
    ...
  ]
}}"""

MERGE_SMALL_THEMES_PROMPT = """아래에 종목이 1개뿐인 테마들이 있습니다.
이 종목들을 기존 테마에 합치거나, 서로 묶어 2개 이상인 새 테마로 재분류해주세요.

기존 테마 목록 (여기에 합칠 수 있음):
{existing_themes}

재분류 대상 (1종목 테마):
{orphan_list}

규칙:
1. 기존 테마에 합칠 수 있으면 그 테마 이름을 그대로 사용
2. 기존 테마에 맞지 않으면 2개 이상 묶어 새 테마 생성
3. 어디에도 맞지 않는 종목은 "기타" 테마로
4. sector는 반드시 영문 고정 코드: semiconductor, ai, energy, battery, bio, defense, auto, robot, media, shipbuilding, finance, software, telecom, consumer, materials, construction, quantum, cybersecurity, blockchain, other

반드시 아래 JSON 형식으로만 응답하세요:
{{
  "테마이름": [
    {{"name": "종목명", "ticker": "티커", "sector": "섹터코드", "reason": "분류 이유"}}
  ]
}}"""

SPLIT_THEME_PROMPT = """테마 '{theme_name}'에 {stock_count}개 종목이 있어 세분화가 필요합니다.

종목 목록:
{stock_list}

이 테마를 세분화된 하위 테마로 나눠주세요.

규칙:
1. 각 하위 테마는 최대 10개 종목
2. 하위 테마명은 구체적 한글 (예: "HBM메모리", "반도체장비")
3. sector는 기존 종목의 섹터코드 유지
4. 하위 테마에 1~2개 종목만 있다면 가장 가까운 테마에 합치세요

반드시 아래 JSON 형식으로만 응답하세요:
{{
  "하위테마명": [
    {{"name": "종목명", "ticker": "티커", "sector": "섹터코드", "reason": "분류 이유"}}
  ]
}}"""


class ThemeClassifier:
    def __init__(
        self,
        settings: Settings,
        repo: Repository,
        rate_limiter: RateLimiter,
    ):
        self.settings = settings
        self.repo = repo
        self.rate_limiter = rate_limiter
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def classify_daily(self, report_date: str) -> dict:
        # 이미 분류된 날짜면 DB에서 로드하여 재사용 (토큰 절약)
        existing = await self.repo.get_daily_classification(report_date)
        if existing and (existing.get("kr") or existing.get("us")):
            kr_count = sum(len(v) for v in existing.get("kr", {}).values())
            us_count = sum(len(v) for v in existing.get("us", {}).values())
            logger.info(
                f"Reusing existing classification for {report_date}: "
                f"KR {len(existing.get('kr', {}))} themes/{kr_count} stocks, "
                f"US {len(existing.get('us', {}))} themes/{us_count} stocks"
            )
            return existing

        mentions = await self.repo.get_daily_stock_mentions(report_date)
        if not mentions:
            logger.info(f"No stock mentions for {report_date}")
            return {"kr": {}, "us": {}}

        # yfinance 업종 정보 조회 (US 종목만, DB 캐싱)
        mentions = await resolve_industries(mentions, self.repo)

        kr_stocks = [m for m in mentions if m["market"] == "KR"]
        us_stocks = [m for m in mentions if m["market"] == "US"]

        themes = self._load_themes()

        kr_result = await self._classify_market(
            kr_stocks, themes.get("kr_themes", {}), "KR"
        )
        us_result = await self._classify_market(
            us_stocks, themes.get("us_themes", {}), "US"
        )

        # Store in DB
        await self._store_classifications(report_date, kr_result, "KR")
        await self._store_classifications(report_date, us_result, "US")

        logger.info(
            f"Classification for {report_date}: "
            f"KR {len(kr_result)} themes, US {len(us_result)} themes"
        )
        return {"kr": kr_result, "us": us_result}

    def _load_themes(self) -> dict:
        themes_path = self.settings.base_dir / "config" / "themes.yaml"
        try:
            with open(themes_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning("themes.yaml not found, using empty themes")
            return {}

    # 한 번의 API 호출에 보낼 최대 종목 수
    CLASSIFY_BATCH_SIZE = 35

    @staticmethod
    def _fix_sector(sector: str) -> str:
        """한글 섹터 → 영문 코드 변환. 이미 영문이면 그대로."""
        if sector in VALID_SECTORS:
            return sector
        return SECTOR_KO_TO_EN.get(sector, "other")

    async def _classify_market(
        self, stocks: list[dict], themes: dict, market: str
    ) -> dict:
        if not stocks:
            return {}

        # ticker가 없는 종목 필터링 (미해결 종목은 분류 불가)
        valid_stocks = [s for s in stocks if s.get("ticker", "").strip()]
        skipped = len(stocks) - len(valid_stocks)
        if skipped:
            logger.info(f"{market} ticker 없는 종목 {skipped}개 제외")
        if not valid_stocks:
            return {}
        stocks = valid_stocks

        market_label = "한국(KR)" if market == "KR" else "미국(US)"
        theme_guide = _build_theme_guide(market)

        # 종목이 많으면 배치 분할
        if len(stocks) <= self.CLASSIFY_BATCH_SIZE:
            batches = [stocks]
        else:
            batches = [
                stocks[i : i + self.CLASSIFY_BATCH_SIZE]
                for i in range(0, len(stocks), self.CLASSIFY_BATCH_SIZE)
            ]
            logger.info(
                f"{market} 종목 {len(stocks)}개 → {len(batches)}개 배치로 분할"
            )

        merged: dict[str, list] = {}
        accumulated_theme_names: list[str] = []  # 이전 배치에서 나온 테마명

        for batch_idx, batch in enumerate(batches):
            result = await self._classify_batch(
                batch, theme_guide, market_label,
                batch_idx, len(batches), accumulated_theme_names,
            )
            # 배치 결과 머지: 같은 테마면 종목 합침
            for theme_name, theme_stocks in result.items():
                if theme_name not in merged:
                    merged[theme_name] = []
                    accumulated_theme_names.append(theme_name)
                existing_tickers = {s.get("ticker") for s in merged[theme_name]}
                for s in theme_stocks:
                    if s.get("ticker") not in existing_tickers:
                        merged[theme_name].append(s)

        # Sector 코드 검증 + 한글→영문 변환
        for theme_stocks in merged.values():
            for s in theme_stocks:
                s["sector"] = self._fix_sector(s.get("sector", "other"))

        # 금지 테마 필터링: 시장 이벤트 테마 → "기타"로 이동
        banned_keywords = [
            "신고가", "매도", "매수", "수급", "실적발표", "순매도", "순매수",
            "기관", "외국인", "특수", "카테고리", "달성", "상위",
        ]
        cleaned: dict[str, list] = {}
        dumped: list = []
        for theme_name, theme_stocks in merged.items():
            if any(kw in theme_name for kw in banned_keywords):
                logger.info(f"금지 테마 필터링: '{theme_name}' ({len(theme_stocks)}종목)")
                dumped.extend(theme_stocks)
            else:
                cleaned[theme_name] = theme_stocks

        # 금지 테마에서 나온 종목들 → 사업 기반으로 재분류 시도
        if dumped:
            # ticker가 있는 것만 재분류
            reclassify = [s for s in dumped if s.get("ticker", "").strip()]
            if reclassify:
                logger.info(f"금지 테마 종목 {len(reclassify)}개 재분류 시도")
                re_result = await self._classify_batch(
                    # stock 포맷을 맞춤
                    [{"ticker": s.get("ticker", ""), "name_ko": s.get("name", ""),
                      "mention_count": 1, "aggregated_context": s.get("reason", "")}
                     for s in reclassify],
                    theme_guide, market_label, 0, 1,
                    list(cleaned.keys()),
                )
                for t_name, t_stocks in re_result.items():
                    for rs in t_stocks:
                        rs["sector"] = self._fix_sector(rs.get("sector", "other"))
                    if t_name in cleaned:
                        existing_tickers = {s.get("ticker") for s in cleaned[t_name]}
                        for rs in t_stocks:
                            if rs.get("ticker") not in existing_tickers:
                                cleaned[t_name].append(rs)
                    else:
                        cleaned[t_name] = t_stocks

        merged = cleaned

        # Handle overflow (> 10 stocks per theme → re-split)
        sized: dict[str, list] = {}
        for theme_name, theme_stocks in merged.items():
            if len(theme_stocks) > 10:
                sub_themes = await self._split_theme(theme_name, theme_stocks)
                for sub_name, sub_stocks in sub_themes.items():
                    if len(sub_stocks) > 10:
                        deeper = await self._split_theme(sub_name, sub_stocks)
                        sized.update(deeper)
                    else:
                        sized[sub_name] = sub_stocks
            else:
                sized[theme_name] = theme_stocks

        # 1종목 테마 통합
        final = await self._merge_small_themes(sized)

        return final

    async def _classify_batch(
        self,
        stocks: list[dict],
        theme_guide: str,
        market_label: str,
        batch_idx: int,
        total_batches: int,
        existing_theme_names: list[str],
    ) -> dict:
        def _fmt_stock(s: dict) -> str:
            name = s.get('name_ko') or s.get('name_en') or s['ticker']
            industry = s.get('industry') or ''
            ticker_part = f"ticker: {s['ticker']}"
            if industry:
                ticker_part += f", industry: {industry}"
            ctx = (s.get('aggregated_context') or '')[:80]
            return f"- {name} ({ticker_part}): 언급 {s['mention_count']}회, 맥락: {ctx}"

        stock_list = "\n".join(_fmt_stock(s) for s in stocks)

        # 이전 배치에서 이미 사용된 테마명 전달
        if existing_theme_names:
            existing_section = (
                "\n\n이전 배치에서 이미 사용된 테마 (동일한 이름 사용 필수):\n"
                + "\n".join(f"  - {n}" for n in existing_theme_names)
                + "\n"
            )
        else:
            existing_section = ""

        prompt = CLASSIFICATION_PROMPT.format(
            market_label=market_label,
            theme_guide=theme_guide,
            existing_themes_section=existing_section,
            stock_list=stock_list,
        )

        await self.rate_limiter.acquire("claude")
        response = await self.client.messages.create(
            model=self.settings.claude_model,
            max_tokens=self.settings.claude_max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip()

        # stop_reason이 max_tokens이면 잘린 것 → 경고
        if response.stop_reason == "max_tokens":
            logger.warning(
                f"배치 {batch_idx+1}/{total_batches}: 응답이 max_tokens에서 잘림 "
                f"({len(stocks)}종목)"
            )

        classification = self._parse_json_response(raw)
        if classification is None:
            logger.error(
                f"배치 {batch_idx+1}/{total_batches}: JSON 파싱 실패 "
                f"({len(stocks)}종목)"
            )
            return {}

        if total_batches > 1:
            theme_count = len(classification)
            stock_count = sum(len(v) for v in classification.values())
            logger.info(
                f"배치 {batch_idx+1}/{total_batches}: "
                f"{theme_count}테마 {stock_count}종목 분류 완료"
            )

        return classification

    async def _merge_small_themes(self, themes: dict[str, list]) -> dict[str, list]:
        """1종목 테마를 기존 테마에 통합하거나 서로 묶어 재분류."""
        big_themes = {}
        orphans = []

        for name, stocks in themes.items():
            if len(stocks) >= 2:
                big_themes[name] = stocks
            else:
                for s in stocks:
                    s["_original_theme"] = name
                    orphans.append(s)

        if not orphans:
            return themes

        logger.info(f"1종목 테마 {len(orphans)}개 → 통합 재분류")

        existing_themes_str = "\n".join(
            f"- {name} ({len(stocks)}종목)" for name, stocks in big_themes.items()
        )
        orphan_str = "\n".join(
            f"- {s.get('name', s.get('ticker', '?'))} "
            f"(ticker: {s.get('ticker', '?')}, sector: {s.get('sector', 'other')}) "
            f"- 원래 테마: {s.get('_original_theme', '?')}"
            for s in orphans
        )

        prompt = MERGE_SMALL_THEMES_PROMPT.format(
            existing_themes=existing_themes_str or "(없음)",
            orphan_list=orphan_str,
        )

        await self.rate_limiter.acquire("claude")
        response = await self.client.messages.create(
            model=self.settings.claude_model,
            max_tokens=self.settings.claude_max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip()
        result = self._parse_json_response(raw)

        if result is None:
            logger.warning("소형 테마 통합 실패 → '기타' 테마로 합침")
            big_themes["기타"] = big_themes.get("기타", [])
            for s in orphans:
                s.pop("_original_theme", None)
                big_themes["기타"].append(s)
            return big_themes

        # 통합 결과를 big_themes에 합침
        for theme_name, theme_stocks in result.items():
            for s in theme_stocks:
                s["sector"] = self._fix_sector(s.get("sector", "other"))
            if theme_name in big_themes:
                existing_tickers = {s.get("ticker") for s in big_themes[theme_name]}
                for s in theme_stocks:
                    if s.get("ticker") not in existing_tickers:
                        big_themes[theme_name].append(s)
            else:
                big_themes[theme_name] = theme_stocks

        # _original_theme 필드 정리
        for stocks in big_themes.values():
            for s in stocks:
                s.pop("_original_theme", None)

        merged_count = sum(len(v) for v in result.values())
        logger.info(f"소형 테마 통합 완료: {len(orphans)}종목 → {len(result)}테마로 재분류")
        return big_themes

    async def _split_theme(self, theme_name: str, stocks: list[dict]) -> dict:
        stock_list = "\n".join(
            f"- {s.get('name', s.get('ticker', '?'))}: {s.get('reason', '')}"
            for s in stocks
        )

        prompt = SPLIT_THEME_PROMPT.format(
            theme_name=theme_name,
            stock_count=len(stocks),
            stock_list=stock_list,
        )

        await self.rate_limiter.acquire("claude")
        response = await self.client.messages.create(
            model=self.settings.claude_model,
            max_tokens=self.settings.claude_max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip()
        result = self._parse_json_response(raw)
        if result is None:
            # Fallback: keep original (may exceed 10)
            logger.warning(f"Could not split theme '{theme_name}', keeping as-is")
            return {theme_name: stocks[:10]}

        # Validate each sub-theme has <= 10
        for sub_name, sub_stocks in list(result.items()):
            if len(sub_stocks) > 10:
                result[sub_name] = sub_stocks[:10]

        return result

    async def _store_classifications(
        self, report_date: str, classification: dict, market: str
    ):
        for theme_name, stocks in classification.items():
            theme_id = await self.repo.get_or_create_theme(
                name_ko=theme_name,
                name_en=None,
                market=market,
            )

            for stock_info in stocks:
                ticker = stock_info.get("ticker", "")
                name = stock_info.get("name", "")

                # Find stock in DB
                results = await self.repo.search_stock(ticker or name)
                market_results = [r for r in results if r["market"] == market]
                if market_results:
                    stock_id = market_results[0]["id"]
                else:
                    stock_id = await self.repo.get_or_create_stock(
                        ticker=ticker or name,
                        name_ko=name if market == "KR" else None,
                        name_en=name if market == "US" else None,
                        market=market,
                    )

                await self.repo.insert_daily_stock_theme(
                    report_date=report_date,
                    stock_id=stock_id,
                    theme_id=theme_id,
                    mention_count=stock_info.get("mention_count", 1),
                    reason=stock_info.get("reason", ""),
                    sector=stock_info.get("sector", "other"),
                )

    def _parse_json_response(self, text: str) -> dict | list | None:
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            import re

            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except json.JSONDecodeError:
                    pass
            logger.warning(f"Cannot parse JSON: {text[:200]}...")
            return None
