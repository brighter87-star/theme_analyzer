import logging
from typing import Optional

from rapidfuzz import fuzz, process

from db.repository import Repository
from utils.text_utils import (
    KR_ALIASES,
    US_KO_ALIASES,
    normalize_stock_name,
    resolve_kr_alias,
    resolve_us_ko_alias,
)

logger = logging.getLogger(__name__)


class StockRegistry:
    def __init__(self, repo: Repository):
        self.repo = repo
        self._kr_name_to_ticker: dict[str, str] = {}
        self._kr_ticker_to_name: dict[str, str] = {}
        self._initialized = False

    async def initialize(self):
        """pykrx에서 한국 종목 마스터를 로드."""
        try:
            from pykrx import stock as pykrx_stock

            for market_name in ["KOSPI", "KOSDAQ"]:
                tickers = pykrx_stock.get_market_ticker_list(market=market_name)
                for ticker in tickers:
                    name = pykrx_stock.get_market_ticker_name(ticker)
                    self._kr_name_to_ticker[name] = ticker
                    self._kr_ticker_to_name[ticker] = name
            logger.info(
                f"Loaded {len(self._kr_name_to_ticker)} Korean stocks from pykrx"
            )
        except Exception as e:
            logger.warning(f"pykrx initialization failed, using DB cache only: {e}")
        self._initialized = True

    async def resolve_stock(
        self, raw_name: str, market_hint: str
    ) -> Optional[int]:
        """
        종목명/티커를 DB stock ID로 변환.
        없으면 새로 생성. 해석 불가면 None.
        """
        name = normalize_stock_name(raw_name)
        if not name:
            return None

        if market_hint == "KR":
            return await self._resolve_kr(name)
        else:
            return await self._resolve_us(name)

    async def _resolve_kr(self, name: str) -> Optional[int]:
        # 1) 약어 사전
        alias_resolved = resolve_kr_alias(name)
        if alias_resolved:
            name = alias_resolved

        # 2) pykrx 정확 매칭
        ticker = self._kr_name_to_ticker.get(name)
        if ticker:
            exchange = self._determine_kr_exchange(ticker)
            return await self.repo.get_or_create_stock(
                ticker=ticker, name_ko=name, name_en=None,
                market="KR", exchange=exchange,
            )

        # 3) 티커 직접 입력 (6자리 숫자)
        if name.isdigit() and len(name) == 6:
            stock_name = self._kr_ticker_to_name.get(name)
            if stock_name:
                exchange = self._determine_kr_exchange(name)
                return await self.repo.get_or_create_stock(
                    ticker=name, name_ko=stock_name, name_en=None,
                    market="KR", exchange=exchange,
                )

        # 4) 퍼지 매칭 (80% 이상)
        if self._kr_name_to_ticker:
            match = process.extractOne(
                name,
                self._kr_name_to_ticker.keys(),
                scorer=fuzz.ratio,
                score_cutoff=80,
            )
            if match:
                matched_name = match[0]
                ticker = self._kr_name_to_ticker[matched_name]
                exchange = self._determine_kr_exchange(ticker)
                logger.debug(f"Fuzzy matched: '{name}' -> '{matched_name}' ({match[1]:.0f}%)")
                return await self.repo.get_or_create_stock(
                    ticker=ticker, name_ko=matched_name, name_en=None,
                    market="KR", exchange=exchange,
                )

        logger.debug(f"Could not resolve KR stock: '{name}'")
        return None

    async def _resolve_us(self, name: str) -> Optional[int]:
        # 1) 한글 약어 → 티커
        ticker = resolve_us_ko_alias(name)
        if ticker:
            return await self.repo.get_or_create_stock(
                ticker=ticker, name_ko=name, name_en=None,
                market="US", exchange=None,
            )

        # 2) 이미 영문 티커인 경우
        if name.isupper() and 1 <= len(name) <= 5 and name.isalpha():
            return await self.repo.get_or_create_stock(
                ticker=name, name_ko=None, name_en=None,
                market="US", exchange=None,
            )

        # 3) DB에서 검색
        results = await self.repo.search_stock(name)
        us_results = [r for r in results if r["market"] == "US"]
        if us_results:
            return us_results[0]["id"]

        logger.debug(f"Could not resolve US stock: '{name}'")
        return None

    def _determine_kr_exchange(self, ticker: str) -> str:
        try:
            from pykrx import stock as pykrx_stock

            kospi = pykrx_stock.get_market_ticker_list(market="KOSPI")
            if ticker in kospi:
                return "KOSPI"
            return "KOSDAQ"
        except Exception:
            return "UNKNOWN"
