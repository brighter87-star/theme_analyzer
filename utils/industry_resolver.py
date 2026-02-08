import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from db.repository import Repository

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=2)


def _fetch_yfinance_industry(ticker: str) -> str | None:
    """yfinance에서 업종(industry) 조회. 동기 함수 (스레드풀에서 실행)."""
    try:
        import yfinance as yf

        info = yf.Ticker(ticker).info
        return info.get("industry")
    except Exception as e:
        logger.debug(f"yfinance lookup failed for {ticker}: {e}")
        return None


async def resolve_industries(stocks: list[dict], repo: Repository) -> list[dict]:
    """
    US 종목의 industry를 조회하여 stocks 리스트에 추가.
    - DB에 이미 industry가 있으면 그대로 사용
    - 없으면 yfinance에서 조회 후 DB에 캐싱
    - KR 종목은 스킵
    """
    loop = asyncio.get_event_loop()
    need_lookup: list[dict] = []

    for s in stocks:
        if s.get("industry"):
            continue
        if s.get("market") != "US":
            continue
        need_lookup.append(s)

    if not need_lookup:
        return stocks

    logger.info(f"yfinance 업종 조회: {len(need_lookup)}개 US 종목")

    for s in need_lookup:
        ticker = s["ticker"]
        try:
            industry = await loop.run_in_executor(
                _executor, _fetch_yfinance_industry, ticker
            )
            if industry:
                s["industry"] = industry
                await repo.update_stock_industry(s["stock_id"], industry)
                logger.debug(f"{ticker} → {industry}")
        except Exception as e:
            logger.debug(f"Industry resolve failed for {ticker}: {e}")

    resolved = sum(1 for s in need_lookup if s.get("industry"))
    logger.info(f"yfinance 업종 조회 완료: {resolved}/{len(need_lookup)}개 성공")

    return stocks
