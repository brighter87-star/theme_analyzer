"""
60시간 데이터 테스트 (금요일 00:00 KST ~ 현재)
수집 → 분석 → 분류 → 리포트 전체 파이프라인 실행
텔레그램 전송 없이 콘솔 출력만
"""
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))

KST = ZoneInfo("Asia/Seoul")

from config.settings import Settings
from db.database import Database
from db.migrations import run_migrations
from db.repository import Repository
from src.analyzer import StockAnalyzer
from src.classifier import ThemeClassifier
from src.collector import MessageCollector
from src.reporter import ReportGenerator
from utils.rate_limiter import RateLimiter
from utils.stock_registry import StockRegistry


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("test_60h.log", encoding="utf-8"),
        ],
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def main():
    setup_logging()
    logger = logging.getLogger("test_60h")

    now = datetime.now(KST)
    logger.info(f"테스트 시작: {now.strftime('%Y-%m-%d %H:%M KST')}")

    # Settings 로드 후 lookback_hours를 60으로 변경
    settings = Settings()
    settings.lookback_hours = 60
    logger.info(f"lookback_hours = {settings.lookback_hours} (금요일 00:00 KST부터)")

    settings.image_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    # DB 초기화
    db = Database(settings.db_path)
    await db.initialize()
    await run_migrations(db)
    repo = Repository(db)

    # Rate limiter
    rate_limiter = RateLimiter()
    rate_limiter.add_bucket("claude", rate=settings.claude_rpm / 60, capacity=5)
    rate_limiter.add_bucket("telegram", rate=0.5, capacity=5)

    # Stock registry
    registry = StockRegistry(repo)
    await registry.initialize()

    # Collector
    collector = MessageCollector(settings, repo)
    await collector.initialize()

    try:
        # ── Step 1: 수집 ──
        logger.info("=" * 60)
        logger.info("STEP 1: 메시지 수집 (60시간)")
        logger.info("=" * 60)
        collection_stats = await collector.collect_all_channels()
        logger.info(f"수집 결과: {collection_stats}")
        print(f"\n--- 수집 완료 ---")
        print(f"  채널: {collection_stats['total_channels']}개")
        print(f"  메시지: {collection_stats['total_messages']}개")
        if collection_stats['errors']:
            print(f"  에러: {collection_stats['errors']}")

        # ── Step 2: 분석 ──
        logger.info("=" * 60)
        logger.info("STEP 2: 종목 추출 (Claude Haiku)")
        logger.info("=" * 60)
        analyzer = StockAnalyzer(settings, repo, registry, rate_limiter)
        analysis_stats = await analyzer.analyze_pending_messages()
        logger.info(f"분석 결과: {analysis_stats}")
        print(f"\n--- 분석 완료 ---")
        print(f"  텍스트 메시지: {analysis_stats['text_messages']}개")
        print(f"  이미지 메시지: {analysis_stats['image_messages']}개")
        print(f"  추출 종목: {analysis_stats['stocks_extracted']}개")
        print(f"  에러: {analysis_stats['errors']}건")

        # ── Step 3: 분류 (각 날짜별) ──
        logger.info("=" * 60)
        logger.info("STEP 3: 테마 분류")
        logger.info("=" * 60)

        classifier = ThemeClassifier(settings, repo, rate_limiter)
        reporter = ReportGenerator(settings, repo)

        # 금요일~일요일 날짜 계산
        today = now.date()
        dates = []
        for i in range(3):  # 오늘, 어제, 그저께
            d = today - timedelta(days=i)
            dates.append(d.strftime("%Y-%m-%d"))
        dates.reverse()  # 금→토→일 순서

        all_classification = {"kr": {}, "us": {}}

        for report_date in dates:
            mentions = await repo.get_daily_stock_mentions(report_date)
            if not mentions:
                print(f"\n  [{report_date}] 종목 언급 없음 - 건너뜀")
                continue

            print(f"\n  [{report_date}] 종목 언급 {len(mentions)}개 → 분류 중...")
            classification = await classifier.classify_daily(report_date)

            kr_count = sum(len(v) for v in classification.get("kr", {}).values())
            us_count = sum(len(v) for v in classification.get("us", {}).values())
            print(f"    KR: {len(classification.get('kr', {}))}테마 {kr_count}종목")
            print(f"    US: {len(classification.get('us', {}))}테마 {us_count}종목")

            # 누적
            for market in ["kr", "us"]:
                for theme, stocks in classification.get(market, {}).items():
                    if theme not in all_classification[market]:
                        all_classification[market][theme] = stocks
                    else:
                        existing_tickers = {s["ticker"] for s in all_classification[market][theme]}
                        for s in stocks:
                            if s["ticker"] not in existing_tickers:
                                all_classification[market][theme].append(s)

        # ── Step 4: 리포트 생성 ──
        logger.info("=" * 60)
        logger.info("STEP 4: 리포트 생성")
        logger.info("=" * 60)

        # 오늘 날짜로 리포트 생성
        report_date = today.strftime("%Y-%m-%d")
        message, csv_path = await reporter.generate_daily_report(
            report_date, all_classification
        )

        print(f"\n{'=' * 60}")
        print(f"리포트 ({report_date})")
        print(f"{'=' * 60}")
        print(message)
        print(f"\nCSV: {csv_path}")

        # 전체 요약
        print(f"\n{'=' * 60}")
        print("전체 요약")
        print(f"{'=' * 60}")
        print(f"기간: {dates[0]} ~ {dates[-1]} ({settings.lookback_hours}시간)")
        print(f"수집: {collection_stats['total_messages']}메시지 / {collection_stats['total_channels']}채널")
        print(f"분석: {analysis_stats['stocks_extracted']}종목 추출")
        total_kr = sum(len(v) for v in all_classification.get("kr", {}).values())
        total_us = sum(len(v) for v in all_classification.get("us", {}).values())
        kr_themes = len(all_classification.get("kr", {}))
        us_themes = len(all_classification.get("us", {}))
        print(f"분류: KR {kr_themes}테마/{total_kr}종목, US {us_themes}테마/{total_us}종목")

        # 테마별 상세
        for market_code, market_name in [("kr", "한국"), ("us", "미국")]:
            themes = all_classification.get(market_code, {})
            if themes:
                print(f"\n--- {market_name} 테마 ---")
                for theme_name, stocks in sorted(themes.items()):
                    stock_names = [s.get("name", s.get("ticker", "?")) for s in stocks]
                    print(f"  [{theme_name}] {', '.join(stock_names)}")

    finally:
        await collector.shutdown()
        await db.close()
        logger.info("테스트 완료")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n중단됨")
