"""
일일 테마 분석 파이프라인
수집 → 분석 → 분류 → 리포트/CSV 생성

사용법:
  # 초기 실행 (수요일 00:00 KST부터)
  python run_pipeline.py --lookback-hours 120

  # 일일 실행 (매일 아침 6시 크론)
  python run_pipeline.py

  # 수집 건너뛰기 (봇이 이미 수집 중일 때)
  python run_pipeline.py --skip-collect
"""
import argparse
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
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                log_dir / f"pipeline_{datetime.now(KST).strftime('%Y%m%d_%H%M')}.log",
                encoding="utf-8",
            ),
        ],
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def main(lookback_hours: int, skip_collect: bool):
    setup_logging()
    logger = logging.getLogger("pipeline")

    now = datetime.now(KST)
    logger.info(f"파이프라인 시작: {now.strftime('%Y-%m-%d %H:%M KST')}")
    logger.info(f"lookback: {lookback_hours}시간, skip_collect: {skip_collect}")

    settings = Settings()
    settings.lookback_hours = lookback_hours

    settings.image_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    db = Database(settings.db_path)
    await db.initialize()
    await run_migrations(db)
    repo = Repository(db)

    rate_limiter = RateLimiter()
    rate_limiter.add_bucket("claude", rate=settings.claude_rpm / 60, capacity=5)
    rate_limiter.add_bucket("telegram", rate=0.5, capacity=5)

    registry = StockRegistry(repo)
    await registry.initialize()

    collector = None

    try:
        # ── Step 1: 수집 ──
        if not skip_collect:
            logger.info("=" * 60)
            logger.info("STEP 1: 메시지 수집")
            logger.info("=" * 60)
            collector = MessageCollector(settings, repo)
            await collector.initialize()
            stats = await collector.collect_all_channels()
            logger.info(
                f"수집: {stats['total_messages']}메시지 / "
                f"{stats['total_channels']}채널"
            )
            if stats["errors"]:
                logger.warning(f"수집 에러: {stats['errors']}")
        else:
            logger.info("STEP 1: 수집 건너뜀 (--skip-collect)")

        # ── Step 2: 분석 ──
        logger.info("=" * 60)
        logger.info("STEP 2: 종목 추출")
        logger.info("=" * 60)
        analyzer = StockAnalyzer(settings, repo, registry, rate_limiter)
        analysis = await analyzer.analyze_pending_messages()
        logger.info(
            f"분석: 텍스트 {analysis['text_messages']}개, "
            f"이미지 {analysis['image_messages']}개, "
            f"종목 {analysis['stocks_extracted']}개, "
            f"에러 {analysis['errors']}건"
        )

        # ── Step 3: 분류 ──
        logger.info("=" * 60)
        logger.info("STEP 3: 테마 분류")
        logger.info("=" * 60)
        classifier = ThemeClassifier(settings, repo, rate_limiter)
        reporter = ReportGenerator(settings, repo)

        today = now.date()
        lookback_days = (lookback_hours // 24) + 1
        dates = []
        for i in range(lookback_days):
            d = today - timedelta(days=i)
            dates.append(d.strftime("%Y-%m-%d"))
        dates.reverse()

        all_classification = {"kr": {}, "us": {}}

        for report_date in dates:
            mentions = await repo.get_daily_stock_mentions(report_date)
            if not mentions:
                continue

            logger.info(f"[{report_date}] 종목 {len(mentions)}개 → 분류")
            classification = await classifier.classify_daily(report_date)

            for market in ["kr", "us"]:
                for theme, stocks in classification.get(market, {}).items():
                    if theme not in all_classification[market]:
                        all_classification[market][theme] = stocks
                    else:
                        existing_tickers = {
                            s["ticker"] for s in all_classification[market][theme]
                        }
                        for s in stocks:
                            if s["ticker"] not in existing_tickers:
                                all_classification[market][theme].append(s)

        # ── Step 4: 리포트 + CSV ──
        logger.info("=" * 60)
        logger.info("STEP 4: 리포트 생성")
        logger.info("=" * 60)
        report_date = today.strftime("%Y-%m-%d")
        message, csv_path = await reporter.generate_daily_report(
            report_date, all_classification
        )

        total_kr = sum(len(v) for v in all_classification.get("kr", {}).values())
        total_us = sum(len(v) for v in all_classification.get("us", {}).values())
        kr_themes = len(all_classification.get("kr", {}))
        us_themes = len(all_classification.get("us", {}))

        logger.info(
            f"완료: KR {kr_themes}테마/{total_kr}종목, "
            f"US {us_themes}테마/{total_us}종목"
        )
        logger.info(f"CSV: {csv_path}")

        print(f"\n{'=' * 50}")
        print(f"파이프라인 완료 - {report_date}")
        print(f"KR: {kr_themes}테마 / {total_kr}종목")
        print(f"US: {us_themes}테마 / {total_us}종목")
        print(f"CSV: {csv_path}")
        print(f"{'=' * 50}")

    finally:
        if collector:
            await collector.shutdown()
        await db.close()
        logger.info("파이프라인 종료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="테마 분석 파이프라인")
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=28,
        help="수집 시작 시점 (현재 - N시간). 기본 28시간 (일일 실행용)",
    )
    parser.add_argument(
        "--skip-collect",
        action="store_true",
        help="메시지 수집 건너뛰기 (봇이 이미 수집 중일 때)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.lookback_hours, args.skip_collect))
    except KeyboardInterrupt:
        print("\n중단됨")
