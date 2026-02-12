"""
누락된 날짜들을 backfill하는 스크립트.

사용법:
  # 최근 5일 backfill
  python backfill.py --days 5

  # 특정 날짜 범위 backfill
  python backfill.py --from 2026-02-07 --to 2026-02-11

  # 수집 건너뛰기 (이미 DB에 메시지가 있을 때)
  python backfill.py --days 5 --skip-collect
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
                log_dir / f"backfill_{datetime.now(KST).strftime('%Y%m%d_%H%M')}.log",
                encoding="utf-8",
            ),
        ],
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def main(dates: list[str], skip_collect: bool, lookback_hours: int):
    setup_logging()
    logger = logging.getLogger("backfill")

    logger.info(f"Backfill 시작: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")
    logger.info(f"skip_collect: {skip_collect}")

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
        # ── Step 1: 전체 기간 메시지 수집 (한 번만) ──
        if not skip_collect:
            logger.info("=" * 60)
            logger.info(f"STEP 1: 메시지 수집 (lookback {lookback_hours}시간)")
            logger.info("=" * 60)
            collector = MessageCollector(settings, repo)
            await collector.initialize()
            stats = await collector.collect_all_channels()
            logger.info(
                f"수집 완료: {stats['total_messages']}메시지 / "
                f"{stats['total_channels']}채널"
            )
            if stats["errors"]:
                logger.warning(f"수집 에러: {stats['errors']}")
        else:
            logger.info("STEP 1: 수집 건너뜀 (--skip-collect)")

        # ── Step 2: 미분석 메시지 일괄 분석 ──
        logger.info("=" * 60)
        logger.info("STEP 2: 종목 추출 (전체)")
        logger.info("=" * 60)
        analyzer = StockAnalyzer(settings, repo, registry, rate_limiter)
        analysis = await analyzer.analyze_pending_messages()
        logger.info(
            f"분석 완료: 텍스트 {analysis['text_messages']}개, "
            f"이미지 {analysis['image_messages']}개, "
            f"종목 {analysis['stocks_extracted']}개"
        )

        # ── Step 3~4: 날짜별 분류 + 리포트 ──
        classifier = ThemeClassifier(settings, repo, rate_limiter)
        reporter = ReportGenerator(settings, repo)

        results = []

        for report_date in dates:
            logger.info("=" * 60)
            logger.info(f"STEP 3-4: [{report_date}] 분류 + 리포트")
            logger.info("=" * 60)

            mentions = await repo.get_daily_stock_mentions(report_date)
            if not mentions:
                logger.info(f"[{report_date}] 종목 언급 없음 → 건너뜀")
                results.append((report_date, 0, 0, 0, 0))
                continue

            logger.info(f"[{report_date}] 종목 {len(mentions)}개 → 분류")
            classification = await classifier.classify_daily(report_date)

            message, csv_path = await reporter.generate_daily_report(
                report_date, classification
            )

            kr_themes = len(classification.get("kr", {}))
            us_themes = len(classification.get("us", {}))
            total_kr = sum(len(v) for v in classification.get("kr", {}).values())
            total_us = sum(len(v) for v in classification.get("us", {}).values())

            results.append((report_date, kr_themes, total_kr, us_themes, total_us))
            logger.info(
                f"[{report_date}] 완료: KR {kr_themes}테마/{total_kr}종목, "
                f"US {us_themes}테마/{total_us}종목"
            )

        # ── 결과 요약 ──
        print(f"\n{'=' * 55}")
        print(f"  Backfill 완료: {dates[0]} ~ {dates[-1]}")
        print(f"{'=' * 55}")
        print(f"  {'날짜':<12} {'KR테마':>6} {'KR종목':>6} {'US테마':>6} {'US종목':>6}")
        print(f"  {'-' * 48}")
        for date, kr_t, kr_s, us_t, us_s in results:
            print(f"  {date:<12} {kr_t:>6} {kr_s:>6} {us_t:>6} {us_s:>6}")
        print(f"{'=' * 55}")

    finally:
        if collector:
            await collector.shutdown()
        await db.close()
        logger.info("Backfill 종료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="테마 분석 Backfill")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--days", type=int, help="오늘 기준 N일 전부터 어제까지 backfill"
    )
    group.add_argument("--from", dest="from_date", help="시작 날짜 (YYYY-MM-DD)")

    parser.add_argument("--to", dest="to_date", help="종료 날짜 (YYYY-MM-DD, 기본: 어제)")
    parser.add_argument(
        "--skip-collect", action="store_true", help="메시지 수집 건너뛰기"
    )

    args = parser.parse_args()

    now = datetime.now(KST)
    today = now.date()

    if args.days:
        start = today - timedelta(days=args.days)
        end = today - timedelta(days=1)
    else:
        start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        end = (
            datetime.strptime(args.to_date, "%Y-%m-%d").date()
            if args.to_date
            else today - timedelta(days=1)
        )

    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    if not dates:
        print("backfill할 날짜가 없습니다.")
        sys.exit(1)

    # 수집 범위: 시작일 00:00 KST ~ 현재 (넉넉하게 +24시간)
    hours_back = int((now - datetime.combine(start, datetime.min.time(), KST)).total_seconds() / 3600) + 24

    try:
        asyncio.run(main(dates, args.skip_collect, hours_back))
    except KeyboardInterrupt:
        print("\n중단됨")
