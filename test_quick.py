"""
빠른 분류 테스트 - 채널당 메시지 N개만 분석/분류
기존 수집된 메시지 재사용, 토큰 최소 사용
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
from src.reporter import ReportGenerator
from utils.rate_limiter import RateLimiter
from utils.stock_registry import StockRegistry

MSGS_PER_CHANNEL = 5  # 채널당 처리할 메시지 수


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("test_quick.log", encoding="utf-8"),
        ],
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def main():
    setup_logging()
    logger = logging.getLogger("test_quick")

    now = datetime.now(KST)
    logger.info(f"빠른 테스트 시작: {now.strftime('%Y-%m-%d %H:%M KST')}")
    logger.info(f"채널당 메시지 제한: {MSGS_PER_CHANNEL}개")

    settings = Settings()
    settings.lookback_hours = 60

    settings.image_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    db = Database(settings.db_path)
    await db.initialize()
    await run_migrations(db)
    repo = Repository(db)

    rate_limiter = RateLimiter()
    rate_limiter.add_bucket("claude", rate=settings.claude_rpm / 60, capacity=5)

    registry = StockRegistry(repo)
    await registry.initialize()

    conn = await db.get_connection()

    try:
        # ── 분석/분류 데이터 초기화 ──
        await conn.execute("UPDATE messages SET is_analyzed = 0")
        await conn.execute("DELETE FROM stock_mentions")
        await conn.execute("DELETE FROM daily_stock_themes")
        await conn.commit()
        logger.info("분석/분류 데이터 초기화 완료")

        # ── 채널당 N개만 분석 대상으로 남기고 나머지는 analyzed 처리 ──
        channels = await repo.get_active_channels()
        total_target = 0
        for ch in channels:
            # 채널별 최신 N개 메시지만 남기고 나머지 is_analyzed=1
            cursor = await conn.execute(
                """SELECT id FROM messages
                   WHERE channel_id = ?
                   ORDER BY message_date DESC
                   LIMIT ?""",
                (ch["id"], MSGS_PER_CHANNEL),
            )
            keep_ids = [row["id"] for row in await cursor.fetchall()]
            total_target += len(keep_ids)

            if keep_ids:
                placeholders = ",".join("?" for _ in keep_ids)
                await conn.execute(
                    f"""UPDATE messages SET is_analyzed = 1
                        WHERE channel_id = ? AND id NOT IN ({placeholders})""",
                    [ch["id"]] + keep_ids,
                )

        await conn.commit()
        logger.info(f"분석 대상: {total_target}개 메시지 ({len(channels)}채널 × {MSGS_PER_CHANNEL}개)")
        print(f"\n=== 분석 대상: {total_target}개 메시지 ===")

        # ── Step 1: 종목 추출 ──
        print("\n--- Step 1: 종목 추출 ---")
        analyzer = StockAnalyzer(settings, repo, registry, rate_limiter)
        stats = await analyzer.analyze_pending_messages()
        print(f"  텍스트: {stats['text_messages']}개, 이미지: {stats['image_messages']}개")
        print(f"  추출 종목: {stats['stocks_extracted']}개, 에러: {stats['errors']}건")

        # ── Step 2: 테마 분류 ──
        print("\n--- Step 2: 테마 분류 ---")
        classifier = ThemeClassifier(settings, repo, rate_limiter)
        reporter = ReportGenerator(settings, repo)

        today = now.date()
        dates = []
        for i in range(3):
            d = today - timedelta(days=i)
            dates.append(d.strftime("%Y-%m-%d"))
        dates.reverse()

        all_classification = {"kr": {}, "us": {}}

        for report_date in dates:
            mentions = await repo.get_daily_stock_mentions(report_date)
            if not mentions:
                print(f"  [{report_date}] 종목 없음 - 건너뜀")
                continue

            print(f"  [{report_date}] 종목 {len(mentions)}개 → 분류 중...")
            classification = await classifier.classify_daily(report_date)

            kr_count = sum(len(v) for v in classification.get("kr", {}).values())
            us_count = sum(len(v) for v in classification.get("us", {}).values())
            print(f"    KR: {len(classification.get('kr', {}))}테마 {kr_count}종목")
            print(f"    US: {len(classification.get('us', {}))}테마 {us_count}종목")

            for market in ["kr", "us"]:
                for theme, stocks in classification.get(market, {}).items():
                    if theme not in all_classification[market]:
                        all_classification[market][theme] = stocks
                    else:
                        existing_tickers = {s["ticker"] for s in all_classification[market][theme]}
                        for s in stocks:
                            if s["ticker"] not in existing_tickers:
                                all_classification[market][theme].append(s)

        # ── Step 3: 리포트 ──
        report_date = today.strftime("%Y-%m-%d")
        message, csv_path = await reporter.generate_daily_report(
            report_date, all_classification
        )

        print(f"\n{'=' * 60}")
        print(f"리포트 ({report_date})")
        print(f"{'=' * 60}")
        print(message)

        # 테마별 상세
        for market_code, market_name in [("kr", "한국"), ("us", "미국")]:
            themes = all_classification.get(market_code, {})
            if themes:
                print(f"\n--- {market_name} 테마 ---")
                for theme_name, stocks in sorted(themes.items()):
                    tickers = [f"{s.get('name', s.get('ticker', '?'))}({s.get('ticker','')})" for s in stocks]
                    print(f"  [{theme_name}] {', '.join(tickers)}")

        print(f"\nCSV: {csv_path}")

    finally:
        await db.close()
        logger.info("테스트 완료")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n중단됨")
