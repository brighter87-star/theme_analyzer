"""전체 파이프라인 테스트 - 수집 → 분석 → 분류 → 봇 발송"""
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import Settings
from db.database import Database
from db.migrations import run_migrations
from db.repository import Repository
from src.analyzer import StockAnalyzer
from src.bot import ThemeAnalyzerBot
from src.classifier import ThemeClassifier
from src.collector import MessageCollector
from src.reporter import ReportGenerator
from utils.rate_limiter import RateLimiter
from utils.stock_registry import StockRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger("test_pipeline")


async def main():
    settings = Settings()
    settings.image_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)

    # DB
    db = Database(settings.db_path)
    await db.initialize()
    await run_migrations(db)
    repo = Repository(db)

    # Rate limiter
    rate_limiter = RateLimiter()
    rate_limiter.add_bucket("claude", rate=settings.claude_rpm / 60, capacity=5)

    # Stock registry
    logger.info("=== 종목 레지스트리 초기화 중... ===")
    registry = StockRegistry(repo)
    await registry.initialize()

    # Collector
    logger.info("=== [1/4] 메시지 수집 시작 ===")
    collector = MessageCollector(settings, repo)
    await collector.initialize()
    collection_stats = await collector.collect_all_channels()
    logger.info(f"수집 완료: {collection_stats}")

    # Analyzer
    logger.info("=== [2/4] AI 분석 시작 (종목 추출) ===")
    analyzer = StockAnalyzer(settings, repo, registry, rate_limiter)
    analysis_stats = await analyzer.analyze_pending_messages()
    logger.info(f"분석 완료: {analysis_stats}")

    # Classifier - use the latest message date for the report
    report_date = datetime.now(KST).strftime("%Y-%m-%d")
    # Check if today has mentions; if not, use latest available date
    daily_mentions = await repo.get_daily_stock_mentions(report_date)
    if not daily_mentions:
        conn = await db.get_connection()
        cur = await conn.execute(
            "SELECT DISTINCT DATE(m.message_date) as d FROM stock_mentions sm "
            "JOIN messages m ON sm.message_id = m.id ORDER BY d DESC LIMIT 1"
        )
        row = await cur.fetchone()
        if row:
            report_date = row[0]
            logger.info(f"오늘 데이터 없음 → 최근 날짜 사용: {report_date}")

    logger.info(f"=== [3/4] 테마 분류 시작 ({report_date}) ===")
    classifier = ThemeClassifier(settings, repo, rate_limiter)
    classification = await classifier.classify_daily(report_date)
    kr_count = len(classification.get("kr", {}))
    us_count = len(classification.get("us", {}))
    logger.info(f"분류 완료: KR {kr_count}개 테마, US {us_count}개 테마")

    # Reporter + Bot send
    logger.info("=== [4/4] 리포트 생성 및 봇 발송 ===")
    reporter = ReportGenerator(settings, repo)
    message, csv_path = await reporter.generate_daily_report(report_date, classification)

    print("\n" + "=" * 60)
    print("생성된 리포트 미리보기:")
    print("=" * 60)
    print(message.replace("<b>", "").replace("</b>", ""))
    print("=" * 60)

    if csv_path:
        logger.info(f"강도 CSV: {csv_path}")
    logger.info(f"히스토리 CSV: {reporter.history_path}")

    # 봇으로 발송
    bot = ThemeAnalyzerBot(settings, repo, reporter)
    await bot.initialize()
    await bot.send_daily_report(message, csv_path)
    logger.info("텔레그램 봇 발송 완료!")

    # Cleanup
    await collector.shutdown()
    await db.close()

    print("\n✅ 전체 파이프라인 테스트 완료!")
    print(f"   수집: {collection_stats['total_messages']}건")
    print(f"   분석: 텍스트 {analysis_stats.get('text_messages', 0)}건 + 이미지 {analysis_stats.get('image_messages', 0)}건")
    print(f"   종목: {analysis_stats.get('stocks_extracted', 0)}개 추출")
    print(f"   테마: KR {kr_count}개 + US {us_count}개")
    print(f"   텔레그램 봇으로 리포트 발송됨 ✅")


asyncio.run(main())
