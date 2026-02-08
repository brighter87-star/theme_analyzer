import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import Settings
from db.database import Database
from db.migrations import run_migrations
from db.repository import Repository
from src.analyzer import StockAnalyzer
from src.bot import ThemeAnalyzerBot
from src.classifier import ThemeClassifier
from src.collector import MessageCollector
from src.pipeline import Pipeline
from src.reporter import ReportGenerator
from src.scheduler import TaskScheduler
from utils.rate_limiter import RateLimiter
from utils.stock_registry import StockRegistry


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                "theme_analyzer.log", encoding="utf-8"
            ),
        ],
    )
    # Reduce noisy loggers
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)


async def main():
    setup_logging()
    logger = logging.getLogger("main")
    logger.info("Theme Analyzer starting...")

    # Load settings
    settings = Settings()

    # Ensure directories exist
    settings.image_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize database
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

    # Core modules
    collector = MessageCollector(settings, repo)
    await collector.initialize()

    analyzer = StockAnalyzer(settings, repo, registry, rate_limiter)
    classifier = ThemeClassifier(settings, repo, rate_limiter)
    reporter = ReportGenerator(settings, repo)

    bot = ThemeAnalyzerBot(settings, repo, reporter)
    await bot.initialize()

    # Pipeline & scheduler
    pipeline = Pipeline(collector, analyzer, classifier, reporter, bot, repo)
    scheduler = TaskScheduler(settings, pipeline)
    scheduler.setup()
    scheduler.start()

    logger.info("All components initialized. Bot + Scheduler running.")

    # Graceful shutdown handler
    shutdown_event = asyncio.Event()

    def handle_signal(*_):
        logger.info("Shutdown signal received")
        shutdown_event.set()

    if sys.platform != "win32":
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, handle_signal)
        loop.add_signal_handler(signal.SIGTERM, handle_signal)

    try:
        await bot.run()
        # On Windows, use keyboard interrupt
        await shutdown_event.wait()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    finally:
        logger.info("Shutting down...")
        scheduler.shutdown()
        await bot.stop()
        await collector.shutdown()
        await db.close()
        logger.info("Theme Analyzer stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
