import logging
from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import Settings
from src.pipeline import Pipeline

logger = logging.getLogger(__name__)


class TaskScheduler:
    def __init__(self, settings: Settings, pipeline: Pipeline):
        self.settings = settings
        self.pipeline = pipeline
        self.scheduler = AsyncIOScheduler(timezone=settings.timezone)

    def setup(self):
        # Collection: every N minutes
        self.scheduler.add_job(
            self._collection_job,
            trigger=IntervalTrigger(
                minutes=self.settings.telegram_collection_interval_min
            ),
            id="collection",
            name="Message Collection",
            max_instances=1,
            misfire_grace_time=300,
        )

        # Daily pipeline: at configured time
        self.scheduler.add_job(
            self._daily_pipeline_job,
            trigger=CronTrigger(
                hour=self.settings.daily_report_hour,
                minute=self.settings.daily_report_minute,
                timezone=self.settings.timezone,
            ),
            id="daily_pipeline",
            name="Daily Report Pipeline",
            max_instances=1,
            misfire_grace_time=3600,
        )

        # Cleanup: daily at 3 AM
        self.scheduler.add_job(
            self._cleanup_job,
            trigger=CronTrigger(hour=3, minute=0, timezone=self.settings.timezone),
            id="cleanup",
            name="Image Cleanup",
        )

        logger.info(
            f"Scheduler configured: "
            f"collection every {self.settings.telegram_collection_interval_min}min, "
            f"daily report at {self.settings.daily_report_hour:02d}:"
            f"{self.settings.daily_report_minute:02d}"
        )

    async def _collection_job(self):
        logger.info("Scheduled collection starting...")
        try:
            stats = await self.pipeline.run_collect_only()
            logger.info(f"Scheduled collection done: {stats}")
        except Exception as e:
            logger.error(f"Scheduled collection failed: {e}", exc_info=True)

    async def _daily_pipeline_job(self):
        report_date = datetime.now(KST).strftime("%Y-%m-%d")
        logger.info(f"Daily pipeline starting for {report_date}...")
        try:
            stats = await self.pipeline.run_full(report_date)
            logger.info(f"Daily pipeline done: {stats}")
        except Exception as e:
            logger.error(f"Daily pipeline failed: {e}", exc_info=True)

    async def _cleanup_job(self):
        """Remove images older than 7 days."""
        import os
        import time

        image_dir = self.settings.image_dir
        if not image_dir.exists():
            return

        cutoff = time.time() - (7 * 24 * 3600)
        removed = 0
        for f in image_dir.iterdir():
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
                removed += 1

        if removed:
            logger.info(f"Cleanup: removed {removed} old images")

    def start(self):
        self.scheduler.start()
        logger.info("Scheduler started")

    def shutdown(self):
        self.scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
