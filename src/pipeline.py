import logging
from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

from src.analyzer import StockAnalyzer
from src.bot import ThemeAnalyzerBot
from src.classifier import ThemeClassifier
from src.collector import MessageCollector
from src.reporter import ReportGenerator
from db.repository import Repository

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        collector: MessageCollector,
        analyzer: StockAnalyzer,
        classifier: ThemeClassifier,
        reporter: ReportGenerator,
        bot: ThemeAnalyzerBot,
        repo: Repository,
    ):
        self.collector = collector
        self.analyzer = analyzer
        self.classifier = classifier
        self.reporter = reporter
        self.bot = bot
        self.repo = repo

    async def run_full(self, report_date: str | None = None) -> dict:
        """Run the complete pipeline: collect -> analyze -> classify -> report."""
        if report_date is None:
            report_date = datetime.now(KST).strftime("%Y-%m-%d")

        logger.info(f"Starting full pipeline for {report_date}")

        # Step 1: Collect
        collection_stats = await self.collector.collect_all_channels()
        logger.info(f"Collection: {collection_stats}")

        # Step 2: Analyze
        analysis_stats = await self.analyzer.analyze_pending_messages()
        logger.info(f"Analysis: {analysis_stats}")

        # Step 3: Classify
        classification = await self.classifier.classify_daily(report_date)
        logger.info(f"Classification: KR={len(classification.get('kr', {}))} US={len(classification.get('us', {}))} themes")

        # Step 4: Generate and send report
        message, csv_path = await self.reporter.generate_daily_report(
            report_date, classification
        )
        await self.bot.send_daily_report(message, csv_path)

        # Step 5: Record stats
        total_stocks = (
            sum(len(v) for v in classification.get("kr", {}).values())
            + sum(len(v) for v in classification.get("us", {}).values())
        )
        total_themes = len(classification.get("kr", {})) + len(classification.get("us", {}))

        await self.repo.record_daily_report(
            report_date=report_date,
            total_messages=analysis_stats.get("text_messages", 0)
            + analysis_stats.get("image_messages", 0),
            total_stocks=total_stocks,
            total_themes=total_themes,
            telegram_sent=True,
            csv_exported=csv_path is not None,
        )

        logger.info(f"Pipeline complete for {report_date}")
        return {
            "report_date": report_date,
            "collection": collection_stats,
            "analysis": analysis_stats,
            "total_stocks": total_stocks,
            "total_themes": total_themes,
        }

    async def run_collect_only(self) -> dict:
        return await self.collector.collect_all_channels()

    async def run_analyze_only(self) -> dict:
        return await self.analyzer.analyze_pending_messages()

    async def run_classify_only(self, report_date: str | None = None) -> dict:
        if report_date is None:
            report_date = datetime.now(KST).strftime("%Y-%m-%d")
        return await self.classifier.classify_daily(report_date)
