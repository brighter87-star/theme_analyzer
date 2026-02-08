import logging
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

import yaml
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from config.settings import Settings
from db.repository import Repository
from src.reporter import ReportGenerator

logger = logging.getLogger(__name__)


class ThemeAnalyzerBot:
    def __init__(
        self,
        settings: Settings,
        repo: Repository,
        reporter: ReportGenerator,
    ):
        self.settings = settings
        self.repo = repo
        self.reporter = reporter
        self.app: Application | None = None

    async def initialize(self):
        self.app = (
            Application.builder()
            .token(self.settings.telegram_bot_token)
            .build()
        )
        self.app.add_handler(CommandHandler("start", self._cmd_start))
        self.app.add_handler(CommandHandler("report", self._cmd_report))
        self.app.add_handler(CommandHandler("themes", self._cmd_themes))
        self.app.add_handler(CommandHandler("search", self._cmd_search))
        self.app.add_handler(CommandHandler("csv", self._cmd_csv))
        self.app.add_handler(CommandHandler("status", self._cmd_status))
        self.app.add_handler(CommandHandler("help", self._cmd_help))
        self.app.add_handler(CommandHandler("add", self._cmd_add))
        self.app.add_handler(CommandHandler("remove", self._cmd_remove))
        self.app.add_handler(CommandHandler("channels", self._cmd_channels))
        self.app.add_handler(CommandHandler("list", self._cmd_channels))
        logger.info("Bot initialized")

    async def send_daily_report(self, message: str, csv_path: Path | None = None):
        bot = self.app.bot
        chunks = ReportGenerator.split_message(message, 4096)
        for chunk in chunks:
            await bot.send_message(
                chat_id=self.settings.telegram_report_chat_id,
                text=chunk,
                parse_mode="HTML",
            )

        if csv_path and csv_path.exists():
            with open(csv_path, "rb") as f:
                await bot.send_document(
                    chat_id=self.settings.telegram_report_chat_id,
                    document=f,
                    filename=csv_path.name,
                    caption="📎 일일 테마 리포트 CSV",
                )

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "🔍 <b>주식 테마 분석기</b>에 오신 걸 환영합니다!\n\n"
            "매일 텔레그램 채널의 주식 종목을 테마별로 분류해드립니다.\n"
            "/help 로 사용법을 확인하세요.",
            parse_mode="HTML",
        )

    async def _cmd_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if context.args:
            report_date = context.args[0]
        else:
            report_date = datetime.now(KST).strftime("%Y-%m-%d")

        classification = await self.repo.get_daily_classification(report_date)
        if not classification or (not classification.get("kr") and not classification.get("us")):
            await update.message.reply_text(
                f"📭 {report_date} 리포트가 없습니다.\n"
                "아직 분석이 완료되지 않았거나 해당 날짜에 데이터가 없습니다."
            )
            return

        msg, csv_path = await self.reporter.generate_daily_report(
            report_date, classification
        )
        chunks = ReportGenerator.split_message(msg, 4096)
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode="HTML")

    async def _cmd_themes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        themes = await self.repo.get_themes()
        if not themes:
            await update.message.reply_text("등록된 테마가 없습니다.")
            return

        kr_themes = [t for t in themes if t["market"] in ("KR", "BOTH")]
        us_themes = [t for t in themes if t["market"] in ("US", "BOTH")]

        lines = ["<b>📋 활성 테마 목록</b>", ""]
        if kr_themes:
            lines.append("<b>🇰🇷 한국</b>")
            for t in kr_themes:
                lines.append(f"  • {t['name_ko']}")
            lines.append("")
        if us_themes:
            lines.append("<b>🇺🇸 미국</b>")
            for t in us_themes:
                name = t.get("name_en") or t["name_ko"]
                lines.append(f"  • {name}")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def _cmd_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("사용법: /search <종목명 또는 티커>")
            return

        query = " ".join(context.args)
        results = await self.repo.search_stock(query)

        if not results:
            await update.message.reply_text(f"'{query}'에 해당하는 종목을 찾을 수 없습니다.")
            return

        lines = [f"<b>🔍 '{query}' 검색 결과</b>", ""]
        for r in results[:10]:
            name = r.get("name_ko") or r.get("name_en") or r["ticker"]
            lines.append(f"  • {name} ({r['ticker']}) [{r['market']}]")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def _cmd_csv(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Send both strength and history CSVs
        strength_path = self.settings.export_dir / "themes_strength.csv"
        history_path = self.settings.export_dir / "themes_history.csv"

        sent = False
        if strength_path.exists():
            with open(strength_path, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename=strength_path.name,
                    caption="📎 종목 강도 점수 (시간 가중)",
                )
            sent = True

        if history_path.exists():
            with open(history_path, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename=history_path.name,
                    caption="📎 일별 누적 히스토리",
                )
            sent = True

        if not sent:
            await update.message.reply_text("📭 CSV 파일이 아직 없습니다.")

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        channels = await self.repo.get_active_channels()
        today = datetime.now(KST).strftime("%Y-%m-%d")
        report = await self.repo.get_report_status(today)

        lines = [
            "<b>📊 시스템 상태</b>",
            "",
            f"활성 채널: {len(channels)}개",
        ]

        if report:
            lines.extend([
                f"오늘 분석 메시지: {report['total_messages_analyzed']}건",
                f"추출 종목: {report['total_stocks_found']}개",
                f"분류 테마: {report['total_themes']}개",
                f"텔레그램 발송: {'✅' if report['telegram_sent'] else '❌'}",
                f"CSV 내보내기: {'✅' if report['csv_exported'] else '❌'}",
            ])
        else:
            lines.append("오늘 리포트: 아직 생성되지 않음")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    @staticmethod
    def _parse_username(raw: str) -> str:
        """Extract username from various formats: URL, @username, plain username."""
        # https://t.me/username or http://t.me/username
        m = re.match(r"https?://t\.me/([a-zA-Z0-9_]+)", raw)
        if m:
            return m.group(1)
        return raw.lstrip("@")

    async def _cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text(
                "사용법: /add <채널>\n"
                "예: /add stockchannel\n"
                "예: /add https://t.me/stockchannel\n"
                "예: /add @stockchannel"
            )
            return

        username = self._parse_username(context.args[0])
        market_focus = "BOTH"

        # Check if already exists in DB
        all_channels = await self.repo.get_all_channels()
        existing = [c for c in all_channels if c.get("username") == username]

        if existing and existing[0].get("is_active"):
            await update.message.reply_text(f"@{username} 은(는) 이미 활성 채널입니다.")
            return

        if existing and not existing[0].get("is_active"):
            # Reactivate
            await self.repo.activate_channel(username)
            self._sync_yaml_add(username, market_focus)
            await update.message.reply_text(f"✅ @{username} 채널을 다시 활성화했습니다.")
            return

        # New channel - upsert to DB
        try:
            await self.repo.upsert_channel(
                telegram_id=0,  # Will be resolved on next collection cycle
                username=username,
                title=username,
                market_focus=market_focus,
                language="ko",
            )
            self._sync_yaml_add(username, market_focus)
            await update.message.reply_text(
                f"✅ @{username} 채널을 추가했습니다. (market: {market_focus})\n"
                "다음 수집 주기에 메시지를 가져옵니다."
            )
        except Exception as e:
            logger.error(f"Failed to add channel {username}: {e}")
            await update.message.reply_text(f"❌ 채널 추가 실패: {e}")

    async def _cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text(
                "사용법: /remove <채널>\n"
                "예: /remove stockchannel"
            )
            return

        username = self._parse_username(context.args[0])
        success = await self.repo.deactivate_channel(username)

        if success:
            self._sync_yaml_remove(username)
            await update.message.reply_text(f"✅ @{username} 채널을 비활성화했습니다.")
        else:
            await update.message.reply_text(f"❌ @{username} 채널을 찾을 수 없습니다.")

    async def _cmd_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        all_channels = await self.repo.get_all_channels()
        if not all_channels:
            await update.message.reply_text("등록된 채널이 없습니다.")
            return

        lines = ["<b>📡 등록 채널 목록</b>", ""]
        for ch in all_channels:
            status = "🟢" if ch.get("is_active") else "🔴"
            username = ch.get("username") or "N/A"
            market = ch.get("market_focus", "BOTH")
            title = ch.get("title", username)
            lines.append(f"  {status} @{username} [{market}] - {title}")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    def _sync_yaml_add(self, username: str, market_focus: str = "BOTH"):
        yaml_path = self.settings.base_dir / "config" / "channels.yaml"
        try:
            if yaml_path.exists():
                with open(yaml_path, encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
            else:
                config = {}

            channels = config.get("channels", [])
            # Check if already in YAML
            if any(c.get("username") == username for c in channels):
                return

            channels.append({
                "username": username,
                "market_focus": market_focus,
                "language": "ko",
            })
            config["channels"] = channels

            with open(yaml_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        except Exception as e:
            logger.warning(f"Failed to sync YAML (add {username}): {e}")

    def _sync_yaml_remove(self, username: str):
        yaml_path = self.settings.base_dir / "config" / "channels.yaml"
        try:
            if not yaml_path.exists():
                return
            with open(yaml_path, encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}

            channels = config.get("channels", [])
            config["channels"] = [c for c in channels if c.get("username") != username]

            with open(yaml_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        except Exception as e:
            logger.warning(f"Failed to sync YAML (remove {username}): {e}")

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "<b>📖 사용 가능한 명령어</b>\n\n"
            "<b>리포트</b>\n"
            "/report - 오늘 테마 리포트\n"
            "/themes - 활성 테마 목록\n"
            "/search 종목명 - 종목 검색\n"
            "/csv - CSV 다운로드\n\n"
            "<b>채널 관리</b>\n"
            "/add 채널 - 채널 추가 (URL, @, 이름)\n"
            "/remove 채널 - 채널 비활성화\n"
            "/list - 등록 채널 목록\n\n"
            "<b>기타</b>\n"
            "/status - 시스템 상태",
            parse_mode="HTML",
        )

    async def run(self):
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)
        logger.info("Bot polling started")

    async def stop(self):
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
            logger.info("Bot stopped")
