from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Telegram User API (Telethon)
    telegram_api_id: int
    telegram_api_hash: str
    telegram_phone: str
    telegram_session_name: str = "theme_analyzer"

    # Telegram Bot
    telegram_bot_token: str
    telegram_report_chat_id: int

    # Claude API
    anthropic_api_key: str
    claude_model: str = "claude-sonnet-4-20250514"
    claude_vision_model: str = "claude-sonnet-4-20250514"
    claude_max_tokens: int = 4096

    # Rate limits
    claude_rpm: int = 50
    claude_daily_limit: int = 1000
    telegram_collection_interval_min: int = 30

    # Scheduling
    daily_report_hour: int = 18
    daily_report_minute: int = 0
    timezone: str = "Asia/Seoul"

    # Paths
    base_dir: Path = Path("c:/theme_analyzer")
    db_path: Optional[Path] = None
    image_dir: Optional[Path] = None
    export_dir: Optional[Path] = None

    # Processing
    max_image_size_kb: int = 1024
    lookback_hours: int = 24
    batch_size: int = 10

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def model_post_init(self, __context):
        if self.db_path is None:
            self.db_path = self.base_dir / "data" / "theme_analyzer.db"
        if self.image_dir is None:
            self.image_dir = self.base_dir / "data" / "images"
        if self.export_dir is None:
            self.export_dir = self.base_dir / "data" / "exports"
