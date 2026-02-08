import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto

from config.settings import Settings
from db.repository import Repository
from utils.image_utils import resize_if_needed

logger = logging.getLogger(__name__)


class MessageCollector:
    def __init__(self, settings: Settings, repo: Repository):
        self.settings = settings
        self.repo = repo
        self.client: TelegramClient | None = None
        self._semaphore = asyncio.Semaphore(3)

    async def initialize(self):
        session_path = str(self.settings.base_dir / self.settings.telegram_session_name)
        self.client = TelegramClient(
            session_path,
            self.settings.telegram_api_id,
            self.settings.telegram_api_hash,
        )
        await self.client.start(phone=self.settings.telegram_phone)
        logger.info("Telethon client started")

        # channels.yaml에서 DB로 채널 시드
        await self._seed_channels_from_yaml()

    async def _seed_channels_from_yaml(self):
        """channels.yaml의 채널 목록을 DB에 등록 (Telegram에서 실제 정보 조회)."""
        import yaml

        yaml_path = self.settings.base_dir / "config" / "channels.yaml"
        if not yaml_path.exists():
            return

        with open(yaml_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        channel_list = config.get("channels", [])
        if not channel_list:
            return

        for ch in channel_list:
            username = ch.get("username")
            if not username:
                continue
            try:
                entity = await self.client.get_entity(username)
                await self.repo.upsert_channel(
                    telegram_id=entity.id,
                    username=username,
                    title=getattr(entity, "title", username),
                    market_focus=ch.get("market_focus", "BOTH"),
                    language=ch.get("language", "ko"),
                )
                logger.info(f"Channel registered: {getattr(entity, 'title', username)}")
            except Exception as e:
                logger.warning(f"Could not register channel @{username}: {e}")

    async def collect_all_channels(self) -> dict:
        channels = await self.repo.get_active_channels()
        if not channels:
            logger.warning("No active channels in DB")
            return {"total_channels": 0, "total_messages": 0, "errors": []}

        tasks = [self._collect_channel(ch) for ch in channels]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_messages = 0
        errors = []
        for ch, result in zip(channels, results):
            if isinstance(result, Exception):
                errors.append(f"{ch['title']}: {result}")
                logger.error(f"Error collecting {ch['title']}: {result}")
            else:
                total_messages += result

        stats = {
            "total_channels": len(channels),
            "total_messages": total_messages,
            "errors": errors,
        }
        logger.info(
            f"Collection complete: {total_messages} messages "
            f"from {len(channels)} channels, {len(errors)} errors"
        )
        return stats

    async def _collect_channel(self, channel: dict) -> int:
        async with self._semaphore:
            cutoff = datetime.now(timezone.utc) - timedelta(
                hours=self.settings.lookback_hours
            )

            try:
                entity = await self.client.get_entity(channel["username"])
            except Exception as e:
                logger.error(f"Cannot find channel {channel['username']}: {e}")
                raise

            # Update channel info in DB
            await self.repo.upsert_channel(
                telegram_id=entity.id,
                username=channel.get("username"),
                title=getattr(entity, "title", channel.get("title", "")),
                market_focus=channel.get("market_focus", "BOTH"),
                language=channel.get("language", "ko"),
            )

            count = 0
            async for message in self.client.iter_messages(entity, limit=2000):
                if message.date.replace(tzinfo=timezone.utc) < cutoff:
                    break

                if await self.repo.message_exists(channel["id"], message.id):
                    continue

                # Download image if present
                image_path = None
                has_image = False
                if message.media and isinstance(message.media, MessageMediaPhoto):
                    image_path = await self._download_image(message, channel["id"])
                    has_image = image_path is not None

                text = message.text or message.message or ""

                await self.repo.insert_message(
                    channel_id=channel["id"],
                    telegram_msg_id=message.id,
                    message_text=text,
                    has_image=has_image,
                    image_path=str(image_path) if image_path else None,
                    message_date=message.date.isoformat(),
                )
                count += 1

                # Small delay to avoid rate limits
                if count % 50 == 0:
                    await asyncio.sleep(1)

            logger.info(f"Collected {count} messages from {channel.get('title', channel.get('username'))}")
            return count

    async def _download_image(self, message, channel_id: int) -> Path | None:
        try:
            filename = f"{channel_id}_{message.id}.jpg"
            file_path = self.settings.image_dir / filename
            self.settings.image_dir.mkdir(parents=True, exist_ok=True)

            await self.client.download_media(
                message, file=str(file_path)
            )

            if file_path.exists():
                file_path = resize_if_needed(
                    file_path, self.settings.max_image_size_kb
                )
                return file_path
        except Exception as e:
            logger.warning(f"Failed to download image: {e}")
        return None

    async def shutdown(self):
        if self.client:
            await self.client.disconnect()
            logger.info("Telethon client disconnected")
