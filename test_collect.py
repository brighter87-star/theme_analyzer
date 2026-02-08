"""채널 수집 테스트 - 각 채널에서 최근 메시지 5개만 가져와서 확인"""
import asyncio
import sys
sys.path.insert(0, '.')

from telethon import TelegramClient
from config.settings import Settings

async def main():
    settings = Settings()
    client = TelegramClient(
        str(settings.base_dir / settings.telegram_session_name),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    await client.start(phone=settings.telegram_phone)

    import yaml
    with open(settings.base_dir / "config" / "channels.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for ch in config["channels"]:
        username = ch["username"]
        print(f"\n{'='*50}")
        print(f"채널: @{username}")
        print(f"{'='*50}")
        try:
            entity = await client.get_entity(username)
            print(f"  제목: {getattr(entity, 'title', 'N/A')}")
            count = 0
            async for msg in client.iter_messages(entity, limit=5):
                count += 1
                text = (msg.text or "")[:80].replace("\n", " ")
                has_photo = "📷" if msg.photo else ""
                print(f"  [{count}] {has_photo} {text}")
            print(f"  -> {count}개 메시지 확인")
        except Exception as e:
            print(f"  ❌ 에러: {e}")

    await client.disconnect()
    print("\n✅ 테스트 완료!")

asyncio.run(main())
