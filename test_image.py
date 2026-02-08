"""이미지 수집 + Claude Vision 분석 테스트"""
import asyncio
import sys
sys.path.insert(0, '.')

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from config.settings import Settings
from utils.image_utils import resize_if_needed, image_to_base64
import anthropic
import yaml

async def main():
    settings = Settings()
    client = TelegramClient(
        str(settings.base_dir / settings.telegram_session_name),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    await client.start(phone=settings.telegram_phone)

    with open(settings.base_dir / "config" / "channels.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 첫 번째 채널에서 이미지가 있는 메시지 1개 찾기
    settings.image_dir.mkdir(parents=True, exist_ok=True)
    found = False

    for ch in config["channels"]:
        if found:
            break
        username = ch["username"]
        print(f"@{username} 에서 이미지 메시지 검색 중...")
        try:
            entity = await client.get_entity(username)
            async for msg in client.iter_messages(entity, limit=30):
                if msg.media and isinstance(msg.media, MessageMediaPhoto):
                    print(f"\n📷 이미지 발견! (msg_id: {msg.id})")
                    text = (msg.text or "")[:100].replace("\n", " ")
                    if text:
                        print(f"   텍스트: {text}")

                    # 이미지 다운로드
                    file_path = settings.image_dir / f"test_{msg.id}.jpg"
                    await client.download_media(msg, file=str(file_path))
                    print(f"   다운로드: {file_path} ({file_path.stat().st_size / 1024:.0f}KB)")

                    # 리사이즈
                    file_path = resize_if_needed(file_path, settings.max_image_size_kb)

                    # Claude Vision 분석
                    print(f"\n🤖 Claude Vision 분석 중...")
                    image_data, media_type = image_to_base64(file_path)

                    claude = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
                    response = await claude.messages.create(
                        model=settings.claude_vision_model,
                        max_tokens=1024,
                        messages=[{
                            "role": "user",
                            "content": [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": media_type,
                                        "data": image_data,
                                    },
                                },
                                {
                                    "type": "text",
                                    "text": "이 이미지는 주식 관련 텔레그램 채널에서 공유된 것입니다. "
                                            "이미지에서 보이는 종목명/티커와 맥락을 간단히 설명해주세요."
                                },
                            ],
                        }],
                    )

                    print(f"\n📊 분석 결과:")
                    print(response.content[0].text)
                    found = True
                    break
        except Exception as e:
            print(f"  ❌ 에러: {e}")

    if not found:
        print("최근 메시지에서 이미지를 찾지 못했습니다.")

    await client.disconnect()
    print("\n✅ 이미지 테스트 완료!")

asyncio.run(main())
