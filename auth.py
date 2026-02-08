"""Telethon 세션 인증용 스크립트. 한 번만 실행하면 됩니다."""
import asyncio
from telethon import TelegramClient

async def main():
    client = TelegramClient(
        'theme_analyzer',
        34726006,
        '985cdba87d7a5c62d55e1b4cca9f5ec0'
    )
    await client.start(phone='+821068886428')
    print("인증 성공! theme_analyzer.session 파일이 생성되었습니다.")
    me = await client.get_me()
    print(f"로그인 계정: {me.first_name} ({me.phone})")
    await client.disconnect()

asyncio.run(main())
