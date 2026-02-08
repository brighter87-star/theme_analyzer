"""DB 현황 확인"""
import asyncio
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(Path(__file__).parent))

from db.database import Database
from config.settings import Settings


async def check():
    s = Settings()
    db = Database(s.db_path)
    await db.initialize()
    conn = await db.get_connection()

    # Total messages
    cur = await conn.execute("SELECT COUNT(*) FROM messages")
    total = (await cur.fetchone())[0]
    print(f"DB 전체 메시지: {total}건")

    # Messages by date
    cur = await conn.execute(
        "SELECT DATE(message_date) as d, COUNT(*) as c "
        "FROM messages GROUP BY d ORDER BY d DESC LIMIT 5"
    )
    rows = await cur.fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]}건")

    # Stock mentions
    cur = await conn.execute("SELECT COUNT(*) FROM stock_mentions")
    sm = (await cur.fetchone())[0]
    print(f"\n종목 멘션: {sm}건")

    # Mentions by date
    cur = await conn.execute(
        "SELECT DATE(m.message_date) as d, COUNT(DISTINCT sm.stock_name) "
        "FROM stock_mentions sm JOIN messages m ON sm.message_id = m.id "
        "GROUP BY d ORDER BY d DESC LIMIT 5"
    )
    rows = await cur.fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]}개 종목")

    # Daily stock themes
    cur = await conn.execute(
        "SELECT report_date, COUNT(*) FROM daily_stock_themes "
        "GROUP BY report_date ORDER BY report_date DESC LIMIT 5"
    )
    rows = await cur.fetchall()
    print(f"\n테마 분류:")
    for r in rows:
        print(f"  {r[0]}: {r[1]}건")

    # Channels
    cur = await conn.execute("SELECT username, is_active FROM channels")
    rows = await cur.fetchall()
    print(f"\n채널 ({len(rows)}개):")
    for r in rows:
        status = "active" if r[1] else "inactive"
        print(f"  @{r[0]} [{status}]")

    await db.close()


asyncio.run(check())
