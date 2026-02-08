"""티커 조회 도구: 분류 결과 및 동일 테마 종목 확인."""
import asyncio
import sys

from config.settings import Settings
from db.database import Database
from db.repository import Repository

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


async def lookup(ticker: str):
    settings = Settings()
    db = Database(settings.db_path)
    await db.initialize()
    repo = Repository(db)
    conn = await db.get_connection()

    # 1) 종목 기본 정보
    cursor = await conn.execute(
        "SELECT * FROM stocks WHERE UPPER(ticker) = UPPER(?)", (ticker,)
    )
    stock = await cursor.fetchone()
    if not stock:
        print(f"'{ticker}' 종목을 찾을 수 없습니다.")
        await db.close()
        return

    stock = dict(stock)
    name = stock.get("name_ko") or stock.get("name_en") or stock["ticker"]
    print(f"\n{'='*50}")
    print(f"  {name} ({stock['ticker']})")
    print(f"  시장: {stock['market']}  |  거래소: {stock.get('exchange') or '-'}")
    print(f"  업종(yfinance): {stock.get('industry') or '미조회'}")
    print(f"{'='*50}")

    # 2) 일별 분류 결과
    cursor = await conn.execute(
        """SELECT dst.report_date, dst.sector, dst.mention_count, dst.reason,
                  t.name_ko as theme_name
           FROM daily_stock_themes dst
           JOIN themes t ON dst.theme_id = t.id
           WHERE dst.stock_id = ?
           ORDER BY dst.report_date DESC""",
        (stock["id"],),
    )
    classifications = [dict(r) for r in await cursor.fetchall()]

    if not classifications:
        print("\n  분류 이력이 없습니다.")
        await db.close()
        return

    print(f"\n  [분류 이력]")
    for c in classifications:
        print(f"  {c['report_date']}  테마: {c['theme_name']}  "
              f"섹터: {c['sector']}  언급: {c['mention_count']}회")
        if c["reason"]:
            print(f"    └ {c['reason']}")

    # 3) 동일 테마 종목 (최신 날짜 기준)
    latest_date = classifications[0]["report_date"]
    # 이 종목이 속한 테마 ID 목록
    cursor = await conn.execute(
        "SELECT DISTINCT theme_id FROM daily_stock_themes WHERE stock_id = ? AND report_date = ?",
        (stock["id"], latest_date),
    )
    theme_ids = [r["theme_id"] for r in await cursor.fetchall()]

    if theme_ids:
        placeholders = ",".join("?" for _ in theme_ids)
        cursor = await conn.execute(
            f"""SELECT s.ticker, s.name_ko, s.name_en, s.market, s.industry,
                       dst.sector, dst.mention_count, t.name_ko as theme_name
                FROM daily_stock_themes dst
                JOIN stocks s ON dst.stock_id = s.id
                JOIN themes t ON dst.theme_id = t.id
                WHERE dst.report_date = ?
                  AND dst.theme_id IN ({placeholders})
                  AND dst.stock_id != ?
                ORDER BY t.name_ko, dst.mention_count DESC""",
            [latest_date] + theme_ids + [stock["id"]],
        )
        peers = [dict(r) for r in await cursor.fetchall()]

        print(f"\n  [동일 테마 종목] ({latest_date} 기준)")
        if peers:
            current_theme = None
            for p in peers:
                if p["theme_name"] != current_theme:
                    current_theme = p["theme_name"]
                    print(f"\n  ▸ {current_theme}")
                pname = p.get("name_ko") or p.get("name_en") or p["ticker"]
                ind = f" ({p['industry']})" if p.get("industry") else ""
                print(f"    - {pname} ({p['ticker']}) [{p['sector']}] "
                      f"언급 {p['mention_count']}회{ind}")
        else:
            print("    동일 테마에 다른 종목이 없습니다.")

    print()
    await db.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python lookup_ticker.py <TICKER>")
        print("예시:   python lookup_ticker.py ASML")
        sys.exit(1)

    asyncio.run(lookup(sys.argv[1]))
