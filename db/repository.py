import logging
from typing import Optional

from db.database import Database

logger = logging.getLogger(__name__)


class Repository:
    def __init__(self, database: Database):
        self.db = database

    # ── Channel operations ──

    async def get_active_channels(self) -> list[dict]:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            "SELECT * FROM channels WHERE is_active = 1"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def upsert_channel(
        self,
        telegram_id: int,
        username: Optional[str],
        title: str,
        market_focus: str = "BOTH",
        language: str = "ko",
    ) -> int:
        conn = await self.db.get_connection()
        await conn.execute(
            """INSERT INTO channels (telegram_id, username, title, market_focus, language)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(telegram_id) DO UPDATE SET
                 username = excluded.username,
                 title = excluded.title,
                 market_focus = excluded.market_focus,
                 language = excluded.language,
                 updated_at = datetime('now')""",
            (telegram_id, username, title, market_focus, language),
        )
        await conn.commit()
        cursor = await conn.execute(
            "SELECT id FROM channels WHERE telegram_id = ?", (telegram_id,)
        )
        row = await cursor.fetchone()
        return row["id"]

    async def deactivate_channel(self, username: str) -> bool:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            "UPDATE channels SET is_active = 0, updated_at = datetime('now') WHERE username = ?",
            (username,),
        )
        await conn.commit()
        return cursor.rowcount > 0

    async def activate_channel(self, username: str) -> bool:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            "UPDATE channels SET is_active = 1, updated_at = datetime('now') WHERE username = ?",
            (username,),
        )
        await conn.commit()
        return cursor.rowcount > 0

    async def get_all_channels(self) -> list[dict]:
        conn = await self.db.get_connection()
        cursor = await conn.execute("SELECT * FROM channels ORDER BY is_active DESC, title")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Message operations ──

    async def message_exists(self, channel_id: int, telegram_msg_id: int) -> bool:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            "SELECT 1 FROM messages WHERE channel_id = ? AND telegram_msg_id = ?",
            (channel_id, telegram_msg_id),
        )
        return await cursor.fetchone() is not None

    async def insert_message(
        self,
        channel_id: int,
        telegram_msg_id: int,
        message_text: Optional[str],
        has_image: bool,
        image_path: Optional[str],
        message_date: str,
    ) -> int:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """INSERT OR IGNORE INTO messages
               (channel_id, telegram_msg_id, message_text, has_image, image_path, message_date)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (channel_id, telegram_msg_id, message_text, int(has_image), image_path, message_date),
        )
        await conn.commit()
        return cursor.lastrowid

    async def get_unanalyzed_messages(
        self, has_image: Optional[bool] = None, limit: int = 500
    ) -> list[dict]:
        conn = await self.db.get_connection()
        query = "SELECT * FROM messages WHERE is_analyzed = 0"
        params: list = []
        if has_image is not None:
            query += " AND has_image = ?"
            params.append(int(has_image))
        query += " ORDER BY message_date ASC LIMIT ?"
        params.append(limit)
        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def mark_message_analyzed(self, message_id: int):
        conn = await self.db.get_connection()
        await conn.execute(
            "UPDATE messages SET is_analyzed = 1 WHERE id = ?", (message_id,)
        )
        await conn.commit()

    async def mark_messages_analyzed(self, message_ids: list[int]):
        if not message_ids:
            return
        conn = await self.db.get_connection()
        placeholders = ",".join("?" for _ in message_ids)
        await conn.execute(
            f"UPDATE messages SET is_analyzed = 1 WHERE id IN ({placeholders})",
            message_ids,
        )
        await conn.commit()

    # ── Stock operations ──

    async def get_or_create_stock(
        self,
        ticker: str,
        name_ko: Optional[str],
        name_en: Optional[str],
        market: str,
        exchange: Optional[str] = None,
    ) -> int:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            "SELECT id FROM stocks WHERE ticker = ? AND market = ?",
            (ticker, market),
        )
        row = await cursor.fetchone()
        if row:
            return row["id"]

        cursor = await conn.execute(
            """INSERT INTO stocks (ticker, name_ko, name_en, market, exchange)
               VALUES (?, ?, ?, ?, ?)""",
            (ticker, name_ko, name_en, market, exchange),
        )
        await conn.commit()
        return cursor.lastrowid

    async def search_stock(self, query: str) -> list[dict]:
        conn = await self.db.get_connection()
        pattern = f"%{query}%"
        cursor = await conn.execute(
            """SELECT * FROM stocks
               WHERE name_ko LIKE ? OR name_en LIKE ? OR ticker LIKE ?""",
            (pattern, pattern, pattern),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Stock mention operations ──

    async def insert_stock_mention(
        self,
        message_id: int,
        stock_id: int,
        mention_context: Optional[str] = None,
        sentiment: str = "neutral",
        confidence: float = 0.0,
    ) -> int:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """INSERT INTO stock_mentions
               (message_id, stock_id, mention_context, sentiment, confidence)
               VALUES (?, ?, ?, ?, ?)""",
            (message_id, stock_id, mention_context, sentiment, confidence),
        )
        await conn.commit()
        return cursor.lastrowid

    async def get_daily_stock_mentions(self, report_date: str) -> list[dict]:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """SELECT
                 s.id as stock_id, s.ticker, s.name_ko, s.name_en, s.market, s.exchange,
                 COUNT(sm.id) as mention_count,
                 GROUP_CONCAT(sm.mention_context, ' | ') as aggregated_context,
                 CASE
                   WHEN SUM(CASE WHEN sm.sentiment = 'positive' THEN 1 ELSE 0 END) >=
                        SUM(CASE WHEN sm.sentiment = 'negative' THEN 1 ELSE 0 END)
                   THEN 'positive' ELSE 'negative'
                 END as dominant_sentiment,
                 AVG(sm.confidence) as avg_confidence
               FROM stock_mentions sm
               JOIN messages m ON sm.message_id = m.id
               JOIN stocks s ON sm.stock_id = s.id
               WHERE DATE(m.message_date) = ?
               GROUP BY s.id
               HAVING AVG(sm.confidence) >= 0.2 OR COUNT(sm.id) >= 1
               ORDER BY COUNT(sm.id) DESC""",
            (report_date,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Theme operations ──

    async def get_themes(self, market: Optional[str] = None) -> list[dict]:
        conn = await self.db.get_connection()
        if market:
            cursor = await conn.execute(
                "SELECT * FROM themes WHERE is_active = 1 AND (market = ? OR market = 'BOTH')",
                (market,),
            )
        else:
            cursor = await conn.execute(
                "SELECT * FROM themes WHERE is_active = 1"
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_or_create_theme(
        self,
        name_ko: str,
        name_en: Optional[str],
        market: str,
        parent_id: Optional[int] = None,
    ) -> int:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            "SELECT id FROM themes WHERE name_ko = ? AND market = ?",
            (name_ko, market),
        )
        row = await cursor.fetchone()
        if row:
            return row["id"]

        cursor = await conn.execute(
            """INSERT INTO themes (name_ko, name_en, market, parent_id)
               VALUES (?, ?, ?, ?)""",
            (name_ko, name_en, market, parent_id),
        )
        await conn.commit()
        return cursor.lastrowid

    # ── Daily classification operations ──

    async def insert_daily_stock_theme(
        self,
        report_date: str,
        stock_id: int,
        theme_id: int,
        mention_count: int = 1,
        reason: Optional[str] = None,
        sector: str = "other",
    ):
        conn = await self.db.get_connection()
        await conn.execute(
            """INSERT OR REPLACE INTO daily_stock_themes
               (report_date, stock_id, theme_id, mention_count, reason, sector)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (report_date, stock_id, theme_id, mention_count, reason, sector),
        )
        await conn.commit()

    async def get_daily_classification(self, report_date: str) -> dict:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            """SELECT
                 dst.report_date, dst.mention_count, dst.reason, dst.sector,
                 s.ticker, s.name_ko, s.name_en, s.market, s.exchange,
                 t.name_ko as theme_name_ko, t.name_en as theme_name_en, t.market as theme_market
               FROM daily_stock_themes dst
               JOIN stocks s ON dst.stock_id = s.id
               JOIN themes t ON dst.theme_id = t.id
               WHERE dst.report_date = ?
               ORDER BY t.market, t.name_ko, dst.mention_count DESC""",
            (report_date,),
        )
        rows = await cursor.fetchall()
        if not rows:
            return {}

        result = {"kr": {}, "us": {}}
        for row in rows:
            row = dict(row)
            market_key = "kr" if row["market"] == "KR" else "us"
            theme_name = row["theme_name_ko"]
            if theme_name not in result[market_key]:
                result[market_key][theme_name] = []
            result[market_key][theme_name].append({
                "name": row["name_ko"] or row["name_en"] or row["ticker"],
                "ticker": row["ticker"],
                "sector": row.get("sector", "other"),
                "reason": row["reason"] or "",
                "mention_count": row["mention_count"],
            })
        return result

    # ── Report tracking ──

    async def record_daily_report(
        self,
        report_date: str,
        total_messages: int,
        total_stocks: int,
        total_themes: int,
        telegram_sent: bool = False,
        csv_exported: bool = False,
    ):
        conn = await self.db.get_connection()
        await conn.execute(
            """INSERT OR REPLACE INTO daily_reports
               (report_date, total_messages_analyzed, total_stocks_found,
                total_themes, telegram_sent, csv_exported)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (report_date, total_messages, total_stocks, total_themes,
             int(telegram_sent), int(csv_exported)),
        )
        await conn.commit()

    async def get_report_status(self, report_date: str) -> Optional[dict]:
        conn = await self.db.get_connection()
        cursor = await conn.execute(
            "SELECT * FROM daily_reports WHERE report_date = ?", (report_date,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None
