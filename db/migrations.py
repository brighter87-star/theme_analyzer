import logging

from db.database import Database

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS channels (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id   INTEGER UNIQUE NOT NULL,
    username      TEXT,
    title         TEXT NOT NULL,
    market_focus  TEXT NOT NULL DEFAULT 'BOTH',
    language      TEXT NOT NULL DEFAULT 'ko',
    is_active     INTEGER NOT NULL DEFAULT 1,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id      INTEGER NOT NULL REFERENCES channels(id),
    telegram_msg_id INTEGER NOT NULL,
    message_text    TEXT,
    has_image       INTEGER NOT NULL DEFAULT 0,
    image_path      TEXT,
    message_date    TEXT NOT NULL,
    collected_at    TEXT NOT NULL DEFAULT (datetime('now')),
    is_analyzed     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(channel_id, telegram_msg_id)
);

CREATE TABLE IF NOT EXISTS stocks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker     TEXT NOT NULL,
    name_ko    TEXT,
    name_en    TEXT,
    market     TEXT NOT NULL,
    exchange   TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(ticker, market)
);

CREATE TABLE IF NOT EXISTS stock_mentions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      INTEGER NOT NULL REFERENCES messages(id),
    stock_id        INTEGER NOT NULL REFERENCES stocks(id),
    mention_context TEXT,
    sentiment       TEXT DEFAULT 'neutral',
    confidence      REAL NOT NULL DEFAULT 0.0,
    extracted_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS themes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name_ko    TEXT NOT NULL,
    name_en    TEXT,
    market     TEXT NOT NULL,
    parent_id  INTEGER REFERENCES themes(id),
    max_stocks INTEGER NOT NULL DEFAULT 10,
    is_active  INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS daily_stock_themes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date   TEXT NOT NULL,
    stock_id      INTEGER NOT NULL REFERENCES stocks(id),
    theme_id      INTEGER NOT NULL REFERENCES themes(id),
    mention_count INTEGER NOT NULL DEFAULT 1,
    reason        TEXT,
    assigned_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(report_date, stock_id, theme_id)
);

CREATE TABLE IF NOT EXISTS daily_reports (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date             TEXT NOT NULL UNIQUE,
    total_messages_analyzed INTEGER NOT NULL DEFAULT 0,
    total_stocks_found      INTEGER NOT NULL DEFAULT 0,
    total_themes            INTEGER NOT NULL DEFAULT 0,
    telegram_sent           INTEGER NOT NULL DEFAULT 0,
    csv_exported            INTEGER NOT NULL DEFAULT 0,
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(message_date);
CREATE INDEX IF NOT EXISTS idx_messages_analyzed ON messages(is_analyzed);
CREATE INDEX IF NOT EXISTS idx_stock_mentions_stock ON stock_mentions(stock_id);
CREATE INDEX IF NOT EXISTS idx_stock_mentions_message ON stock_mentions(message_id);
CREATE INDEX IF NOT EXISTS idx_daily_stock_themes_date ON daily_stock_themes(report_date);
CREATE INDEX IF NOT EXISTS idx_daily_stock_themes_theme ON daily_stock_themes(theme_id);
CREATE INDEX IF NOT EXISTS idx_stocks_market ON stocks(market);
"""


MIGRATIONS = [
    # v1: daily_stock_themes에 sector 컬럼 추가
    "ALTER TABLE daily_stock_themes ADD COLUMN sector TEXT NOT NULL DEFAULT 'other'",
]


async def run_migrations(db: Database):
    conn = await db.get_connection()
    for statement in SCHEMA_SQL.strip().split(";"):
        statement = statement.strip()
        if statement:
            await conn.execute(statement)
    await conn.commit()

    # 추가 마이그레이션 (ALTER TABLE 등)
    for migration in MIGRATIONS:
        try:
            await conn.execute(migration)
            await conn.commit()
            logger.info(f"Migration applied: {migration[:60]}...")
        except Exception as e:
            if "duplicate column" in str(e).lower():
                pass  # 이미 적용됨
            else:
                logger.debug(f"Migration skipped: {e}")

    logger.info("Database migrations completed")
