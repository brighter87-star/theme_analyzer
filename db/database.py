import logging
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._connection: aiosqlite.Connection | None = None

    async def initialize(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(str(self.db_path))
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute("PRAGMA journal_mode=WAL")
        await self._connection.execute("PRAGMA foreign_keys=ON")
        logger.info(f"Database initialized: {self.db_path}")

    async def get_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            await self.initialize()
        return self._connection

    async def close(self):
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Database connection closed")
