from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Channel(BaseModel):
    id: Optional[int] = None
    telegram_id: int
    username: Optional[str] = None
    title: str
    market_focus: str = "BOTH"  # KR, US, BOTH
    language: str = "ko"  # ko, en, mixed
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class Message(BaseModel):
    id: Optional[int] = None
    channel_id: int
    telegram_msg_id: int
    message_text: Optional[str] = None
    has_image: bool = False
    image_path: Optional[str] = None
    message_date: datetime
    collected_at: Optional[datetime] = None
    is_analyzed: bool = False


class Stock(BaseModel):
    id: Optional[int] = None
    ticker: str
    name_ko: Optional[str] = None
    name_en: Optional[str] = None
    market: str  # KR or US
    exchange: Optional[str] = None  # KOSPI, KOSDAQ, NYSE, NASDAQ
    created_at: Optional[datetime] = None


class StockMention(BaseModel):
    id: Optional[int] = None
    message_id: int
    stock_id: Optional[int] = None
    raw_name: str
    market_hint: str  # KR or US
    mention_context: Optional[str] = None
    sentiment: str = "neutral"  # positive, negative, neutral
    confidence: float = 0.0
    extracted_at: Optional[datetime] = None


class Theme(BaseModel):
    id: Optional[int] = None
    name_ko: str
    name_en: Optional[str] = None
    market: str  # KR, US, BOTH
    parent_id: Optional[int] = None
    max_stocks: int = 10
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DailyStockTheme(BaseModel):
    id: Optional[int] = None
    report_date: str  # YYYY-MM-DD
    stock_id: int
    theme_id: int
    mention_count: int = 1
    reason: Optional[str] = None
    assigned_at: Optional[datetime] = None


class DailyReport(BaseModel):
    id: Optional[int] = None
    report_date: str
    total_messages_analyzed: int = 0
    total_stocks_found: int = 0
    total_themes: int = 0
    telegram_sent: bool = False
    csv_exported: bool = False
    created_at: Optional[datetime] = None
