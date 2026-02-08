import csv
import logging
import math
from datetime import datetime, timedelta
from pathlib import Path

from config.settings import Settings
from db.repository import Repository

logger = logging.getLogger(__name__)

# 시간 가중치 감쇠 계수 (0.85^30 ≈ 0.007 → 1개월 전 데이터는 거의 무시)
DECAY_FACTOR = 0.85


class ReportGenerator:
    def __init__(self, settings: Settings, repo: Repository):
        self.settings = settings
        self.repo = repo
        self.history_path = settings.export_dir / "themes_history.csv"
        self.strength_path = settings.export_dir / "themes_strength.csv"

    async def generate_daily_report(
        self, report_date: str, classification: dict
    ) -> tuple[str, Path | None]:
        """Generate Telegram message and CSV files. Returns (message, csv_path)."""
        self.settings.export_dir.mkdir(parents=True, exist_ok=True)

        # 1) 기존 히스토리 로드
        history = self._load_history()

        # 2) 어제 데이터 추출 (신규 판별용)
        yesterday_entries = self._get_previous_entries(history, report_date)

        # 3) 오늘 데이터 추가 (기존 오늘 데이터 제거 후 덮어쓰기)
        history = [r for r in history if r["date"] != report_date]
        today_rows = self._build_today_rows(report_date, classification)
        history.extend(today_rows)

        # 4) 히스토리 CSV 저장 (누적)
        self._save_history(history)

        # 5) 강도 점수 계산 + strength CSV 저장
        strength = self._calculate_strength(history, report_date)
        self._save_strength(strength)

        # 6) 텔레그램 메시지: 신규 항목만
        telegram_msg = self._build_telegram_message(
            report_date, classification, yesterday_entries
        )

        logger.info(f"CSV exported: {self.history_path}, {self.strength_path}")
        return telegram_msg, self.strength_path

    # ── 히스토리 CSV 관리 ──

    def _load_history(self) -> list[dict]:
        """themes_history.csv 로드. 없으면 빈 리스트 반환."""
        if not self.history_path.exists():
            return []

        rows = []
        with open(self.history_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["mention_count"] = int(row.get("mention_count", 1))
                rows.append(row)
        return rows

    def _get_previous_entries(self, history: list[dict], report_date: str) -> set[tuple]:
        """report_date 이전 모든 (market, theme, ticker) 조합을 반환."""
        prev = set()
        for row in history:
            if row["date"] < report_date:
                prev.add((row["market"], row["theme"], row["ticker"]))
        return prev

    def _build_today_rows(self, report_date: str, classification: dict) -> list[dict]:
        """오늘 분류 결과를 CSV row 형태로 변환."""
        rows = []
        kr = classification.get("kr", {})
        us = classification.get("us", {})

        for market_code, themes in [("KR", kr), ("US", us)]:
            for theme_name, stocks in themes.items():
                for s in stocks:
                    rows.append({
                        "date": report_date,
                        "market": market_code,
                        "sector": s.get("sector", "other"),
                        "theme": theme_name,
                        "ticker": s.get("ticker", ""),
                        "stock_name": s.get("name", ""),
                        "mention_count": s.get("mention_count", 1),
                        "sentiment": s.get("sentiment", ""),
                        "reason": s.get("reason", ""),
                    })
        return rows

    def _save_history(self, history: list[dict]):
        """누적 히스토리 CSV 저장."""
        fieldnames = [
            "date", "market", "sector", "theme", "ticker", "stock_name",
            "mention_count", "sentiment", "reason",
        ]
        with open(self.history_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in sorted(history, key=lambda r: r["date"]):
                writer.writerow({k: row.get(k, "") for k in fieldnames})

    # ── 강도 점수 계산 ──

    def _calculate_strength(self, history: list[dict], report_date: str) -> list[dict]:
        """
        시간 가중 강도 점수 계산.

        strength_score = Σ (mention_count × DECAY^days_ago)
        - 오늘: ×1.0
        - 1주 전: ×0.32
        - 2주 전: ×0.10
        - 1개월 전: ×0.007 (거의 0)
        """
        ref_date = datetime.strptime(report_date, "%Y-%m-%d")

        # (market, theme, ticker) → 집계 데이터
        agg: dict[tuple, dict] = {}

        for row in history:
            key = (row["market"], row["theme"], row["ticker"])
            row_date = datetime.strptime(row["date"], "%Y-%m-%d")
            days_ago = (ref_date - row_date).days

            if days_ago < 0:
                continue  # 미래 데이터 무시

            weight = DECAY_FACTOR ** days_ago
            score = int(row.get("mention_count", 1)) * weight

            if key not in agg:
                agg[key] = {
                    "market": row["market"],
                    "sector": row.get("sector", "other"),
                    "theme": row["theme"],
                    "ticker": row["ticker"],
                    "stock_name": row.get("stock_name", ""),
                    "strength_score": 0.0,
                    "mention_total": 0,
                    "first_seen": row["date"],
                    "last_seen": row["date"],
                    "days_count": 0,
                    "last_mention_count": 0,
                    "last_reason": "",
                }

            entry = agg[key]
            entry["strength_score"] += score
            entry["mention_total"] += int(row.get("mention_count", 1))
            entry["first_seen"] = min(entry["first_seen"], row["date"])
            entry["last_seen"] = max(entry["last_seen"], row["date"])
            entry["days_count"] += 1

            if row["date"] == report_date:
                entry["last_mention_count"] = int(row.get("mention_count", 1))
                entry["last_reason"] = row.get("reason", "")

        # 트렌드 판별
        results = []
        for key, entry in agg.items():
            if entry["first_seen"] == report_date:
                trend = "NEW"
            elif entry["last_seen"] == report_date:
                trend = "ACTIVE"
            else:
                trend = "INACTIVE"

            entry["trend"] = trend
            entry["strength_score"] = round(entry["strength_score"], 2)
            results.append(entry)

        # 강도 순 정렬
        results.sort(key=lambda x: (-x["strength_score"],))
        return results

    def _save_strength(self, strength: list[dict]):
        """강도 점수 CSV 저장. 매일 재계산."""
        fieldnames = [
            "market", "sector", "theme", "ticker", "stock_name",
            "strength_score", "mention_total", "last_mention_count",
            "first_seen", "last_seen", "days_count", "trend", "last_reason",
        ]
        with open(self.strength_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in strength:
                writer.writerow({k: row.get(k, "") for k in fieldnames})

    # ── 텔레그램 메시지 (신규 항목만) ──

    def _build_telegram_message(
        self,
        report_date: str,
        classification: dict,
        prev_entries: set[tuple],
    ) -> str:
        kr = classification.get("kr", {})
        us = classification.get("us", {})

        # KR/US 통합 - 신규 항목만 추출
        new_themes: dict[str, list] = {}   # 완전 새로운 테마
        added_themes: dict[str, list] = {}  # 기존 테마에 신규 종목

        prev_theme_names = {t for _, t, _ in prev_entries}

        for market_code, themes in [("KR", kr), ("US", us)]:
            for theme_name, stocks in themes.items():
                is_new_theme = theme_name not in prev_theme_names
                for s in stocks:
                    ticker = s.get("ticker", "")
                    key = (market_code, theme_name, ticker)
                    if key not in prev_entries:
                        target = new_themes if is_new_theme else added_themes
                        if theme_name not in target:
                            target[theme_name] = []
                        target[theme_name].append(s)

        lines = [
            f"<b>일일 테마 업데이트</b> - {report_date}",
            "",
        ]

        has_content = False

        # 신규 테마
        for theme_name in sorted(new_themes.keys()):
            stocks = new_themes[theme_name]
            lines.append(f"🆕 <b>{theme_name}</b> ({len(stocks)}종목)")
            for s in stocks[:10]:
                self._append_stock_line(lines, s)
            lines.append("")
            has_content = True

        # 기존 테마에 추가된 종목
        for theme_name in sorted(added_themes.keys()):
            stocks = added_themes[theme_name]
            lines.append(f"📈 <b>{theme_name}</b> +{len(stocks)}종목")
            for s in stocks[:10]:
                self._append_stock_line(lines, s)
            lines.append("")
            has_content = True

        if not has_content:
            lines.append("오늘 신규 종목/테마 변동이 없습니다.")
            lines.append("")

        # 요약
        total = sum(len(v) for v in kr.values()) + sum(len(v) for v in us.values())
        new_count = (
            sum(len(v) for v in new_themes.values())
            + sum(len(v) for v in added_themes.values())
        )
        total_themes = len(kr) + len(us)
        lines.append(
            f"📊 전체 {total}종목 중 <b>신규 {new_count}건</b> | 테마 {total_themes}개"
        )

        return "\n".join(lines)

    @staticmethod
    def _append_stock_line(lines: list, s: dict):
        name = s.get("name", s.get("ticker", "?"))
        ticker = s.get("ticker", "")
        reason = s.get("reason", "")
        # ticker가 이름과 다르면 괄호로 표시
        ticker_str = f" ({ticker})" if ticker and ticker != name else ""
        reason_str = f" - {reason}" if reason else ""
        lines.append(f"  • {name}{ticker_str}{reason_str}")

    @staticmethod
    def split_message(text: str, max_len: int = 4096) -> list[str]:
        """Split long messages at newlines, respecting max_len."""
        if len(text) <= max_len:
            return [text]

        chunks = []
        current = ""
        for line in text.split("\n"):
            if len(current) + len(line) + 1 > max_len:
                if current:
                    chunks.append(current)
                current = line
            else:
                current = f"{current}\n{line}" if current else line

        if current:
            chunks.append(current)

        return chunks
