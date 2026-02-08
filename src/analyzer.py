import json
import logging
from pathlib import Path

import anthropic

from config.settings import Settings
from db.repository import Repository
from utils.image_utils import image_to_base64, resize_if_needed
from utils.rate_limiter import RateLimiter
from utils.stock_registry import StockRegistry

logger = logging.getLogger(__name__)

TEXT_ANALYSIS_PROMPT = """다음은 한국/미국 주식 관련 텔레그램 채널 메시지들입니다.
각 메시지에서 언급된 종목을 추출해주세요.

규칙:
1. 종목명, 시장(KR/US), 맥락(왜 언급되었는지 1줄), 감성(positive/negative/neutral)을 추출
2. 종목이 아닌 일반 단어는 제외 (예: AI, ETF, HBM 자체는 종목이 아님. 하지만 "SK하이닉스 HBM"은 SK하이닉스를 추출)
3. 약어나 별명은 정식 종목명으로 변환:
   - 삼전 -> 삼성전자, 하닉 -> SK하이닉스, 엘솔 -> LG에너지솔루션
   - 에프엠 -> 에코프로비엠, 포홀 -> 포스코홀딩스
   - 엔비디아 -> NVDA, 테슬라 -> TSLA 등
4. 미국 종목은 영문 티커로 표기 (NVDA, AAPL, MSFT 등)
5. 한국 종목은 한글 정식명으로 표기
6. 종목이 없는 메시지는 빈 배열로 반환

메시지들:
{messages}

반드시 아래 JSON 형식으로만 응답하세요. 다른 텍스트는 포함하지 마세요:
[
  {{
    "msg_id": <메시지 ID>,
    "stocks": [
      {{
        "name": "<종목명 또는 티커>",
        "market": "KR" 또는 "US",
        "context": "<언급 맥락 1줄>",
        "sentiment": "positive" 또는 "negative" 또는 "neutral"
      }}
    ]
  }}
]"""

IMAGE_ANALYSIS_PROMPT = """이 이미지는 주식 관련 텔레그램 채널에서 공유된 것입니다.
이미지에서 다음 정보를 추출해주세요:

1. 보이는 모든 종목명/티커
2. 각 종목의 시장 (KR/US)
3. 이미지의 맥락 (차트, 뉴스 스크린샷, 종목 리스트 등)
4. 각 종목에 대한 감성 (positive/negative/neutral)

약어 변환 규칙:
- 삼전 -> 삼성전자, 하닉 -> SK하이닉스 등
- 미국 종목은 영문 티커로 (NVDA, AAPL 등)

차트가 있다면 종목명과 추세(상승/하락/횡보)를 파악해주세요.

반드시 아래 JSON 형식으로만 응답하세요. 종목 정보가 없으면 빈 배열 []을 반환:
[
  {
    "name": "<종목명 또는 티커>",
    "market": "KR" 또는 "US",
    "context": "<맥락 1줄>",
    "sentiment": "positive" 또는 "negative" 또는 "neutral"
  }
]"""


class StockAnalyzer:
    def __init__(
        self,
        settings: Settings,
        repo: Repository,
        registry: StockRegistry,
        rate_limiter: RateLimiter,
    ):
        self.settings = settings
        self.repo = repo
        self.registry = registry
        self.rate_limiter = rate_limiter
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def analyze_pending_messages(self) -> dict:
        text_msgs = await self.repo.get_unanalyzed_messages(has_image=False)
        image_msgs = await self.repo.get_unanalyzed_messages(has_image=True)

        logger.info(
            f"Analyzing {len(text_msgs)} text + {len(image_msgs)} image messages"
        )

        total_stocks = 0
        errors = 0

        # Process text in batches
        for i in range(0, len(text_msgs), self.settings.batch_size):
            batch = text_msgs[i : i + self.settings.batch_size]
            try:
                count = await self._analyze_text_batch(batch)
                total_stocks += count
            except Exception as e:
                logger.error(f"Text batch analysis error: {e}")
                errors += 1

        # Process images: text-with-image → text batch, image-only → Vision API
        image_with_text = [m for m in image_msgs if m.get("message_text", "").strip()]
        image_only = [m for m in image_msgs if not m.get("message_text", "").strip()]

        # Images with text: just analyze the text (cheap, Haiku)
        if image_with_text:
            for i in range(0, len(image_with_text), self.settings.batch_size):
                batch = image_with_text[i : i + self.settings.batch_size]
                try:
                    count = await self._analyze_text_batch(batch)
                    total_stocks += count
                except Exception as e:
                    logger.error(f"Image-text batch error: {e}")
                    errors += 1

        # Images without text: use Vision API (selective)
        for msg in image_only:
            try:
                count = await self._analyze_image(msg)
                total_stocks += count
            except Exception as e:
                logger.error(f"Image analysis error (msg {msg['id']}): {e}")
                errors += 1

        logger.info(
            f"Image split: {len(image_with_text)} text-analyzed, "
            f"{len(image_only)} vision-analyzed"
        )

        stats = {
            "text_messages": len(text_msgs),
            "image_messages": len(image_msgs),
            "stocks_extracted": total_stocks,
            "errors": errors,
        }
        logger.info(f"Analysis complete: {stats}")
        return stats

    async def _analyze_text_batch(self, messages: list[dict]) -> int:
        if not messages:
            return 0

        combined = "\n---\n".join(
            f"[MSG_ID:{m['id']}] {m['message_text']}" for m in messages
        )

        prompt = TEXT_ANALYSIS_PROMPT.format(messages=combined)

        await self.rate_limiter.acquire("claude")
        response = await self.client.messages.create(
            model=self.settings.claude_model,
            max_tokens=self.settings.claude_max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )

        raw_text = response.content[0].text.strip()
        parsed = self._parse_json_response(raw_text)
        if parsed is None:
            logger.warning(f"Failed to parse text analysis response")
            return 0

        # 배치 내 유효한 msg_id 집합
        valid_ids = {m["id"] for m in messages}

        stock_count = 0
        for item in parsed:
            msg_id = item.get("msg_id")
            # Claude가 반환한 msg_id가 이 배치에 없으면 무시
            if msg_id not in valid_ids:
                logger.debug(
                    f"msg_id {msg_id} not in batch {valid_ids}, skipping"
                )
                continue
            for stock_info in item.get("stocks", []):
                saved = await self._save_stock_mention(
                    message_id=msg_id,
                    stock_info=stock_info,
                )
                if saved:
                    stock_count += 1

        # Mark messages as analyzed
        all_ids = [m["id"] for m in messages]
        await self.repo.mark_messages_analyzed(all_ids)
        return stock_count

    async def _analyze_image(self, message: dict) -> int:
        image_path = Path(message["image_path"])
        if not image_path.exists():
            logger.warning(f"Image not found: {image_path}")
            await self.repo.mark_message_analyzed(message["id"])
            return 0

        image_path = resize_if_needed(image_path, self.settings.max_image_size_kb)
        image_data, media_type = image_to_base64(image_path)

        await self.rate_limiter.acquire("claude")
        response = await self.client.messages.create(
            model=self.settings.claude_vision_model,
            max_tokens=self.settings.claude_max_tokens,
            messages=[
                {
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
                        {"type": "text", "text": IMAGE_ANALYSIS_PROMPT},
                    ],
                }
            ],
        )

        raw_text = response.content[0].text.strip()
        parsed = self._parse_json_response(raw_text)
        if parsed is None:
            logger.warning(f"Failed to parse image analysis for msg {message['id']}")
            await self.repo.mark_message_analyzed(message["id"])
            return 0

        stock_count = 0
        for stock_info in parsed:
            saved = await self._save_stock_mention(
                message_id=message["id"],
                stock_info=stock_info,
            )
            if saved:
                stock_count += 1

        # Also analyze text if present
        if message.get("message_text"):
            # Text accompanying the image - extract from it too
            pass

        await self.repo.mark_message_analyzed(message["id"])
        return stock_count

    async def _save_stock_mention(self, message_id: int, stock_info: dict) -> bool:
        name = stock_info.get("name", "").strip()
        market = stock_info.get("market", "KR")
        context = stock_info.get("context", "")
        sentiment = stock_info.get("sentiment", "neutral")

        if not name:
            return False

        stock_id = await self.registry.resolve_stock(name, market)
        if stock_id is None:
            logger.debug(f"Could not resolve stock: {name} ({market})")
            return False

        await self.repo.insert_stock_mention(
            message_id=message_id,
            stock_id=stock_id,
            mention_context=context,
            sentiment=sentiment,
            confidence=0.8,
        )
        return True

    def _parse_json_response(self, text: str) -> list | None:
        import re

        text = text.strip()

        # Remove markdown code fences
        if text.startswith("```"):
            lines = text.split("\n")
            lines = lines[1:]
            if lines and lines[-1].strip().startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        # 1) Direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 2) Extract JSON array
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        # 3) Extract JSON object (for classifier responses)
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group())
                if isinstance(result, dict):
                    return result
            except json.JSONDecodeError:
                pass

        # 4) Try to fix truncated JSON by closing brackets
        for suffix in ["]", "}]", "}]}]", '"}]']:
            try:
                result = json.loads(text + suffix)
                if isinstance(result, list):
                    return result
            except json.JSONDecodeError:
                continue

        logger.warning(f"Cannot parse JSON: {text[:300]}...")
        return None
