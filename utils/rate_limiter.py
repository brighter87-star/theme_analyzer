import asyncio
import time


class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate  # tokens per second
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now

    def consume(self, n: int = 1) -> bool:
        self._refill()
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

    def time_until_available(self, n: int = 1) -> float:
        self._refill()
        if self.tokens >= n:
            return 0.0
        deficit = n - self.tokens
        return deficit / self.rate


class RateLimiter:
    def __init__(self):
        self._buckets: dict[str, TokenBucket] = {}

    def add_bucket(self, name: str, rate: float, capacity: int):
        """rate: tokens/second, capacity: max burst size."""
        self._buckets[name] = TokenBucket(rate, capacity)

    async def acquire(self, bucket_name: str, tokens: int = 1):
        bucket = self._buckets[bucket_name]
        while not bucket.consume(tokens):
            wait_time = bucket.time_until_available(tokens)
            await asyncio.sleep(wait_time)
