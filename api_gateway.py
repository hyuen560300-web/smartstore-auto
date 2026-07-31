"""API Gateway — TokenBucket rate limiter + 429 circuit breaker.
사용법:
    from api_gateway import dg_gateway, naver_gateway
    result = await dg_gateway.call(my_async_fn, arg1, arg2)
"""
import asyncio
import os
import time


class _TokenBucket:
    """토큰버킷 rate limiter."""

    def __init__(self, rate_per_min: int, burst: int | None = None):
        self._rate = rate_per_min / 60.0
        self._burst = float(burst or rate_per_min)
        self._tokens = self._burst
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self._burst,
                    self._tokens + (now - self._last) * self._rate,
                )
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            await asyncio.sleep(wait)


class ApiGateway:
    """단일 외부 API용 게이트웨이.

    - rate limit: TokenBucket (rate_per_min 기본 30)
    - 429 감지: exponential backoff (10→20→40초)
    - circuit breaker: 429 3회 연속 → 5분 차단 + SOURCING_PAUSED=true
    """

    def __init__(self, name: str, rate_per_min: int = 30):
        self.name = name
        self._bucket = _TokenBucket(rate_per_min)
        self._open_until = 0.0
        self._consec_429 = 0

    @property
    def circuit_open(self) -> bool:
        if self._open_until > 0.0 and time.monotonic() >= self._open_until:
            # circuit 만료 시 자동 reset — SOURCING_PAUSED 런타임 플래그 해제
            self.reset_circuit()
        return time.monotonic() < self._open_until

    def reset_circuit(self) -> None:
        self._open_until = 0.0
        self._consec_429 = 0
        os.environ["SOURCING_PAUSED"] = "false"

    async def call(self, fn, *args, retries: int = 3, **kwargs):
        """rate limit 통과 후 fn(*args, **kwargs) 실행.

        429 응답 시 backoff 후 재시도. 3회 연속 429 → circuit open.
        circuit open 중에는 즉시 RuntimeError.
        """
        if self.circuit_open:
            rem = int(self._open_until - time.monotonic())
            raise RuntimeError(
                f"[{self.name}] circuit open — {rem}초 남음. SOURCING_PAUSED=true"
            )

        await self._bucket.acquire()

        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                result = await fn(*args, **kwargs)
                self._consec_429 = 0
                return result
            except Exception as e:
                last_exc = e
                err = str(e)
                is_429 = "429" in err or "rate limit" in err.lower() or "too many" in err.lower()
                if not is_429:
                    raise
                self._consec_429 += 1
                wait = 10 * (2**attempt)  # 10 → 20 → 40초
                print(
                    f"[{self.name}] 429 #{self._consec_429} → {wait}초 대기 "
                    f"(시도 {attempt + 1}/{retries})",
                    flush=True,
                )
                if self._consec_429 >= 3:
                    self._open_until = time.monotonic() + 300  # 5분
                    os.environ["SOURCING_PAUSED"] = "true"
                    print(
                        f"[{self.name}] ⛔ circuit open 5분 — SOURCING_PAUSED=true",
                        flush=True,
                    )
                await asyncio.sleep(wait)

        raise last_exc  # type: ignore[misc]


# ── 싱글톤 인스턴스 ────────────────────────────────────────────────────────────
dg_gateway = ApiGateway("DG", rate_per_min=30)      # 도매꾹: 30req/min 보수적
naver_gateway = ApiGateway("NAVER", rate_per_min=20)  # 네이버: 20req/min
