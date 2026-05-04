"""模块 3：OI 轮询。

- Aster 是 Binance fork，预期 `/fapi/v1/openInterest?symbol=...` 可用。
- 启动时做一次端点探测；不可用则降级为 `premiumIndex`。"""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from signal_engine import SignalEngine

logger = logging.getLogger(__name__)

OPEN_INTEREST_PATH = "/fapi/v1/openInterest"
PREMIUM_INDEX_PATH = "/fapi/v1/premiumIndex"
HTTP_TIMEOUT_SEC = 10


class OIEndpointError(RuntimeError):
    pass


class OIPoller:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        rest_base: str,
        engine: SignalEngine,
        poll_interval_sec: int,
        concurrency: int = 20,
    ) -> None:
        self._session = session
        self._rest_base = rest_base.rstrip("/")
        self._engine = engine
        self._poll_interval = poll_interval_sec
        self._mode: str = "openInterest"  # 或 "premiumIndex"
        self._sem = asyncio.Semaphore(max(1, concurrency))
        self._concurrency = max(1, concurrency)

    async def detect_endpoint(self, probe_symbol: str) -> None:
        """启动时探测一次，确定走哪个端点。"""
        url = f"{self._rest_base}{OPEN_INTEREST_PATH}?symbol={probe_symbol}"
        try:
            async with self._session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "openInterest" in data:
                        self._mode = "openInterest"
                        logger.info("OI 端点可用：%s", OPEN_INTEREST_PATH)
                        return
                logger.warning(
                    "OI 端点 %s 返回 status=%s，尝试降级 premiumIndex",
                    OPEN_INTEREST_PATH,
                    resp.status,
                )
        except Exception as exc:
            logger.warning("OI 端点探测异常 %s，尝试降级 premiumIndex", exc)

        # 降级探测
        url = f"{self._rest_base}{PREMIUM_INDEX_PATH}?symbol={probe_symbol}"
        try:
            async with self._session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "openInterest" in data:
                        self._mode = "premiumIndex"
                        logger.info("OI 通过 %s 字段获取（降级模式）", PREMIUM_INDEX_PATH)
                        return
        except Exception as exc:
            logger.error("premiumIndex 探测也失败：%s", exc)

        raise OIEndpointError("Aster 上没有可用的 OI 数据源，请检查 API 文档")

    async def _fetch_one(self, symbol: str) -> bool:
        """请求 + 落库到 engine。返回是否拿到有效值。"""
        if self._mode == "openInterest":
            url = f"{self._rest_base}{OPEN_INTEREST_PATH}?symbol={symbol}"
        else:
            url = f"{self._rest_base}{PREMIUM_INDEX_PATH}?symbol={symbol}"

        async with self._sem:
            try:
                async with self._session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC),
                ) as resp:
                    if resp.status != 200:
                        if resp.status in (418, 429):
                            logger.warning("OI %s 限速 status=%s，本轮跳过", symbol, resp.status)
                        else:
                            body = await resp.text()
                            logger.debug("OI %s status=%s body=%s", symbol, resp.status, body[:200])
                        return False
                    data = await resp.json()
            except asyncio.TimeoutError:
                logger.debug("OI %s 超时", symbol)
                return False
            except Exception as exc:
                logger.debug("OI %s 异常 %s", symbol, exc)
                return False

        raw = data.get("openInterest")
        if raw is None:
            return False
        try:
            oi = float(raw)
        except (TypeError, ValueError):
            return False

        self._engine.on_oi_sample(symbol, oi, time.time())
        return True

    async def run(self) -> None:
        logger.info(
            "OI poller 启动 mode=%s interval=%ds 并发=%d",
            self._mode,
            self._poll_interval,
            self._concurrency,
        )
        while True:
            symbols = self._engine.all_symbols()
            cycle_start = time.monotonic()
            results = await asyncio.gather(
                *(self._fetch_one(s) for s in symbols),
                return_exceptions=True,
            )
            count_ok = sum(1 for r in results if r is True)
            elapsed = time.monotonic() - cycle_start
            logger.info(
                "OI 轮询完成 ok=%d/%d 耗时=%.1fs",
                count_ok,
                len(symbols),
                elapsed,
            )
            sleep_for = max(0.0, self._poll_interval - elapsed)
            await asyncio.sleep(sleep_for)
