"""模块 2：Aster WebSocket 管理（combined stream）。

每条连接最多承载 WS_BATCH_SIZE 个 symbol（每 symbol 2 个 stream）。
断线指数退避重连，符合 Binance fork 行为。"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Iterable

import websockets
from websockets.exceptions import ConnectionClosed

from signal_engine import SignalEngine

logger = logging.getLogger(__name__)


def _chunk(items: Iterable[str], size: int) -> list[list[str]]:
    items = list(items)
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def _build_streams(symbols: list[str]) -> str:
    parts: list[str] = []
    for s in symbols:
        sym = s.lower()
        parts.append(f"{sym}@bookTicker")
        parts.append(f"{sym}@aggTrade")
    return "/".join(parts)


def _build_url(ws_base: str, symbols: list[str]) -> str:
    return f"{ws_base.rstrip('/')}/stream?streams={_build_streams(symbols)}"


class _BatchConnection:
    def __init__(
        self,
        ws_base: str,
        symbols: list[str],
        engine: SignalEngine,
        batch_id: int,
    ) -> None:
        self._ws_base = ws_base
        self._symbols = symbols
        self._engine = engine
        self._batch_id = batch_id

    async def run(self, stop_event: asyncio.Event) -> None:
        url = _build_url(self._ws_base, self._symbols)
        backoff = 1.0
        log_url = url[:120] + ("..." if len(url) > 120 else "")
        while not stop_event.is_set():
            try:
                logger.info(
                    "WS[%d] 连接中 (%d symbols) %s",
                    self._batch_id,
                    len(self._symbols),
                    log_url,
                )
                async with websockets.connect(
                    url,
                    max_size=2**20,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                ) as ws:
                    logger.info("WS[%d] 已连接", self._batch_id)
                    backoff = 1.0
                    await self._consume(ws, stop_event)
            except asyncio.CancelledError:
                raise
            except ConnectionClosed as exc:
                logger.warning("WS[%d] 关闭 code=%s reason=%s", self._batch_id, exc.code, exc.reason)
            except Exception as exc:
                logger.exception("WS[%d] 异常: %s", self._batch_id, exc)

            if stop_event.is_set():
                return
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                return
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2.0, 60.0)

    async def _consume(self, ws, stop_event: asyncio.Event) -> None:
        async for raw in ws:
            if stop_event.is_set():
                return
            try:
                msg = json.loads(raw)
            except (TypeError, ValueError):
                continue
            await self._dispatch(msg)

    async def _dispatch(self, msg: dict) -> None:
        # combined stream 包了一层 {stream, data}
        data = msg.get("data") if "stream" in msg else msg
        if not isinstance(data, dict):
            return

        event = data.get("e")
        symbol = data.get("s")
        if not symbol:
            return

        stream_name = (msg.get("stream") or "").lower()

        if event == "bookTicker" or stream_name.endswith("@booktickers") or stream_name.endswith("@bookticker"):
            self._handle_book_ticker(symbol, data)
        elif event == "aggTrade" or stream_name.endswith("@aggtrade"):
            await self._handle_agg_trade(symbol, data)
        # 其他事件忽略

    def _handle_book_ticker(self, symbol: str, data: dict) -> None:
        try:
            bid = float(data.get("b") or 0)
            ask = float(data.get("a") or 0)
        except (TypeError, ValueError):
            return
        if bid > 0 and ask > 0:
            self._engine.on_book_ticker(symbol, bid, ask)

    async def _handle_agg_trade(self, symbol: str, data: dict) -> None:
        try:
            price = float(data.get("p") or 0)
        except (TypeError, ValueError):
            return
        if price <= 0:
            return
        # 用本地时钟做窗口对齐，避免和 OI 采样时间不一致
        await self._engine.on_agg_trade(symbol, price, time.time())


class WSManager:
    def __init__(self, ws_base: str, batch_size: int, engine: SignalEngine) -> None:
        self._ws_base = ws_base
        self._batch_size = batch_size
        self._engine = engine
        self._tasks: list[asyncio.Task] = []
        self._stop_events: list[asyncio.Event] = []

    async def start(self, symbols: list[str]) -> None:
        await self.stop()
        if not symbols:
            logger.warning("WSManager: symbol 列表为空，不启动连接")
            return
        batches = _chunk(symbols, self._batch_size)
        logger.info(
            "WSManager: 启动 %d 个连接，共 %d 个 symbol",
            len(batches),
            len(symbols),
        )
        for i, batch in enumerate(batches):
            ev = asyncio.Event()
            conn = _BatchConnection(self._ws_base, batch, self._engine, batch_id=i)
            task = asyncio.create_task(conn.run(ev), name=f"ws-batch-{i}")
            self._tasks.append(task)
            self._stop_events.append(ev)

    async def stop(self) -> None:
        if not self._tasks:
            return
        for ev in self._stop_events:
            ev.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._stop_events.clear()
