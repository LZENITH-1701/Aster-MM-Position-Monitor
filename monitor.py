"""主入口：装配并运行整个监控。"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

import aiohttp

from alert import AlertSink
from config import Config, load_config
from oi_poller import OIEndpointError, OIPoller
from signal_engine import SignalEngine
from symbols import fetch_intersection_symbols
from ws_manager import WSManager

logger = logging.getLogger("aster_monitor")


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    # 抑制过吵的库日志
    logging.getLogger("websockets.client").setLevel(logging.WARNING)
    logging.getLogger("websockets.server").setLevel(logging.WARNING)


async def _symbol_refresher(
    session: aiohttp.ClientSession,
    cfg: Config,
    engine: SignalEngine,
    ws: WSManager,
    initial_symbols: list[str],
    stop_event: asyncio.Event,
) -> None:
    current = set(initial_symbols)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cfg.symbol_refresh_interval_sec)
            return
        except asyncio.TimeoutError:
            pass
        try:
            new_symbols = await fetch_intersection_symbols(
                session,
                cfg.aster_rest_base,
                cfg.binance_rest_base,
            )
        except Exception as exc:
            logger.warning("Symbol 刷新失败，沿用旧列表: %s", exc)
            continue
        new_set = set(new_symbols)
        if new_set == current:
            logger.debug("Symbol 列表未变化")
            continue
        added = new_set - current
        removed = current - new_set
        logger.info("Symbol 变更：新增 %s 移除 %s", sorted(added), sorted(removed))
        engine.ensure_symbols(new_symbols)
        await ws.start(new_symbols)
        current = new_set


async def _run() -> int:
    cfg = load_config()
    _setup_logging(cfg.log_level)

    logger.info(
        "启动 Aster Mid-Price 监控 | window=%ds tolerance=%.2f hit=%.2f oi_inc=%.2f%% poll=%ds tg=%s",
        cfg.signal_window_sec,
        cfg.midprice_tolerance,
        cfg.midprice_hit_ratio,
        cfg.oi_increase_ratio * 100,
        cfg.oi_poll_interval_sec,
        cfg.telegram_enabled,
    )

    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # 1) 初始 symbol 列表
        try:
            symbols = await fetch_intersection_symbols(
                session,
                cfg.aster_rest_base,
                cfg.binance_rest_base,
            )
        except Exception as exc:
            logger.error("初始 symbol 拉取失败：%s", exc)
            return 1
        if not symbols:
            logger.error("Binance 与 Aster 交集为空，退出")
            return 1

        # 2) 装配组件
        alert_sink = AlertSink(
            session=session,
            telegram_bot_token=cfg.telegram_bot_token,
            telegram_chat_id=cfg.telegram_chat_id,
        )
        engine = SignalEngine(cfg, alert_sink)
        engine.ensure_symbols(symbols)

        ws_mgr = WSManager(cfg.aster_ws_base, cfg.ws_batch_size, engine)
        oi_poller = OIPoller(session, cfg.aster_rest_base, engine, cfg.oi_poll_interval_sec)

        # 3) OI 端点探测
        try:
            await oi_poller.detect_endpoint(symbols[0])
        except OIEndpointError as exc:
            logger.error(str(exc))
            return 1

        # 4) 启动各组件
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass  # Windows

        await ws_mgr.start(symbols)

        oi_task = asyncio.create_task(oi_poller.run(), name="oi-poller")
        refresher_task = asyncio.create_task(
            _symbol_refresher(session, cfg, engine, ws_mgr, symbols, stop_event),
            name="symbol-refresher",
        )

        try:
            await stop_event.wait()
        finally:
            logger.info("收到停止信号，关闭中...")
            for t in (oi_task, refresher_task):
                t.cancel()
            await asyncio.gather(oi_task, refresher_task, return_exceptions=True)
            await ws_mgr.stop()

    logger.info("已退出")
    return 0


def main() -> None:
    try:
        rc = asyncio.run(_run())
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()
