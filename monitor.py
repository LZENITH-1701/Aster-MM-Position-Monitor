"""主入口：装配并运行整个监控。"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from collections import deque

# 本机若装了 truststore（pip install truststore），自动启用以读取系统钥匙串里的根证书。
# 处理 macOS 在企业代理 / GFW 解密代理下 Python 的 SSL 证书链问题。Heroku 上没装也不影响。
try:
    import truststore as _truststore
    _truststore.inject_into_ssl()
except ImportError:
    pass

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


async def _stats_heartbeat(
    cfg: Config,
    engine: SignalEngine,
    stop_event: asyncio.Event,
) -> None:
    """周期性扫描 + 上榜频次跟踪。

    每次扫描记录「本轮上榜的 ticker」（hit↑ top5 ∪ oi↑ top5）。
    维护过去 SIGNAL_WINDOW_SEC 内的上榜时间戳，按 ≥10/≥20/≥30 次分档输出。
    详细 top 表节流到 ~2 分钟一次，避免刷屏。
    """
    if cfg.stats_interval_sec <= 0:
        return

    interval = cfg.stats_interval_sec
    window = cfg.signal_window_sec
    top_n = 5
    detail_interval = max(interval * 4, 120)  # 详细 top 表的最小间隔

    sweep_count = 0
    appearances: dict[str, deque[float]] = {}
    last_detail_ts = 0.0

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            pass

        now = time.time()
        sweep_count += 1
        rows = engine.snapshot()
        total_trades = sum(r.trade_count for r in rows)
        with_trades = sum(1 for r in rows if r.trade_count > 0)
        eligible = [r for r in rows if r.trade_count >= cfg.min_trades_in_window and r.has_oi]
        ready = [
            r for r in eligible
            if r.hit_ratio >= cfg.midprice_hit_ratio and r.oi_change_ratio >= cfg.oi_increase_ratio
        ]

        top_hit = sorted(eligible, key=lambda r: r.hit_ratio, reverse=True)[:top_n]
        top_oi = sorted(eligible, key=lambda r: r.oi_change_ratio, reverse=True)[:top_n]
        on_top = {r.symbol for r in top_hit} | {r.symbol for r in top_oi}

        # 记录本轮上榜
        for sym in on_top:
            appearances.setdefault(sym, deque()).append(now)
        # 裁掉超出窗口的
        cutoff = now - window
        for sym in list(appearances.keys()):
            dq = appearances[sym]
            while dq and dq[0] < cutoff:
                dq.popleft()
            if not dq:
                del appearances[sym]

        # 总览（每轮都打）
        logger.info(
            "[STATS] sweep#%d symbols=%d 有成交=%d eligible=%d 同时满足=%d 总成交=%d",
            sweep_count, len(rows), with_trades, len(eligible), len(ready), total_trades,
        )

        # 详细 top 表（节流）
        if now - last_detail_ts >= detail_interval and eligible:
            last_detail_ts = now
            for r in top_hit:
                logger.info(
                    "  hit↑  %-14s hit=%.2f oi_chg=%+.3f%% trades=%d",
                    r.symbol, r.hit_ratio, r.oi_change_ratio * 100, r.trade_count,
                )
            for r in top_oi:
                logger.info(
                    "  oi↑   %-14s oi_chg=%+.3f%% hit=%.2f trades=%d",
                    r.symbol, r.oi_change_ratio * 100, r.hit_ratio, r.trade_count,
                )

        # 频次报告
        b30: list[tuple[str, int]] = []
        b20: list[tuple[str, int]] = []
        b10: list[tuple[str, int]] = []
        for sym, dq in appearances.items():
            c = len(dq)
            if c >= 30:
                b30.append((sym, c))
            elif c >= 20:
                b20.append((sym, c))
            elif c >= 10:
                b10.append((sym, c))
        b30.sort(key=lambda x: -x[1])
        b20.sort(key=lambda x: -x[1])
        b10.sort(key=lambda x: -x[1])

        if b30 or b20 or b10:
            max_possible = min(sweep_count, window // interval)
            logger.info(
                "[FREQ] 过去 %d 分钟上榜频次（已扫 %d 次，窗口最多 %d 次）",
                window // 60, sweep_count, max_possible,
            )
            if b30:
                items = ", ".join(f"{s}({c})" for s, c in b30)
                logger.warning("  🔥 ≥30 次: %s", items)
            if b20:
                items = ", ".join(f"{s}({c})" for s, c in b20)
                logger.info("  ⭐ ≥20 次: %s", items)
            if b10:
                items = ", ".join(f"{s}({c})" for s, c in b10)
                logger.info("  ·  ≥10 次: %s", items)


async def _symbol_refresher(
    session: aiohttp.ClientSession,
    cfg: Config,
    engine: SignalEngine,
    ws: WSManager,
    initial_symbols: list[str],
    stop_event: asyncio.Event,
) -> None:
    if cfg.symbol_refresh_interval_sec <= 0:
        logger.info("Symbol 刷新已关闭，使用启动时锁定的 %d 个 symbol", len(initial_symbols))
        return
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
        oi_poller = OIPoller(
            session,
            cfg.aster_rest_base,
            engine,
            cfg.oi_poll_interval_sec,
            concurrency=cfg.oi_concurrency,
        )

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
        stats_task = asyncio.create_task(
            _stats_heartbeat(cfg, engine, stop_event),
            name="stats-heartbeat",
        )

        try:
            await stop_event.wait()
        finally:
            logger.info("收到停止信号，关闭中...")
            for t in (oi_task, refresher_task, stats_task):
                t.cancel()
            await asyncio.gather(oi_task, refresher_task, stats_task, return_exceptions=True)
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
