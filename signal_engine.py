"""模块 4：信号检测引擎（滑动窗口）。"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from alert import AlertSink, SignalPayload
from config import Config

logger = logging.getLogger(__name__)


@dataclass
class TickerState:
    symbol: str
    best_bid: float = 0.0
    best_ask: float = 0.0
    last_price: float = 0.0
    # (timestamp, is_near_mid)
    trade_hits: deque = field(default_factory=deque)
    # (timestamp, oi_value)
    oi_history: deque = field(default_factory=deque)
    last_alert_ts: float = 0.0


class SignalEngine:
    """所有 ticker 的状态聚合 + 检测逻辑。"""

    def __init__(self, cfg: Config, alert_sink: AlertSink) -> None:
        self._cfg = cfg
        self._alert = alert_sink
        self._states: dict[str, TickerState] = {}
        self._lock = asyncio.Lock()

    # ---------- 状态维护 ----------

    def ensure_symbols(self, symbols: list[str]) -> None:
        """启动 / 热更新时调用。新 symbol 会建 state，被移除的暂时保留（避免抖动丢数据）。"""
        for s in symbols:
            if s not in self._states:
                self._states[s] = TickerState(symbol=s)

    def get_state(self, symbol: str) -> Optional[TickerState]:
        return self._states.get(symbol)

    def all_symbols(self) -> list[str]:
        return list(self._states.keys())

    # ---------- 输入：行情 ----------

    def on_book_ticker(self, symbol: str, bid: float, ask: float) -> None:
        st = self._states.get(symbol)
        if st is None:
            return
        st.best_bid = bid
        st.best_ask = ask

    async def on_agg_trade(self, symbol: str, price: float, ts: float) -> None:
        st = self._states.get(symbol)
        if st is None:
            return
        st.last_price = price

        bid = st.best_bid
        ask = st.best_ask
        if bid <= 0 or ask <= 0 or ask < bid:
            # 盘口未就绪，跳过此笔的判定（不计入分母）
            return

        spread = ask - bid
        mid = (bid + ask) / 2.0
        if spread <= 0:
            is_near_mid = price == mid
        else:
            is_near_mid = abs(price - mid) <= spread * self._cfg.midprice_tolerance

        st.trade_hits.append((ts, is_near_mid))
        await self._maybe_alert(st, ts)

    # ---------- 输入：OI ----------

    def on_oi_sample(self, symbol: str, oi_value: float, ts: float) -> None:
        st = self._states.get(symbol)
        if st is None:
            return
        st.oi_history.append((ts, oi_value))

    # ---------- 检测 ----------

    async def _maybe_alert(self, st: TickerState, now_ts: float) -> None:
        cfg = self._cfg
        window = cfg.signal_window_sec

        # 修剪 trade_hits
        cutoff = now_ts - window
        while st.trade_hits and st.trade_hits[0][0] < cutoff:
            st.trade_hits.popleft()

        # 修剪 oi_history（只删落在窗口外的）
        while st.oi_history and st.oi_history[0][0] < cutoff:
            st.oi_history.popleft()

        total = len(st.trade_hits)
        if total < cfg.min_trades_in_window:
            return
        if len(st.oi_history) < 2:
            return

        near = sum(1 for _, hit in st.trade_hits if hit)
        hit_ratio = near / total

        oi_start_ts, oi_start = st.oi_history[0]
        oi_end_ts, oi_end = st.oi_history[-1]
        if oi_start <= 0:
            return
        oi_change_ratio = (oi_end - oi_start) / oi_start

        if hit_ratio < cfg.midprice_hit_ratio:
            return
        if oi_change_ratio < cfg.oi_increase_ratio:
            return

        # 冷却
        if now_ts - st.last_alert_ts < cfg.alert_cooldown_sec:
            return
        st.last_alert_ts = now_ts

        bid = st.best_bid
        ask = st.best_ask
        spread = max(ask - bid, 0.0)
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0

        payload = SignalPayload(
            symbol=st.symbol,
            hit_ratio=hit_ratio,
            oi_change_ratio=oi_change_ratio,
            oi_start=oi_start,
            oi_end=oi_end,
            spread=spread,
            mid_price=mid,
            last_price=st.last_price,
            trade_count=total,
            timestamp=now_ts,
        )
        logger.info(
            "信号触发 %s: hit_ratio=%.3f oi_change=%.4f trades=%d oi_window=%.1fs",
            st.symbol,
            hit_ratio,
            oi_change_ratio,
            total,
            oi_end_ts - oi_start_ts,
        )
        await self._alert.emit(payload)


def now_ts() -> float:
    return time.time()
