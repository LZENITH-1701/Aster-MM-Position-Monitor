"""环境变量与默认值集中管理。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get_str(key: str, default: str) -> str:
    val = os.environ.get(key)
    return val if val is not None and val != "" else default


def _get_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"环境变量 {key} 不是合法的 float: {raw!r}")


def _get_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"环境变量 {key} 不是合法的 int: {raw!r}")


@dataclass(frozen=True)
class Config:
    aster_rest_base: str
    aster_ws_base: str
    binance_rest_base: str

    midprice_tolerance: float
    signal_window_sec: int
    midprice_hit_ratio: float
    oi_increase_ratio: float
    oi_poll_interval_sec: int
    min_trades_in_window: int
    alert_cooldown_sec: int
    symbol_refresh_interval_sec: int
    ws_batch_size: int

    telegram_bot_token: str
    telegram_chat_id: str

    log_level: str

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token) and bool(self.telegram_chat_id)


def load_config() -> Config:
    return Config(
        aster_rest_base=_get_str("ASTER_REST_BASE", "https://fapi.asterdex.com"),
        aster_ws_base=_get_str("ASTER_WS_BASE", "wss://fstream.asterdex.com"),
        binance_rest_base=_get_str("BINANCE_REST_BASE", "https://fapi.binance.com"),
        midprice_tolerance=_get_float("MIDPRICE_TOLERANCE", 0.35),
        signal_window_sec=_get_int("SIGNAL_WINDOW_SEC", 1200),
        midprice_hit_ratio=_get_float("MIDPRICE_HIT_RATIO", 0.75),
        oi_increase_ratio=_get_float("OI_INCREASE_RATIO", 0.02),
        oi_poll_interval_sec=_get_int("OI_POLL_INTERVAL_SEC", 30),
        min_trades_in_window=_get_int("MIN_TRADES_IN_WINDOW", 30),
        alert_cooldown_sec=_get_int("ALERT_COOLDOWN_SEC", 600),
        symbol_refresh_interval_sec=_get_int("SYMBOL_REFRESH_INTERVAL_SEC", 3600),
        ws_batch_size=_get_int("WS_BATCH_SIZE", 80),
        telegram_bot_token=_get_str("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=_get_str("TELEGRAM_CHAT_ID", ""),
        log_level=_get_str("LOG_LEVEL", "INFO").upper(),
    )
