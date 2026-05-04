"""模块 5：告警输出（console + Telegram）。"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class SignalPayload:
    symbol: str
    hit_ratio: float          # 0..1
    oi_change_ratio: float    # 0..1, 可能为负
    oi_start: float
    oi_end: float
    spread: float
    mid_price: float
    last_price: float
    trade_count: int
    timestamp: float          # epoch seconds


def _format_message(p: SignalPayload) -> str:
    ts = _dt.datetime.fromtimestamp(p.timestamp, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return (
        f"🚨 ASTER SIGNAL: {p.symbol}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Mid-price hit ratio: {p.hit_ratio * 100:.1f}%\n"
        f"OI change: {p.oi_change_ratio * 100:+.2f}%\n"
        f"OI: {p.oi_start:.4f} → {p.oi_end:.4f}\n"
        f"Current spread: {p.spread:.8f}\n"
        f"Mid-price: {p.mid_price:.8f}\n"
        f"Last price: {p.last_price:.8f}\n"
        f"Window trades: {p.trade_count}\n"
        f"Time: {ts}\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )


class AlertSink:
    """统一的告警出口。"""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        telegram_bot_token: str = "",
        telegram_chat_id: str = "",
    ) -> None:
        self._session = session
        self._tg_token = telegram_bot_token
        self._tg_chat = telegram_chat_id

    @property
    def telegram_enabled(self) -> bool:
        return bool(self._tg_token) and bool(self._tg_chat)

    async def emit(self, payload: SignalPayload) -> None:
        text = _format_message(payload)
        logger.warning("[SIGNAL] %s", text.replace("\n", " | "))
        print(text, flush=True)

        if self.telegram_enabled:
            await self._send_telegram(text)

    async def _send_telegram(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
        data = {
            "chat_id": self._tg_chat,
            "text": text,
            "disable_web_page_preview": True,
        }
        try:
            async with self._session.post(
                url,
                json=data,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("Telegram 推送失败 status=%s body=%s", resp.status, body[:500])
        except Exception as exc:
            logger.exception("Telegram 推送异常: %s", exc)
