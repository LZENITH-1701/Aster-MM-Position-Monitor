"""模块 1：Binance Perp ∩ Aster Perp 的 USDT 永续合约 symbol 交集。"""
from __future__ import annotations

import logging
from typing import Iterable

import aiohttp

logger = logging.getLogger(__name__)

EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
HTTP_TIMEOUT_SEC = 15


async def _fetch_exchange_info(session: aiohttp.ClientSession, base_url: str) -> dict:
    url = f"{base_url.rstrip('/')}{EXCHANGE_INFO_PATH}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SEC)) as resp:
        resp.raise_for_status()
        return await resp.json()


def _extract_perpetual_usdt_bases(payload: dict) -> set[str]:
    bases: set[str] = set()
    for sym in payload.get("symbols", []):
        if sym.get("contractType") != "PERPETUAL":
            continue
        if sym.get("quoteAsset") != "USDT":
            continue
        if sym.get("status") != "TRADING":
            continue
        base = sym.get("baseAsset")
        if base:
            bases.add(base)
    return bases


def _to_symbols(bases: Iterable[str]) -> list[str]:
    return sorted(f"{b}USDT" for b in bases)


async def fetch_intersection_symbols(
    session: aiohttp.ClientSession,
    aster_rest_base: str,
    binance_rest_base: str,
) -> list[str]:
    """返回两边都在 TRADING 的 USDT 永续 baseAsset 拼成的 symbol 列表。"""
    binance_info = await _fetch_exchange_info(session, binance_rest_base)
    aster_info = await _fetch_exchange_info(session, aster_rest_base)

    binance_bases = _extract_perpetual_usdt_bases(binance_info)
    aster_bases = _extract_perpetual_usdt_bases(aster_info)

    common = binance_bases & aster_bases
    only_binance = binance_bases - aster_bases
    only_aster = aster_bases - binance_bases

    symbols = _to_symbols(common)

    logger.info(
        "Symbol 交集: %d 个 (Binance=%d, Aster=%d, 仅 Binance=%d, 仅 Aster=%d)",
        len(symbols),
        len(binance_bases),
        len(aster_bases),
        len(only_binance),
        len(only_aster),
    )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("交集 symbols: %s", symbols)
        if only_aster:
            logger.debug("仅 Aster 上线: %s", sorted(only_aster))

    return symbols
