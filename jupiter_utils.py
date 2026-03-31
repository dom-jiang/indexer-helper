"""
Jupiter V2 Swap API integration for Solana.

Endpoints:
  GET /swap/v2/order  - Get quote + assembled transaction
  GET /swap/v2/build  - Get raw swap instructions (Metis only)

Docs: https://docs.jup.ag/docs/swap-api
"""

import requests
from typing import Dict, Optional
from loguru import logger
from config import Cfg

JUPITER_BASE_URL = "https://api.jup.ag"

_session = requests.Session()

try:
    from db_info import JUPITER_API_KEY
except ImportError:
    JUPITER_API_KEY = ""


def _get_headers() -> dict:
    headers = {"Content-Type": "application/json"}
    key = JUPITER_API_KEY or getattr(Cfg, "JUPITER_API_KEY", "")
    if key:
        headers["x-api-key"] = key
    return headers


def jupiter_order(
    input_mint: str,
    output_mint: str,
    amount: str,
    slippage_bps: int,
    taker: str,
    swap_mode: str = "ExactIn",
) -> Dict:
    """
    Get quote + pre-built transaction from Jupiter.

    Args:
        input_mint:  Solana token mint address
        output_mint: Solana token mint address
        amount:      Smallest unit (lamports for SOL)
        slippage_bps: Slippage in basis points (e.g. 50 = 0.5%)
        taker:       User wallet public key
        swap_mode:   ExactIn or ExactOut
    Returns:
        {"success": True/False, "data": {...}, "error": "..."}
    """
    try:
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(slippage_bps),
            "taker": taker,
            "swapMode": swap_mode,
        }
        resp = _session.get(
            f"{JUPITER_BASE_URL}/swap/v2/order",
            params=params,
            headers=_get_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"success": True, "router": "jupiter", "data": data}
    except Exception as e:
        logger.error(f"jupiter_order error: {e}")
        return {"success": False, "router": "jupiter", "error": str(e)}


def jupiter_build(
    input_mint: str,
    output_mint: str,
    amount: str,
    slippage_bps: int,
    taker: str,
) -> Dict:
    """
    Get raw swap instructions from Jupiter (Metis router only, no platform fees).

    Returns instructions the client assembles into a transaction.
    """
    try:
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(slippage_bps),
            "taker": taker,
        }
        resp = _session.get(
            f"{JUPITER_BASE_URL}/swap/v2/build",
            params=params,
            headers=_get_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"success": True, "router": "jupiter", "data": data}
    except Exception as e:
        logger.error(f"jupiter_build error: {e}")
        return {"success": False, "router": "jupiter", "error": str(e)}


def jupiter_price(input_mint: str, output_mint: str = None) -> Dict:
    """Get token price from Jupiter Price API V3."""
    try:
        params = {"ids": input_mint}
        if output_mint:
            params["vsToken"] = output_mint
        resp = _session.get(
            f"{JUPITER_BASE_URL}/price/v3",
            params=params,
            headers=_get_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        return {"success": True, "data": resp.json()}
    except Exception as e:
        logger.error(f"jupiter_price error: {e}")
        return {"success": False, "error": str(e)}
