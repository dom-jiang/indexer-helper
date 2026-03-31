"""
Panora DEX Aggregator for Aptos.

Single endpoint returns quote + transaction data:
  POST https://api.panora.exchange/swap

Docs: https://docs.panora.exchange/developer/swap/api
"""

import requests
from typing import Dict, Optional
from loguru import logger
from config import Cfg

PANORA_BASE_URL = "https://api.panora.exchange"

_session = requests.Session()

try:
    from db_info import PANORA_API_KEY
except ImportError:
    PANORA_API_KEY = ""


def _get_api_key() -> str:
    return PANORA_API_KEY or getattr(Cfg, "PANORA_API_KEY", "")


def panora_swap(
    from_token: str,
    to_token: str,
    from_amount: str,
    to_wallet: str,
    slippage: float = None,
    integrator_fee: float = None,
) -> Dict:
    """
    Get quote + transaction data from Panora.

    Args:
        from_token:  Aptos token address (e.g. "0xa" for APT)
        to_token:    Aptos token address (full Move type path)
        from_amount: Human-readable amount (NOT smallest unit)
        to_wallet:   Recipient Aptos wallet address
        slippage:    Percentage (e.g. 0.5 for 0.5%), omit for auto
        integrator_fee: 0-2 percentage
    Returns:
        {"success": True/False, "router": "panora", "data": {...}, "error": "..."}
    """
    try:
        params = {
            "fromTokenAddress": from_token,
            "toTokenAddress": to_token,
            "fromTokenAmount": str(from_amount),
            "toWalletAddress": to_wallet,
        }
        if slippage is not None:
            params["slippagePercentage"] = str(slippage)
        if integrator_fee is not None:
            params["integratorFeePercentage"] = str(integrator_fee)

        headers = {"Content-Type": "application/json"}
        key = _get_api_key()
        if key:
            headers["x-api-key"] = key

        resp = _session.post(
            f"{PANORA_BASE_URL}/swap",
            params=params,
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"success": True, "router": "panora", "data": data}
    except Exception as e:
        logger.error(f"panora_swap error: {e}")
        return {"success": False, "router": "panora", "error": str(e)}


def panora_supported_tokens() -> Dict:
    """Get list of supported tokens on Panora."""
    try:
        headers = {}
        key = _get_api_key()
        if key:
            headers["x-api-key"] = key

        resp = _session.get(
            f"{PANORA_BASE_URL}/tokenlist",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        return {"success": True, "data": resp.json()}
    except Exception as e:
        logger.error(f"panora_supported_tokens error: {e}")
        return {"success": False, "error": str(e)}
