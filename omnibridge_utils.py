"""
OmniBridge cross-chain DEX integration.

Strategy:
  - /api/swap/quote with cross-chain pairs: calls getBaseInfo for exchange rate estimates
  - /api/swap/build-tx with cross-chain pairs: calls accountExchange to create an order,
    returns deposit info for the user to send funds to

Docs: Based on OmniBridge partner API documentation.
"""

import requests
from typing import Dict, Optional
from loguru import logger
from config import Cfg

OMNI_BASE_URL = "https://www.omnibtc.finance"

_session = requests.Session()

try:
    from db_info import OMNI_SOURCE_FLAG
except ImportError:
    OMNI_SOURCE_FLAG = ""


def _get_source_flag() -> str:
    return OMNI_SOURCE_FLAG or getattr(Cfg, "OMNI_SOURCE_FLAG", "")


def omni_get_base_info(
    from_chain: str,
    to_chain: str,
    from_token: str,
    to_token: str,
    amount: str,
) -> Dict:
    """
    Get cross-chain exchange rate and fee estimate from OmniBridge.

    Args:
        from_chain: source chain identifier (e.g. "ethereum", "bsc", "solana")
        to_chain:   destination chain identifier
        from_token: source token symbol or address
        to_token:   destination token symbol or address
        amount:     human-readable amount
    Returns:
        {"success": True/False, "data": {...}, "error": "..."}
    """
    try:
        params = {
            "fromChain": from_chain,
            "toChain": to_chain,
            "fromToken": from_token,
            "toToken": to_token,
            "amount": str(amount),
        }
        source_flag = _get_source_flag()
        if source_flag:
            params["sourceFlag"] = source_flag

        resp = _session.get(
            f"{OMNI_BASE_URL}/api/v1/getBaseInfo",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"success": True, "data": data}
    except Exception as e:
        logger.error(f"omni_get_base_info error: {e}")
        return {"success": False, "error": str(e)}


def omni_account_exchange(
    from_chain: str,
    to_chain: str,
    from_token: str,
    to_token: str,
    amount: str,
    to_address: str,
    slippage: float = 0.5,
) -> Dict:
    """
    Create a cross-chain exchange order on OmniBridge.

    Returns deposit address and amount that the user needs to send.
    """
    try:
        body = {
            "fromChain": from_chain,
            "toChain": to_chain,
            "fromToken": from_token,
            "toToken": to_token,
            "amount": str(amount),
            "toAddress": to_address,
            "slippage": str(slippage),
        }
        source_flag = _get_source_flag()
        if source_flag:
            body["sourceFlag"] = source_flag

        resp = _session.post(
            f"{OMNI_BASE_URL}/api/v1/accountExchange",
            json=body,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return {"success": True, "data": data}
    except Exception as e:
        logger.error(f"omni_account_exchange error: {e}")
        return {"success": False, "error": str(e)}


def omni_get_order_status(order_id: str) -> Dict:
    """Query the status of an OmniBridge cross-chain order."""
    try:
        resp = _session.get(
            f"{OMNI_BASE_URL}/api/v1/getOrderStatus",
            params={"orderId": order_id},
            timeout=10,
        )
        resp.raise_for_status()
        return {"success": True, "data": resp.json()}
    except Exception as e:
        logger.error(f"omni_get_order_status error: {e}")
        return {"success": False, "error": str(e)}


def omni_get_supported_chains() -> Dict:
    """Get list of chains supported by OmniBridge."""
    try:
        resp = _session.get(
            f"{OMNI_BASE_URL}/api/v1/getSupportedChains",
            timeout=10,
        )
        resp.raise_for_status()
        return {"success": True, "data": resp.json()}
    except Exception as e:
        logger.error(f"omni_get_supported_chains error: {e}")
        return {"success": False, "error": str(e)}


# ── Cross-chain quote / build helpers ────────────────────

def cross_chain_quote(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    sender: str,
    recipient: str = None,
) -> Dict:
    """
    Get a cross-chain swap quote.
    Uses OmniBridge getBaseInfo to estimate exchange rate.
    """
    from swap_utils import shrink_token

    token_in_decimals = token_in.get("decimals", 18)
    readable_amount = shrink_token(amount_in, token_in_decimals)

    result = omni_get_base_info(
        from_chain=from_chain,
        to_chain=to_chain,
        from_token=token_in.get("symbol", token_in.get("address", "")),
        to_token=token_out.get("symbol", token_out.get("address", "")),
        amount=readable_amount,
    )

    if not result.get("success"):
        return {"success": False, "error": result.get("error", "OmniBridge getBaseInfo failed")}

    data = result["data"]
    if isinstance(data, dict) and data.get("code") and str(data["code"]) != "0":
        return {"success": False, "error": data.get("msg", "OmniBridge error")}

    info = data.get("data", data)

    return {
        "success": True,
        "chainType": "cross-chain",
        "quote": {
            "router": "omnibridge",
            "fromChain": from_chain,
            "toChain": to_chain,
            "tokenIn": token_in,
            "tokenOut": token_out,
            "amountIn": amount_in,
            "estimatedOut": info.get("estimatedAmount", ""),
            "fee": info.get("fee", ""),
            "exchangeRate": info.get("exchangeRate", ""),
            "minReceived": info.get("minReceived", ""),
            "sender": sender,
            "recipient": recipient or sender,
        },
    }


def cross_chain_build_tx(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    sender: str,
    recipient: str,
    slippage: float = 0.5,
) -> Dict:
    """
    Create a cross-chain exchange order.
    Returns deposit address + amount for the user to execute.
    """
    from swap_utils import shrink_token, convert_slippage_to_decimal

    token_in_decimals = token_in.get("decimals", 18)
    readable_amount = shrink_token(amount_in, token_in_decimals)
    slippage_pct = convert_slippage_to_decimal(slippage) * 100

    result = omni_account_exchange(
        from_chain=from_chain,
        to_chain=to_chain,
        from_token=token_in.get("symbol", token_in.get("address", "")),
        to_token=token_out.get("symbol", token_out.get("address", "")),
        amount=readable_amount,
        to_address=recipient or sender,
        slippage=slippage_pct,
    )

    if not result.get("success"):
        return {"success": False, "error": result.get("error", "OmniBridge accountExchange failed")}

    data = result["data"]
    if isinstance(data, dict) and data.get("code") and str(data["code"]) != "0":
        return {"success": False, "error": data.get("msg", "OmniBridge order failed")}

    order_data = data.get("data", data)

    return {
        "success": True,
        "chainType": "cross-chain",
        "tx": {
            "depositAddress": order_data.get("depositAddress", ""),
            "depositAmount": order_data.get("depositAmount", readable_amount),
            "depositChain": from_chain,
            "orderId": order_data.get("orderId", ""),
        },
        "router": "omnibridge",
    }
