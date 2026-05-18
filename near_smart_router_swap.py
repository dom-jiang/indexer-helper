# -*- coding: utf-8 -*-
"""
NEAR same-chain swaps via Ref SmartRouter GET /findPath + Ref v2 `ft_transfer_call`.

Mirrors frontend: `centralized_api.findPath` → `getSwapActionsList`
→ `swap_tx_query` (single `ft_transfer_call` on tokenIn to REF exchange).
"""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger

ROUTER_NEAR_REF_SMARTROUTER = "near-ref-smart"

_NEAR_GAS_FT_TRANSFER_CALL = "100000000000000"  # 100 Tgas
_NEAR_GAS_NEAR_DEPOSIT = "30000000000000"  # 30 Tgas
_NEAR_ATTACHED_YOCTO = "1"
WRAP_NEAR_CONTRACT = "wrap.near"


def _cfg():
    try:
        from config import Cfg as _Cfg  # pragma: no cover

        return _Cfg
    except Exception:  # pragma: no cover
        return None


def _findpath_endpoint() -> str:
    cfg = _cfg()
    url = getattr(cfg, "REF_SDK_URL", None) if cfg else None
    url = (url or "https://smartrouter.ref.finance/findPath").strip().rstrip("/")
    # db_info historically sets full path ending with /findPath; accept base-only too.
    if not url.endswith("/findPath"):
        url = f"{url}/findPath"
    return url


def _ref_exchange_receiver_id() -> str:
    cfg = _cfg()
    if cfg and getattr(cfg, "NETWORK", None) and getattr(cfg, "NETWORK_ID", None):
        nw = cfg.NETWORK.get(cfg.NETWORK_ID) or cfg.NETWORK.get("MAINNET") or {}
        rid = nw.get("REF_CONTRACT")
        if isinstance(rid, str) and rid.strip():
            return rid.strip()
    return "v2.ref-finance.near"


def _native_wrap_input(addr_raw: Optional[str]) -> bool:
    a = (addr_raw or "").strip().lower()
    return a in ("", "near", "wnear")


def _canonical_token_contract(addr_raw: Optional[str]) -> str:
    """Map native markers to wrap.near for SmartRouter; keep other NEP-141 ids."""
    return WRAP_NEAR_CONTRACT if _native_wrap_input(addr_raw) else (addr_raw or "").strip()


def _requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


_session = _requests_session()


def near_find_path_get(
    amount_in: str,
    token_in: str,
    token_out: str,
    slippage_decimal: float,
    *,
    path_deep: int = 3,
    timeout_seconds: float = 8,
) -> Dict[str, Any]:
    endpoint = _findpath_endpoint()
    tip = Decimal(str(amount_in))
    if tip <= 0:
        return {
            "result_code": 1007,
            "result_message": "invalid amount_in",
            "result_data": None,
        }

    qs = {
        "amountIn": str(int(tip)),
        "tokenIn": token_in,
        "tokenOut": token_out,
        "pathDeep": str(int(path_deep)),
        "slippage": str(float(slippage_decimal)),
    }
    try:
        r = _session.get(endpoint, params=qs, timeout=timeout_seconds)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"near_find_path_get failed: {e}")
        return {
            "result_code": 1007,
            "result_message": str(e),
            "result_data": None,
        }


def _swap_actions_and_raw_amount_out(
    result_data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], str]:
    """Flatten routes.pools → actions (frontend getSwapActionsList)."""
    actions: List[Dict[str, Any]] = []
    routes = result_data.get("routes") or []
    for route in routes:
        for pool in route.get("pools") or []:
            if not isinstance(pool, dict):
                continue
            p = dict(pool)
            try:
                if float(p.get("amount_in") or 0) == 0:
                    p.pop("amount_in", None)
            except (TypeError, ValueError):
                pass
            try:
                p["pool_id"] = int(p["pool_id"])
            except Exception:
                pass
            actions.append(p)
    raw_out = result_data.get("amount_out") or "0"
    return actions, str(raw_out)


def _min_out_after_slip(amount_out_raw: str, slip_dec: float) -> str:
    try:
        raw = Decimal(str(amount_out_raw))
        slip = Decimal(str(slip_dec))
        scaled = raw * (Decimal(1) - slip)
        floored = scaled.quantize(Decimal(1), rounding=ROUND_DOWN)
        out = str(int(max(floored, Decimal(0))))
        return out
    except (InvalidOperation, ValueError):
        return "0"


def _shrink_token(amount: str, decimals: int) -> str:
    try:
        d = Decimal(str(amount)) / Decimal(10 ** int(decimals))
        result = format(d, "f")
        if "." in result:
            result = result.rstrip("0").rstrip(".")
        return result if result else "0"
    except (InvalidOperation, ValueError):
        return "0"


def near_same_chain_quote(
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Unified quote blob compatible with aggregate_quote / `_same_chain_quote`.
    """
    if not recipient:
        recipient = sender

    tin = _canonical_token_contract(token_in.get("address"))
    tout = _canonical_token_contract(token_out.get("address"))
    if not tin or not tout:
        return {"success": False, "chainType": "near", "error": "Missing token contract id for NEAR swap"}
    if tin == tout:
        return {"success": False, "chainType": "near", "error": "tokenIn and tokenOut must differ"}

    res = near_find_path_get(amount_in, tin, tout, float(slippage_decimal))
    if res.get("result_code") != 0:
        return {
            "success": False,
            "chainType": "near",
            "error": res.get("result_message") or f"SmartRouter error code {res.get('result_code')}",
        }
    rd = res.get("result_data") or {}
    routes = rd.get("routes") or []
    if not routes:
        return {"success": False, "chainType": "near", "error": "No path available to make a swap"}

    swap_actions, raw_out = _swap_actions_and_raw_amount_out(rd)
    if not swap_actions:
        return {"success": False, "chainType": "near", "error": "SmartRouter returned empty route actions"}

    min_out = _min_out_after_slip(raw_out, float(slippage_decimal))
    if int(Decimal(min_out or "0")) <= 0:
        return {"success": False, "chainType": "near", "error": "Quoted output amount too small"}

    decimals_out = int(token_out.get("decimals") or 24)

    quote_core = {
        "router": ROUTER_NEAR_REF_SMARTROUTER,
        "amountOut": str(int(Decimal(str(raw_out)))),
        "amountOutReadable": _shrink_token(str(int(Decimal(str(raw_out)))), decimals_out),
        "minAmountOut": min_out,
        "chainId": "near",
        "tokenIn": token_in,
        "tokenOut": token_out,
        "amountIn": str(amount_in),
        "slippage": float(slippage_decimal),
        "sender": sender,
        "recipient": recipient,
        "isBluechipIn": False,
        "isBluechipOut": False,
        "_amountOutDecimal": Decimal(str(raw_out)),
    }

    summary = {
        k: v
        for k, v in quote_core.items()
        if not k.startswith("_")
    }

    qc = dict(quote_core)
    qc.pop("_amountOutDecimal", None)

    return {
        "success": True,
        "chainType": "near",
        "quote": qc,
        "allQuotes": [summary],
        "errors": None,
    }


def _build_near_ref_signed_tx_skeleton(
    *,
    swap_out_recipient: str,
    token_in_original_address: str,
    amount_smallest: str,
    swap_actions_list: List[Dict[str, Any]],
) -> Tuple[str, str, List[Dict[str, Any]], str]:
    """
    ``receiver_contract_id`` — first Interaction contract (wallet shows this).
    ``standard``: native | nep141 — hints for frontend.
    """
    ref_ex = _ref_exchange_receiver_id()
    msg_payload = {
        "swap_out_recipient": swap_out_recipient,
        "force": 0,
        "actions": swap_actions_list,
        "skip_unwrap_near": True,
        "skip_degen_price_sync": True,
    }
    msg_s = json.dumps(msg_payload, separators=(",", ":"))
    fc = {
        "type": "FunctionCall",
        "params": {
            "methodName": "ft_transfer_call",
            "args": {
                "receiver_id": ref_ex,
                "amount": str(amount_smallest),
                "msg": msg_s,
            },
            "gas": _NEAR_GAS_FT_TRANSFER_CALL,
            "deposit": _NEAR_ATTACHED_YOCTO,
        },
    }

    if _native_wrap_input(token_in_original_address):
        receiver = WRAP_NEAR_CONTRACT
        nd = {
            "type": "FunctionCall",
            "params": {
                "methodName": "near_deposit",
                "args": {},
                "gas": _NEAR_GAS_NEAR_DEPOSIT,
                "deposit": str(amount_smallest),
            },
        }
        return receiver, "native", [nd, fc], WRAP_NEAR_CONTRACT

    addr = (token_in_original_address or "").strip()
    return addr, "nep141", [fc], addr


def near_same_chain_build_tx(
    router: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: Optional[str],
) -> Dict[str, Any]:
    if (router or "").strip() != ROUTER_NEAR_REF_SMARTROUTER:
        return {
            "success": False,
            "chainType": "near",
            "error": f"Unsupported NEAR same-chain router '{router}'. Use '{ROUTER_NEAR_REF_SMARTROUTER}'.",
        }

    if not recipient:
        recipient = sender

    tin = _canonical_token_contract(token_in.get("address"))
    tout = _canonical_token_contract(token_out.get("address"))
    res = near_find_path_get(amount_in, tin, tout, float(slippage_decimal))
    if res.get("result_code") != 0:
        return {
            "success": False,
            "chainType": "near",
            "error": res.get("result_message") or "SmartRouter quote failed during build",
        }
    rd = res.get("result_data") or {}
    routes = rd.get("routes") or []
    swap_actions, raw_out = _swap_actions_and_raw_amount_out(rd)
    if not routes or not swap_actions:
        return {"success": False, "chainType": "near", "error": "No path available to make a swap"}

    min_out = _min_out_after_slip(raw_out, float(slippage_decimal))
    if int(Decimal(min_out or "0")) <= 0:
        return {"success": False, "chainType": "near", "error": "Quoted output amount too small"}

    receiver_contract, standard, actions, token_addr_for_meta = _build_near_ref_signed_tx_skeleton(
        swap_out_recipient=recipient.strip(),
        token_in_original_address=token_in.get("address", ""),
        amount_smallest=str(amount_in),
        swap_actions_list=swap_actions,
    )

    tx = {
        "chainId": "near",
        "signerId": sender.strip(),
        "receiverId": receiver_contract,
        "standard": standard,
        "swapKind": ROUTER_NEAR_REF_SMARTROUTER,
        "tokenAddress": token_addr_for_meta,
        "recipient": recipient.strip(),
        "amount": str(amount_in),
        "actions": actions,
        "nearRefSmartRouterHints": {"tokenInRoute": tin, "tokenOutRoute": tout},
    }

    return {
        "success": True,
        "chainType": "near",
        "router": ROUTER_NEAR_REF_SMARTROUTER,
        "tx": tx,
        "estimatedOut": str(int(Decimal(str(raw_out)))),
        "minAmountOut": min_out,
    }
