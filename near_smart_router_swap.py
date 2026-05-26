# -*- coding: utf-8 -*-
"""
NEAR same-chain swaps via Ref SmartRouter GET /findPath and SmartX swapMultiDexPath.

Mirrors frontend SDK 0.2.3: parallel v1 (findPath → Ref ft_transfer_call) and
v2 (SmartX → aggregatedex.near ft_transfer_call), pick best amountOut.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger

ROUTER_NEAR_REF_SMARTROUTER = "near-ref-smart"
ROUTER_NEAR_SMARTX = "near-smartx"
NEAR_TX_FORMAT_BATCH = "near_batch"

_NEAR_GAS_FT_TRANSFER_CALL = "100000000000000"  # 100 Tgas
_NEAR_GAS_SMARTX_FT_TRANSFER_CALL = "300000000000000"  # 300 Tgas
_NEAR_GAS_NEAR_DEPOSIT_SMARTX = "50000000000000"  # 50 Tgas — SDK AggregateDexRouter
_NEAR_GAS_NEAR_DEPOSIT = "30000000000000"  # 30 Tgas — Ref SmartRouter wrap
_NEAR_GAS_STORAGE_DEPOSIT = "50000000000000"  # 50 Tgas — SDK AggregateDexRouter
_NEAR_GAS_TOKENS_STORAGE_DEPOSIT = "30000000000000"  # 30 Tgas
_NEAR_ATTACHED_YOCTO = "1"
_NEAR_NEW_ACCOUNT_STORAGE_COST = "1250000000000000000000"  # 0.00125 NEAR
_NEAR_NATIVE_RESERVE_YOCTO = Decimal("50000000000000000000000")  # 0.05 NEAR
_NEAR_TOKENS_STORAGE_DEPOSIT_PER_TOKEN = str(
    int(Decimal("0.005") * Decimal(10 ** 24))
)  # 0.005 NEAR per token — aggregatedex.tokens_storage_deposit
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
    url = (url or "https://smartrouter.rhea.finance").strip().rstrip("/")
    if url.endswith("/findPath"):
        url = url[: -len("/findPath")]
    return f"{url.rstrip('/')}/findPath"


def _smartx_base_url() -> str:
    cfg = _cfg()
    url = getattr(cfg, "SMARTX_URL", None) if cfg else None
    return (url or "https://smartx.rhea.finance").strip().rstrip("/")


def _aggregate_dex_contract_id() -> str:
    cfg = _cfg()
    explicit = getattr(cfg, "NEAR_AGGREGATE_DEX_CONTRACT", None) if cfg else None
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    if cfg and getattr(cfg, "NETWORK", None) and getattr(cfg, "NETWORK_ID", None):
        nw = cfg.NETWORK.get(cfg.NETWORK_ID) or cfg.NETWORK.get("MAINNET") or {}
        aid = nw.get("AGGREGATE_DEX_CONTRACT")
        if isinstance(aid, str) and aid.strip():
            return aid.strip()
    return "aggregatedex.near"


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


def _ccb():
    """Lazy import to avoid circular dependency (swap_utils → near_smart_router_swap)."""
    import cross_chain_tx_builder as ccb  # noqa: WPS433

    return ccb


def _near_network_id() -> str:
    cfg = _cfg()
    if cfg and getattr(cfg, "NETWORK_ID", None):
        return str(cfg.NETWORK_ID)
    return "MAINNET"


def _fc_action(
    method_name: str,
    args: Dict[str, Any],
    gas: str,
    deposit: str = "0",
) -> Dict[str, Any]:
    return {
        "type": "FunctionCall",
        "params": {
            "methodName": method_name,
            "args": args,
            "gas": str(gas),
            "deposit": str(deposit),
        },
    }


def _storage_registered(
    network_id: str,
    contract_id: str,
    account_id: str,
) -> bool:
    if not contract_id or not account_id:
        return False
    try:
        bal = _ccb()._near_view_call(
            network_id,
            contract_id=contract_id,
            method_name="storage_balance_of",
            args={"account_id": account_id},
        )
        return bool(bal)
    except Exception as e:
        logger.warning(f"storage_balance_of {contract_id}@{account_id}: {e}")
        return False


def _ft_balance_of(network_id: str, contract_id: str, account_id: str) -> Decimal:
    if not contract_id or not account_id:
        return Decimal("0")
    try:
        bal = _ccb()._near_view_call(
            network_id,
            contract_id=contract_id,
            method_name="ft_balance_of",
            args={"account_id": account_id},
        )
        return Decimal(str(bal or "0"))
    except Exception as e:
        logger.warning(f"ft_balance_of {contract_id}@{account_id}: {e}")
        return Decimal("0")


def _native_near_available_balance(network_id: str, account_id: str) -> Decimal:
    if not account_id:
        return Decimal("0")
    body = {
        "jsonrpc": "2.0",
        "id": "near-smartx-native-bal",
        "method": "query",
        "params": {
            "request_type": "view_account",
            "account_id": account_id.strip(),
            "finality": "final",
        },
    }
    cfg = _cfg()
    urls: List[str] = []
    if cfg and getattr(cfg, "NETWORK", None):
        nw = cfg.NETWORK.get(network_id) or cfg.NETWORK.get("MAINNET") or {}
        raw = nw.get("NEAR_RPC_URL") or []
        if isinstance(raw, str) and raw.strip():
            urls = [raw.strip()]
        else:
            urls = [str(u) for u in raw if u]
    if not urls:
        urls = ["https://rpc.mainnet.near.org"]
    for url in urls:
        try:
            r = requests.post(url.strip(), json=body, timeout=8)
            r.raise_for_status()
            data = r.json()
            res = data.get("result") or {}
            amt = Decimal(str(res.get("amount") or "0"))
            avail = amt - _NEAR_NATIVE_RESERVE_YOCTO
            return avail if avail > 0 else Decimal("0")
        except Exception as e:
            logger.warning(f"view_account {account_id} via {url}: {e}")
    return Decimal("0")


def _account_can_receive_ft_storage(network_id: str, account_id: str) -> bool:
    """NEP-141 storage_deposit requires the account to exist on NEAR protocol."""
    aid = (account_id or "").strip()
    if not aid:
        return False
    if _ccb()._looks_like_implicit_near_account_id(aid):
        existed = _ccb()._near_protocol_account_exists(network_id, aid)
        return existed is True
    return True


def _query_user_tokens_registered(
    network_id: str,
    user: str,
    tokens: List[str],
) -> List[bool]:
    aggregate = _aggregate_dex_contract_id()
    if not tokens:
        return []
    try:
        result = _ccb()._near_view_call(
            network_id,
            contract_id=aggregate,
            method_name="query_user_tokens_registered",
            args={"user": user, "tokens": tokens},
        )
        if isinstance(result, list):
            return [bool(x) for x in result]
    except Exception as e:
        logger.warning(f"query_user_tokens_registered failed: {e}")
    return [False] * len(tokens)


def _is_native_near_token_input(token_in: Dict[str, Any]) -> bool:
    addr = (token_in.get("address") or "").strip().lower()
    sym = (token_in.get("symbol") or "").strip().upper()
    if addr == WRAP_NEAR_CONTRACT:
        return False
    return addr in ("", "near", "wnear") or sym == "NEAR"


def _is_wrapped_near_token_input(token_in: Dict[str, Any]) -> bool:
    addr = (token_in.get("address") or "").strip().lower()
    sym = (token_in.get("symbol") or "").strip().upper()
    return addr == WRAP_NEAR_CONTRACT or sym in ("WNEAR", "WNEAR".lower())


def _smartx_adjust_amount_in(
    amount_in: str,
    balance: Decimal,
    *,
    is_native_near: bool,
) -> str:
    """Mirror SDK ``ensureQuoteAmountWithinBalance`` for SmartX build-time re-quote."""
    try:
        requested = Decimal(str(amount_in))
    except (InvalidOperation, ValueError):
        return str(amount_in)
    if balance <= 0:
        return str(amount_in)
    if requested > balance:
        return str(int(balance))
    if requested < balance:
        diff = balance - requested
        diff_pct = (diff / balance) * Decimal("100") if balance > 0 else Decimal("0")
        if diff_pct < Decimal("0.1") or diff < Decimal("1000"):
            return str(int(balance))
    return str(int(requested))


def _segments_to_batch_transactions(
    segments: List[Tuple[str, Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Group consecutive actions targeting the same receiver into one NEAR transaction."""
    batch: List[Dict[str, Any]] = []
    for receiver_id, action in segments:
        rid = str(receiver_id).strip()
        if batch and batch[-1]["receiverId"] == rid:
            batch[-1]["actions"].append(action)
        else:
            batch.append({"receiverId": rid, "actions": [action]})
    return batch


def _smartx_storage_deposit_action(account_id: str) -> Dict[str, Any]:
    return _fc_action(
        "storage_deposit",
        {"account_id": account_id, "registration_only": True},
        _NEAR_GAS_STORAGE_DEPOSIT,
        _NEAR_NEW_ACCOUNT_STORAGE_COST,
    )


def _smartx_build_prep_segments(
    *,
    network_id: str,
    sender: str,
    receive_user: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    tokens: List[str],
    dexs: List[Any],
) -> Tuple[List[Tuple[str, Dict[str, Any]]], str]:
    """
    Pre-swap prep actions mirroring SDK AggregateDexRouter.executeSwap ordering.
    Returns ordered (receiverId, action) segments.
    """
    segments: List[Tuple[str, Dict[str, Any]]] = []
    tin_orig = (token_in.get("address") or "").strip()
    tin_route = _canonical_token_contract(tin_orig)
    tout_route = _canonical_token_contract(token_out.get("address"))
    token_in_contract = WRAP_NEAR_CONTRACT if _is_native_near_token_input(token_in) else (tin_orig or tin_route)
    token_out_contract = (token_out.get("address") or "").strip() or tout_route

    is_native = _is_native_near_token_input(token_in)
    is_wrapped = _is_wrapped_near_token_input(token_in)

    if is_native:
        if not _storage_registered(network_id, WRAP_NEAR_CONTRACT, sender):
            segments.append((WRAP_NEAR_CONTRACT, _smartx_storage_deposit_action(sender)))
        segments.append(
            (
                WRAP_NEAR_CONTRACT,
                _fc_action("near_deposit", {}, _NEAR_GAS_NEAR_DEPOSIT_SMARTX, str(amount_in)),
            )
        )
    elif is_wrapped:
        wnear_bal = _ft_balance_of(network_id, WRAP_NEAR_CONTRACT, sender)
        required = Decimal(str(amount_in))
        if wnear_bal < required:
            convert_amt = required - wnear_bal
            if not _storage_registered(network_id, WRAP_NEAR_CONTRACT, sender):
                segments.append((WRAP_NEAR_CONTRACT, _smartx_storage_deposit_action(sender)))
            segments.append(
                (
                    WRAP_NEAR_CONTRACT,
                    _fc_action(
                        "near_deposit",
                        {},
                        _NEAR_GAS_NEAR_DEPOSIT_SMARTX,
                        str(int(convert_amt)),
                    ),
                )
            )

    tokens_to_check: List[str] = []
    if len(dexs or []) > 1:
        tokens_to_check = [str(t) for t in (tokens or []) if t]
    else:
        tokens_to_check = [token_out_contract] if token_out_contract else []

    for token_id in tokens_to_check:
        if not token_id:
            continue
        if not _storage_registered(network_id, token_id, sender):
            segments.append((token_id, _smartx_storage_deposit_action(sender)))

    recv = (receive_user or "").strip()
    snd = (sender or "").strip()
    if recv and recv != snd and token_out_contract:
        if _account_can_receive_ft_storage(network_id, recv):
            if not _storage_registered(network_id, token_out_contract, recv):
                segments.append((token_out_contract, _smartx_storage_deposit_action(recv)))

    aggregate = _aggregate_dex_contract_id()
    token_list = [str(t) for t in (tokens or []) if t]
    for token_id in token_list:
        if not _storage_registered(network_id, token_id, aggregate):
            segments.append((token_id, _smartx_storage_deposit_action(aggregate)))

    if token_list:
        registered = _query_user_tokens_registered(network_id, sender, token_list)
        unregistered = [token_list[i] for i, ok in enumerate(registered) if not ok]
        if unregistered:
            total_deposit = str(int(Decimal(_NEAR_TOKENS_STORAGE_DEPOSIT_PER_TOKEN) * len(unregistered)))
            segments.append(
                (
                    aggregate,
                    _fc_action(
                        "tokens_storage_deposit",
                        {"user": sender, "tokens": unregistered},
                        _NEAR_GAS_TOKENS_STORAGE_DEPOSIT,
                        total_deposit,
                    ),
                )
            )

    return segments, token_in_contract


def _smartx_fetch_quote_for_build(
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    network_id = _near_network_id()
    tin = _canonical_token_contract(token_in.get("address"))
    tout = _canonical_token_contract(token_out.get("address"))

    if _is_native_near_token_input(token_in):
        balance = _native_near_available_balance(network_id, sender)
    else:
        tin_contract = (token_in.get("address") or "").strip() or tin
        balance = _ft_balance_of(network_id, tin_contract, sender)

    quote_amount = _smartx_adjust_amount_in(
        amount_in,
        balance,
        is_native_near=_is_native_near_token_input(token_in),
    )

    res = near_swap_multi_dex_path_get(
        quote_amount, tin, tout, float(slippage_decimal), sender, recipient,
    )
    if res.get("result_code") != 0:
        msg = res.get("result_message") or f"code {res.get('result_code')}"
        return None, msg

    rd = res.get("result_data") or {}
    raw_out = str(rd.get("amount_out") or "0")
    router_msg = rd.get("msg")
    signature = rd.get("signature")
    if not router_msg or not signature:
        return None, "SmartX returned incomplete route (missing msg/signature)"
    if int(Decimal(raw_out or "0")) <= 0:
        return None, "SmartX quoted output amount too small"

    return {
        "amount_in": str(rd.get("amount_in") or quote_amount),
        "amount_out": raw_out,
        "min_amount_out": str(rd.get("min_amount_out") or _min_out_after_slip(raw_out, float(slippage_decimal))),
        "router_msg": str(router_msg),
        "signature": str(signature),
        "tokens": [str(t) for t in (rd.get("tokens") or []) if t],
        "dexs": list(rd.get("dexs") or []),
        "tin": tin,
        "tout": tout,
    }, None


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


def near_swap_multi_dex_path_get(
    amount_in: str,
    token_in: str,
    token_out: str,
    slippage_decimal: float,
    user: str,
    receive_user: str,
    *,
    path_deep: int = 2,
    timeout_seconds: float = 30,
) -> Dict[str, Any]:
    """SmartX GET /swapMultiDexPath (frontend AggregateDexRouter quote)."""
    base = _smartx_base_url()
    tip = Decimal(str(amount_in))
    if tip <= 0:
        return {
            "result_code": 1007,
            "result_message": "invalid amount_in",
            "result_data": None,
        }
    if not (user or "").strip() or not (receive_user or "").strip():
        return {
            "result_code": 1007,
            "result_message": "SmartX requires user and receiveUser",
            "result_data": None,
        }

    qs = {
        "amountIn": str(int(tip)),
        "tokenIn": token_in,
        "tokenOut": token_out,
        "slippage": str(float(slippage_decimal)),
        "pathDeep": str(int(path_deep)),
        "chainId": "0",
        "routerCount": "1",
        "skipUnwrapNativeToken": "true",
        "user": user.strip(),
        "receiveUser": receive_user.strip(),
    }
    try:
        r = _session.get(f"{base}/swapMultiDexPath", params=qs, timeout=timeout_seconds)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"near_swap_multi_dex_path_get failed: {e}")
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


def _quote_core_from_findpath(
    *,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: str,
    tin: str,
    tout: str,
    res: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if res.get("result_code") != 0:
        return None
    rd = res.get("result_data") or {}
    routes = rd.get("routes") or []
    if not routes:
        return None
    swap_actions, raw_out = _swap_actions_and_raw_amount_out(rd)
    if not swap_actions:
        return None
    min_out = _min_out_after_slip(raw_out, float(slippage_decimal))
    if int(Decimal(min_out or "0")) <= 0:
        return None
    decimals_out = int(token_out.get("decimals") or 24)
    return {
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
        "_findpath_res": res,
        "_swap_actions": swap_actions,
    }


def _quote_core_from_smartx(
    *,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: str,
    tin: str,
    tout: str,
    res: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if res.get("result_code") != 0:
        return None
    rd = res.get("result_data") or {}
    raw_out = str(rd.get("amount_out") or "0")
    if int(Decimal(raw_out or "0")) <= 0:
        return None
    min_out = str(rd.get("min_amount_out") or _min_out_after_slip(raw_out, float(slippage_decimal)))
    if int(Decimal(min_out or "0")) <= 0:
        return None
    router_msg = rd.get("msg")
    signature = rd.get("signature")
    if not router_msg or not signature:
        return None
    decimals_out = int(token_out.get("decimals") or 24)
    amount_in_actual = str(rd.get("amount_in") or amount_in)
    return {
        "router": ROUTER_NEAR_SMARTX,
        "amountOut": str(int(Decimal(str(raw_out)))),
        "amountOutReadable": _shrink_token(str(int(Decimal(str(raw_out)))), decimals_out),
        "minAmountOut": min_out,
        "chainId": "near",
        "tokenIn": token_in,
        "tokenOut": token_out,
        "amountIn": amount_in_actual,
        "slippage": float(slippage_decimal),
        "sender": sender,
        "recipient": recipient,
        "isBluechipIn": False,
        "isBluechipOut": False,
        "_amountOutDecimal": Decimal(str(raw_out)),
        "_smartx_res": res,
        "routerMsg": router_msg,
        "signature": signature,
        "tokens": rd.get("tokens") or [],
        "dexs": rd.get("dexs") or [],
    }


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
    Queries Ref findPath and SmartX swapMultiDexPath in parallel; picks best amountOut.
    """
    if not recipient:
        recipient = sender

    tin = _canonical_token_contract(token_in.get("address"))
    tout = _canonical_token_contract(token_out.get("address"))
    if not tin or not tout:
        return {"success": False, "chainType": "near", "error": "Missing token contract id for NEAR swap"}
    if tin == tout:
        return {"success": False, "chainType": "near", "error": "tokenIn and tokenOut must differ"}

    slip = float(slippage_decimal)
    errors: List[str] = []
    candidates: List[Dict[str, Any]] = []

    def _run_findpath():
        res = near_find_path_get(amount_in, tin, tout, slip)
        qc = _quote_core_from_findpath(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage_decimal=slip,
            sender=sender,
            recipient=recipient,
            tin=tin,
            tout=tout,
            res=res,
        )
        if qc:
            return ("findpath", qc)
        code = res.get("result_code")
        msg = res.get("result_message") or res.get("result_msg") or f"code {code}"
        return ("findpath", None, f"findPath: {msg}")

    def _run_smartx():
        res = near_swap_multi_dex_path_get(
            amount_in, tin, tout, slip, sender, recipient,
        )
        qc = _quote_core_from_smartx(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage_decimal=slip,
            sender=sender,
            recipient=recipient,
            tin=tin,
            tout=tout,
            res=res,
        )
        if qc:
            return ("smartx", qc)
        code = res.get("result_code")
        msg = res.get("result_message") or res.get("result_msg") or f"code {code}"
        return ("smartx", None, f"SmartX: {msg}")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_run_findpath), pool.submit(_run_smartx)]
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if len(result) == 2:
                    _, qc = result
                    candidates.append(qc)
                else:
                    errors.append(result[2])
            except Exception as e:
                errors.append(str(e))

    if not candidates:
        detail = "; ".join(errors) if errors else "no NEAR swap route"
        return {"success": False, "chainType": "near", "error": f"NEAR SmartRouter quote failed: {detail}"}

    best = max(candidates, key=lambda q: q["_amountOutDecimal"])
    summary = {k: v for k, v in best.items() if not k.startswith("_") and k not in ("_findpath_res", "_smartx_res", "_swap_actions")}
    qc = {k: v for k, v in best.items() if not k.startswith("_")}

    all_summaries = []
    for c in sorted(candidates, key=lambda q: q["_amountOutDecimal"], reverse=True):
        all_summaries.append({k: v for k, v in c.items() if not k.startswith("_") and k not in ("routerMsg", "signature", "tokens", "dexs")})

    return {
        "success": True,
        "chainType": "near",
        "quote": qc,
        "allQuotes": all_summaries,
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


def _build_near_smartx_tx(
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: str,
) -> Dict[str, Any]:
    network_id = _near_network_id()
    quote_data, err = _smartx_fetch_quote_for_build(
        token_in, token_out, amount_in, slippage_decimal, sender, recipient,
    )
    if err or not quote_data:
        return {"success": False, "chainType": "near", "error": err or "SmartX quote failed during build"}

    amount_actual = quote_data["amount_in"]
    raw_out = quote_data["amount_out"]
    min_out = quote_data["min_amount_out"]
    router_msg = quote_data["router_msg"]
    signature = quote_data["signature"]
    tokens = quote_data["tokens"]
    dexs = quote_data["dexs"]
    tin = quote_data["tin"]
    tout = quote_data["tout"]

    prep_segments, token_in_contract = _smartx_build_prep_segments(
        network_id=network_id,
        sender=sender.strip(),
        receive_user=recipient.strip(),
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_actual,
        tokens=tokens,
        dexs=dexs,
    )

    aggregate_dex = _aggregate_dex_contract_id()
    msg_s = json.dumps({"msg": router_msg, "signature": signature}, separators=(",", ":"))
    swap_action = _fc_action(
        "ft_transfer_call",
        {
            "receiver_id": aggregate_dex,
            "amount": str(amount_actual),
            "msg": msg_s,
        },
        _NEAR_GAS_SMARTX_FT_TRANSFER_CALL,
        _NEAR_ATTACHED_YOCTO,
    )
    all_segments = list(prep_segments) + [(token_in_contract, swap_action)]
    batch_txs = _segments_to_batch_transactions(all_segments)

    is_native = _is_native_near_token_input(token_in)
    standard = "native" if is_native else "nep141"
    first_receiver = batch_txs[0]["receiverId"] if batch_txs else token_in_contract

    tx: Dict[str, Any] = {
        "chainId": "near",
        "signerId": sender.strip(),
        "receiverId": first_receiver,
        "standard": standard,
        "swapKind": ROUTER_NEAR_SMARTX,
        "format": NEAR_TX_FORMAT_BATCH if len(batch_txs) > 1 else "near_single",
        "tokenAddress": token_in_contract,
        "recipient": recipient.strip(),
        "amount": amount_actual,
        "transactions": batch_txs,
        "nearSmartXHints": {
            "tokenInRoute": tin,
            "tokenOutRoute": tout,
            "aggregateDexContract": aggregate_dex,
            "tokens": tokens,
            "dexs": dexs,
        },
    }
    if len(batch_txs) == 1:
        tx["actions"] = batch_txs[0]["actions"]

    return {
        "success": True,
        "chainType": "near",
        "router": ROUTER_NEAR_SMARTX,
        "tx": tx,
        "estimatedOut": str(int(Decimal(str(raw_out)))),
        "minAmountOut": min_out,
    }


def _build_near_ref_smart_tx(
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: str,
) -> Dict[str, Any]:
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


def near_same_chain_build_tx(
    router: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: Optional[str],
) -> Dict[str, Any]:
    if not recipient:
        recipient = sender

    router_norm = (router or "").strip()
    if router_norm == ROUTER_NEAR_SMARTX:
        return _build_near_smartx_tx(token_in, token_out, amount_in, slippage_decimal, sender, recipient)
    if router_norm == ROUTER_NEAR_REF_SMARTROUTER:
        return _build_near_ref_smart_tx(token_in, token_out, amount_in, slippage_decimal, sender, recipient)

    # Unknown router: re-quote and build with the winning path.
    if router_norm:
        logger.warning(f"near_same_chain_build_tx: unknown router '{router_norm}', re-quoting")
    quote = near_same_chain_quote(
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        slippage_decimal=slippage_decimal,
        sender=sender,
        recipient=recipient,
    )
    if not quote.get("success"):
        return {
            "success": False,
            "chainType": "near",
            "error": quote.get("error") or f"Unsupported NEAR router '{router}'",
        }
    picked = (quote.get("quote") or {}).get("router") or ROUTER_NEAR_REF_SMARTROUTER
    return near_same_chain_build_tx(
        router=picked,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        slippage_decimal=slippage_decimal,
        sender=sender,
        recipient=recipient,
    )


_NEAR_TX_META_KEYS = (
    "chainId",
    "standard",
    "tokenAddress",
    "depositAddress",
    "amount",
    "swapKind",
    "format",
    "recipient",
    "nearSmartXHints",
)


def near_source_tx_to_array(
    tx: Any,
    sender: str = "",
) -> List[Dict[str, Any]]:
    """
    Expand a NEAR swap/deposit tx blob into an ordered list for unified swap API.

    Used when the source chain is NEAR so ``data.tx`` is always an array of
    wallet-signable transactions (bootstrap transfer, prep calls, deposit leg, …).
    """
    if isinstance(tx, list):
        out: List[Dict[str, Any]] = []
        for item in tx:
            out.extend(near_source_tx_to_array(item, sender=sender))
        return out

    if not isinstance(tx, dict) or not tx:
        return []

    signer = (tx.get("signerId") or sender or "").strip()
    meta = {
        k: tx[k]
        for k in _NEAR_TX_META_KEYS
        if k in tx and tx[k] is not None
    }
    out: List[Dict[str, Any]] = []

    setup = tx.get("depositSetupTransaction")
    if isinstance(setup, dict):
        setup_item = dict(setup)
        if signer and not setup_item.get("signerId"):
            setup_item["signerId"] = signer
        if setup_item.get("receiverId") and isinstance(setup_item.get("actions"), list):
            out.append(setup_item)

    batch = tx.get("transactions")
    if isinstance(batch, list) and batch:
        for item in batch:
            if not isinstance(item, dict):
                continue
            rid = (item.get("receiverId") or "").strip()
            actions = item.get("actions")
            if not rid or not isinstance(actions, list) or not actions:
                continue
            out.append({
                "signerId": (item.get("signerId") or signer).strip(),
                "receiverId": rid,
                "actions": actions,
            })
        if out and meta:
            out[-1].update(meta)
        return out

    main = {
        k: v
        for k, v in tx.items()
        if k not in ("depositSetupTransaction", "transactions")
    }
    if signer and not main.get("signerId"):
        main["signerId"] = signer
    if main.get("receiverId") and isinstance(main.get("actions"), list):
        out.append(main)

    return out


def near_tx_to_sign_transactions(
    tx: Any,
    sender: str,
) -> List[Dict[str, Any]]:
    """
    Expand a NEAR build ``tx`` blob into an ordered wallet-sign batch.

    API consumers should prefer ``nearSignTransactions`` from ``/api/swap/swap``
    when present — SmartX prep (storage_deposit, tokens_storage_deposit, …)
    may span multiple ``receiverId`` values and cannot fit a single NEAR tx.
    """
    if isinstance(tx, list):
        out: List[Dict[str, Any]] = []
        for item in tx:
            out.extend(near_tx_to_sign_transactions(item, sender))
        return out

    if not isinstance(tx, dict):
        return []
    signer = (tx.get("signerId") or sender or "").strip()
    if not signer:
        return []

    setup = tx.get("depositSetupTransaction")
    if isinstance(setup, dict):
        rid = (setup.get("receiverId") or "").strip()
        actions = setup.get("actions")
        if rid and isinstance(actions, list) and actions:
            return near_source_tx_to_array(tx, sender=sender)

    batch = tx.get("transactions")
    if isinstance(batch, list) and batch:
        out: List[Dict[str, Any]] = []
        for item in batch:
            if not isinstance(item, dict):
                continue
            rid = (item.get("receiverId") or "").strip()
            actions = item.get("actions")
            if rid and isinstance(actions, list) and actions:
                out.append({
                    "signerId": signer,
                    "receiverId": rid,
                    "actions": actions,
                })
        return out

    rid = (tx.get("receiverId") or "").strip()
    actions = tx.get("actions")
    if rid and isinstance(actions, list) and actions:
        return [{"signerId": signer, "receiverId": rid, "actions": actions}]
    return []
