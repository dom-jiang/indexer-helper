"""
Unified Swap API dispatch layer.

Routes requests to same-chain or cross-chain handlers based on fromChain vs toChain.
For cross-chain: runs OmniBridge and NearIntents 1Click in parallel, picks best price.
For same-chain:  delegates to existing multi_chain_* functions.
"""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, Tuple

from loguru import logger
from redis_provider import get_chain_tokens_with_prices
from swap_utils import (
    multi_chain_quote, multi_chain_build_tx, multi_chain_approve_tx,
    detect_chain_type, shrink_token, convert_slippage_to_decimal,
    SOLANA_CHAIN_IDS, APTOS_CHAIN_IDS,
)
from omnibridge_utils import cross_chain_quote as omni_quote, cross_chain_build_tx as omni_build_tx
from nearintents_utils import (
    nearintents_quote, nearintents_build_tx,
    resolve_omni_chain, CHAIN_TO_1CLICK,
)

_executor = ThreadPoolExecutor(max_workers=4)


def _resolve_token_info(chain: str, address: str) -> Optional[Dict]:
    """
    Look up token metadata (symbol, decimals) from Redis multichain token data.
    Returns dict with address/symbol/decimals or None.
    """
    chain_str = str(chain)
    tokens = get_chain_tokens_with_prices(chain_str)
    if not tokens:
        return None

    addr_lower = address.lower()
    for tok_addr, tok_info in tokens.items():
        if isinstance(tok_info, dict) and tok_addr.lower() == addr_lower:
            return {
                "address": address,
                "symbol": tok_info.get("symbol", ""),
                "decimals": int(tok_info.get("decimals", 18)),
            }
    return None


def _is_cross_chain(from_chain: str, to_chain: str) -> bool:
    return str(from_chain) != str(to_chain)


def _normalize_chain_id(chain):
    """Normalize chain id to string for consistency."""
    return str(chain) if chain is not None else ""


def _safe_decimal(value) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _compare_cross_chain_quotes(omni_result: Dict, near_result: Dict, token_out_decimals: int) -> Tuple[Dict, list]:
    """
    Compare OmniBridge and NearIntents quotes, return (bestQuote, allQuotes).
    Compares estimatedOut in smallest units of the destination token.
    """
    all_quotes = []
    best = None
    best_amount = Decimal("-1")

    if omni_result and omni_result.get("success"):
        q = omni_result.get("quote", {})
        est_out_str = q.get("estimatedOut", "0")
        try:
            est_out_val = Decimal(str(est_out_str))
        except (InvalidOperation, ValueError):
            est_out_val = Decimal("0")

        if "." in str(est_out_str):
            from swap_utils import expand_token
            est_out_smallest = Decimal(expand_token(est_out_str, token_out_decimals))
        else:
            est_out_smallest = est_out_val

        q["estimatedOutSmallest"] = str(est_out_smallest)
        all_quotes.append(q)
        if est_out_smallest > best_amount:
            best_amount = est_out_smallest
            best = q

    if near_result and near_result.get("success"):
        q = near_result.get("quote", {})
        est_out_str = q.get("estimatedOut", "0")
        try:
            est_out_smallest = Decimal(str(est_out_str))
        except (InvalidOperation, ValueError):
            est_out_smallest = Decimal("0")

        q["estimatedOutSmallest"] = str(est_out_smallest)
        all_quotes.append(q)
        if est_out_smallest > best_amount:
            best_amount = est_out_smallest
            best = q

    return best, all_quotes


def unified_quote(
    from_chain: str,
    to_chain: str,
    token_in_address: str,
    token_out_address: str,
    amount_in: str,
    slippage: float = 0.5,
    sender: str = "",
    recipient: str = "",
) -> Dict:
    """
    Unified quote entry point.
    - Same chain: delegates to multi_chain_quote
    - Cross chain: parallel OmniBridge + NearIntents, best price
    """
    from_chain = _normalize_chain_id(from_chain)
    to_chain = _normalize_chain_id(to_chain)

    if not from_chain or not to_chain:
        return {"code": -1, "msg": "fromChain and toChain are required"}
    if not token_in_address or not token_out_address:
        return {"code": -1, "msg": "tokenIn and tokenOut addresses are required"}
    if not amount_in:
        return {"code": -1, "msg": "amountIn is required"}
    if not sender:
        return {"code": -1, "msg": "sender is required"}
    if not recipient:
        recipient = sender

    token_in_info = _resolve_token_info(from_chain, token_in_address)
    token_out_info = _resolve_token_info(to_chain, token_out_address)

    if not token_in_info:
        return {"code": -1, "msg": f"Token {token_in_address} not found on chain {from_chain}. Check address and chain."}
    if not token_out_info:
        return {"code": -1, "msg": f"Token {token_out_address} not found on chain {to_chain}. Check address and chain."}

    if not _is_cross_chain(from_chain, to_chain):
        return _same_chain_quote(from_chain, token_in_info, token_out_info, amount_in, slippage, sender, recipient)
    else:
        return _cross_chain_quote(from_chain, to_chain, token_in_info, token_out_info, amount_in, slippage, sender, recipient)


def _same_chain_quote(
    chain_id: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
) -> Dict:
    """Wrap multi_chain_quote for same-chain swaps."""
    try:
        chain_id_val = chain_id
        try:
            chain_id_val = int(chain_id)
        except (ValueError, TypeError):
            pass

        result = multi_chain_quote(
            chain_id=chain_id_val,
            token_in=token_in,
            token_out=token_out,
            amount_in=str(amount_in),
            slippage=float(slippage),
            sender=sender,
            recipient=recipient,
        )

        if result.get("success"):
            quote_data = result.get("quote", {})
            all_quotes = result.get("allQuotes", [])
            if not all_quotes and quote_data:
                all_quotes = [quote_data]
            return {
                "code": 0,
                "msg": "success",
                "data": {
                    "isCrossChain": False,
                    "bestQuote": quote_data,
                    "allQuotes": all_quotes,
                    "chainType": result.get("chainType", "evm"),
                },
            }
        else:
            return {"code": -1, "msg": result.get("error", "Quote failed"), "data": result}
    except Exception as e:
        logger.error(f"_same_chain_quote error: {e}")
        return {"code": -1, "msg": str(e)}


def _cross_chain_quote(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
) -> Dict:
    """Run OmniBridge + NearIntents quotes in parallel, return best."""
    omni_result = None
    near_result = None
    errors = []

    futures = {}

    omni_from = resolve_omni_chain(from_chain)
    omni_to = resolve_omni_chain(to_chain)
    if omni_from and omni_to:
        f = _executor.submit(
            omni_quote,
            from_chain=omni_from,
            to_chain=omni_to,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            sender=sender,
            recipient=recipient,
        )
        futures[f] = "omnibridge"

    oneclick_from = CHAIN_TO_1CLICK.get(from_chain)
    oneclick_to = CHAIN_TO_1CLICK.get(to_chain)
    if oneclick_from or oneclick_to:
        f = _executor.submit(
            nearintents_quote,
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            sender=sender,
            recipient=recipient,
            slippage=slippage,
        )
        futures[f] = "nearintents"

    if not futures:
        return {"code": -1, "msg": "No cross-chain provider supports this chain pair"}

    for future in as_completed(futures, timeout=30):
        provider = futures[future]
        try:
            result = future.result()
            if provider == "omnibridge":
                omni_result = result
            else:
                near_result = result
            if not result.get("success"):
                errors.append(f"{provider}: {result.get('error', 'unknown error')}")
        except Exception as e:
            errors.append(f"{provider}: {str(e)}")

    token_out_decimals = token_out.get("decimals", 18)
    best_quote, all_quotes = _compare_cross_chain_quotes(omni_result, near_result, token_out_decimals)

    if not best_quote:
        error_detail = "; ".join(errors) if errors else "All providers failed"
        return {"code": -1, "msg": f"Cross-chain quote failed: {error_detail}"}

    return {
        "code": 0,
        "msg": "success",
        "data": {
            "isCrossChain": True,
            "bestQuote": best_quote,
            "allQuotes": all_quotes,
            "chainType": "cross-chain",
            "errors": errors if errors else None,
        },
    }


def unified_swap(
    from_chain: str,
    to_chain: str,
    token_in_address: str,
    token_out_address: str,
    amount_in: str,
    slippage: float = 0.5,
    sender: str = "",
    recipient: str = "",
    router: str = "",
    market: str = "",
) -> Dict:
    """
    Unified swap (build tx) entry point.
    - Same chain: build tx + approve info
    - Cross chain: build via specified router (omnibridge / nearintents)
    """
    from_chain = _normalize_chain_id(from_chain)
    to_chain = _normalize_chain_id(to_chain)

    if not from_chain or not to_chain:
        return {"code": -1, "msg": "fromChain and toChain are required"}
    if not token_in_address or not token_out_address:
        return {"code": -1, "msg": "tokenIn and tokenOut addresses are required"}
    if not amount_in:
        return {"code": -1, "msg": "amountIn is required"}
    if not sender:
        return {"code": -1, "msg": "sender is required"}
    if not recipient:
        recipient = sender

    token_in_info = _resolve_token_info(from_chain, token_in_address)
    token_out_info = _resolve_token_info(to_chain, token_out_address)

    if not token_in_info:
        return {"code": -1, "msg": f"Token {token_in_address} not found on chain {from_chain}"}
    if not token_out_info:
        return {"code": -1, "msg": f"Token {token_out_address} not found on chain {to_chain}"}

    if not _is_cross_chain(from_chain, to_chain):
        return _same_chain_swap(from_chain, token_in_info, token_out_info, amount_in, slippage, sender, recipient, router, market)
    else:
        return _cross_chain_swap(from_chain, to_chain, token_in_info, token_out_info, amount_in, slippage, sender, recipient, router)


def _same_chain_swap(
    chain_id: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
    router: str,
    market: str,
) -> Dict:
    """Build same-chain swap tx + approve info."""
    if not router:
        return {"code": -1, "msg": "router is required for same-chain swap (from quote response)"}

    try:
        chain_id_val = chain_id
        try:
            chain_id_val = int(chain_id)
        except (ValueError, TypeError):
            pass

        build_result = multi_chain_build_tx(
            chain_id=chain_id_val,
            router=router,
            token_in=token_in,
            token_out=token_out,
            amount_in=str(amount_in),
            slippage=float(slippage),
            sender=sender,
            recipient=recipient,
            market=market or None,
        )

        if not build_result.get("success"):
            return {"code": -1, "msg": build_result.get("error", "Build tx failed"), "data": build_result}

        response_data = {
            "isCrossChain": False,
            "chainType": build_result.get("chainType", "evm"),
            "router": router,
            "tx": build_result.get("tx", {}),
        }

        chain_type = detect_chain_type(chain_id_val)
        if chain_type == "evm":
            approve_spender = build_result.get("approveSpender") or build_result.get("tx", {}).get("to", "")
            approve_result = multi_chain_approve_tx(
                chain_id=chain_id_val,
                router=router,
                token_address=token_in.get("address", ""),
                approve_amount=str(amount_in),
                spender=approve_spender,
            )
            if approve_result.get("success"):
                approve_tx = approve_result.get("tx")
                if approve_tx:
                    response_data["approve"] = {
                        "tx": approve_tx,
                        "spender": approve_result.get("dexContractAddress", approve_spender),
                    }
                else:
                    response_data["approve"] = None
            else:
                response_data["approve"] = None
                response_data["approveError"] = approve_result.get("error", approve_result.get("msg", ""))
        else:
            response_data["approve"] = None

        return {"code": 0, "msg": "success", "data": response_data}
    except Exception as e:
        logger.error(f"_same_chain_swap error: {e}")
        return {"code": -1, "msg": str(e)}


def _cross_chain_swap(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
    router: str,
) -> Dict:
    """Build cross-chain swap via specified router."""
    if not router:
        return {"code": -1, "msg": "router is required for cross-chain swap (from quote response, e.g. 'omnibridge' or 'nearintents')"}

    try:
        if router == "omnibridge":
            omni_from = resolve_omni_chain(from_chain)
            omni_to = resolve_omni_chain(to_chain)
            if not omni_from or not omni_to:
                return {"code": -1, "msg": f"OmniBridge does not support chain {from_chain} -> {to_chain}"}

            result = omni_build_tx(
                from_chain=omni_from,
                to_chain=omni_to,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                sender=sender,
                recipient=recipient,
                slippage=slippage,
            )
        elif router == "nearintents":
            result = nearintents_build_tx(
                from_chain=from_chain,
                to_chain=to_chain,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                sender=sender,
                recipient=recipient,
                slippage=slippage,
            )
        else:
            return {"code": -1, "msg": f"Unknown cross-chain router: {router}. Supported: omnibridge, nearintents"}

        if result.get("success"):
            return {
                "code": 0,
                "msg": "success",
                "data": {
                    "isCrossChain": True,
                    "chainType": "cross-chain",
                    "router": router,
                    "tx": result.get("tx", {}),
                    "approve": None,
                },
            }
        else:
            return {"code": -1, "msg": result.get("error", "Cross-chain swap failed"), "data": result}
    except Exception as e:
        logger.error(f"_cross_chain_swap error: {e}")
        return {"code": -1, "msg": str(e)}
