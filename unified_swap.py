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
    CHAIN_TYPE_EVM, CHAIN_TYPE_SOLANA, CHAIN_TYPE_APTOS,
    BLUECHIP_TOKENS, is_native_token, normalize_evm_address,
    okx_quote_exact_out, build_okx_exact_out_swap_tx,
    build_swap_tx as build_same_chain_swap_tx,
    build_approve_tx as build_same_chain_approve_tx,
)


# Native (gas) token metadata per chain. Redis token-price cache is keyed by ERC20
# contract address and therefore does NOT contain the native gas token, so when the
# frontend passes `0x000...0` / empty string / `0xEeeeE...` we have to resolve metadata
# from this static table instead of via a Redis lookup. Keys use the same aliases as
# `_candidate_chain_keys`; the first match wins.
_NATIVE_TOKEN_META = {
    # EVM
    "1":      {"symbol": "ETH",  "decimals": 18},
    "10":     {"symbol": "ETH",  "decimals": 18},
    "42161":  {"symbol": "ETH",  "decimals": 18},
    "8453":   {"symbol": "ETH",  "decimals": 18},
    "59144":  {"symbol": "ETH",  "decimals": 18},
    "534352": {"symbol": "ETH",  "decimals": 18},
    "324":    {"symbol": "ETH",  "decimals": 18},
    "81457":  {"symbol": "ETH",  "decimals": 18},
    "1101":   {"symbol": "ETH",  "decimals": 18},
    "169":    {"symbol": "ETH",  "decimals": 18},
    "130":    {"symbol": "ETH",  "decimals": 18},
    "56":     {"symbol": "BNB",  "decimals": 18},
    "137":    {"symbol": "POL",  "decimals": 18},
    "43114":  {"symbol": "AVAX", "decimals": 18},
    "100":    {"symbol": "xDAI", "decimals": 18},
    "25":     {"symbol": "CRO",  "decimals": 18},
    "250":    {"symbol": "FTM",  "decimals": 18},
    "1088":   {"symbol": "METIS", "decimals": 18},
    "1030":   {"symbol": "CFX",  "decimals": 18},
    "5000":   {"symbol": "MNT",  "decimals": 18},
    "7000":   {"symbol": "ZETA", "decimals": 18},
    "146":    {"symbol": "S",    "decimals": 18},
    "80094":  {"symbol": "BERA", "decimals": 18},
    "196":    {"symbol": "OKB",  "decimals": 18},
    "143":    {"symbol": "MON",  "decimals": 18},
    "9745":   {"symbol": "XPL",  "decimals": 18},
    "4200":   {"symbol": "BTC",  "decimals": 18},
    "36900":  {"symbol": "BTC",  "decimals": 18},
    # Non-EVM
    "sol":    {"symbol": "SOL",  "decimals": 9},
    "501":    {"symbol": "SOL",  "decimals": 9},
    "aptos":  {"symbol": "APT",  "decimals": 8},
    "637":    {"symbol": "APT",  "decimals": 8},
    "sui":    {"symbol": "SUI",  "decimals": 9},
    "784":    {"symbol": "SUI",  "decimals": 9},
    "tron":   {"symbol": "TRX",  "decimals": 6},
    "195":    {"symbol": "TRX",  "decimals": 6},
    "near":   {"symbol": "NEAR", "decimals": 24},
    "ton":    {"symbol": "TON",  "decimals": 9},
    "btc":    {"symbol": "BTC",  "decimals": 8},
    "doge":   {"symbol": "DOGE", "decimals": 8},
    "ltc":    {"symbol": "LTC",  "decimals": 8},
    "bch":    {"symbol": "BCH",  "decimals": 8},
    "zec":    {"symbol": "ZEC",  "decimals": 8},
    "xrp":    {"symbol": "XRP",  "decimals": 6},
    "stellar": {"symbol": "XLM", "decimals": 7},
    "cardano": {"symbol": "ADA", "decimals": 6},
}
from omnibridge_utils import cross_chain_quote as omni_quote, cross_chain_build_tx as omni_build_tx
from nearintents_utils import (
    nearintents_quote, nearintents_build_tx,
    resolve_omni_chain, resolve_1click_asset_id, CHAIN_TO_1CLICK,
)
from cross_chain_tx_builder import (
    build_evm_deposit_tx, build_aptos_deposit_tx, build_solana_deposit_tx,
)

_executor = ThreadPoolExecutor(max_workers=4)


# Map of human-readable / canonical chain identifiers => ordered list of
# Redis chain-key candidates used by the multichain token price job.
#
# Background: the token-price writer stores the same chain under multiple
# keys depending on the upstream source (e.g. Solana is present under both
# `sol` and `501`, Arbitrum under both `42161` and `arb`). The frontend may
# pass any of those forms for `fromChain` / `toChain`, so we try each
# candidate in order and use the first one that yields data.
_CHAIN_KEY_ALIASES = {
    # Non-EVM
    "solana": ["sol", "501"],
    "sol": ["sol", "501"],
    "501": ["501", "sol"],
    "aptos": ["aptos"],
    "637": ["aptos"],
    "near": ["near"],
    "sui": ["sui", "784"],
    "784": ["784", "sui"],
    "tron": ["tron", "195"],
    "195": ["195", "tron"],
    "ton": ["ton"],
    "btc": ["btc"], "bitcoin": ["btc"],
    "doge": ["doge"],
    "ltc": ["ltc"], "litecoin": ["ltc"],
    "bch": ["bch"],
    "zec": ["zec"], "zcash": ["zec"],
    "xrp": ["xrp"], "ripple": ["xrp"],
    "cardano": ["cardano"], "ada": ["cardano"],
    "starknet": ["starknet"],
    "stellar": ["stellar"], "xlm": ["stellar"],
    "aleo": ["aleo"],

    # EVM (name => numeric canonical + short alias)
    "ethereum": ["1", "eth"], "eth": ["1", "eth"], "1": ["1", "eth"],
    "arbitrum": ["42161", "arb"], "arb": ["42161", "arb"], "42161": ["42161", "arb"],
    "bsc": ["56", "bsc"], "bnb": ["56", "bsc"], "56": ["56", "bsc"],
    "base": ["8453", "base"], "8453": ["8453", "base"],
    "optimism": ["10", "op"], "op": ["10", "op"], "10": ["10", "op"],
    "polygon": ["137", "pol"], "matic": ["137", "pol"], "pol": ["137", "pol"], "137": ["137", "pol"],
    "avalanche": ["43114", "avax"], "avax": ["43114", "avax"], "43114": ["43114", "avax"],
    "linea": ["59144"], "59144": ["59144"],
    "scroll": ["534352"], "534352": ["534352"],
    "zksync": ["324"], "324": ["324"],
    "mantle": ["5000"], "5000": ["5000"],
    "blast": ["81457"], "81457": ["81457"],
    "cronos": ["25"], "25": ["25"],
    "metis": ["1088"], "1088": ["1088"],
    "polygon-zkevm": ["1101"], "1101": ["1101"],
    "zeta": ["7000"], "7000": ["7000"],
    "sonic": ["146"], "146": ["146"],
    "unichain": ["130"], "130": ["130"],
    "berachain": ["80094", "bera"], "bera": ["80094", "bera"], "80094": ["80094", "bera"],
    "xlayer": ["196", "xlayer"], "196": ["196", "xlayer"],
    "monad": ["monad", "143"], "143": ["143", "monad"],
    "plasma": ["plasma", "9745"], "9745": ["9745", "plasma"],
    "gnosis": ["100", "gnosis"], "100": ["100", "gnosis"],
    "manta": ["169"], "169": ["169"],
    "merlin": ["4200"], "4200": ["4200"],
    "fantom": ["250"], "250": ["250"],
    "conflux": ["1030"], "1030": ["1030"],
    "botanix": ["36900"], "36900": ["36900"],
}


def _candidate_chain_keys(chain) -> list:
    """Return list of Redis chain-key candidates for a given chain input."""
    if chain is None:
        return []
    s = str(chain).strip().lower()
    if not s:
        return []
    aliases = _CHAIN_KEY_ALIASES.get(s)
    if aliases:
        return aliases
    return [s]


def _resolve_native_token_meta(chain) -> Dict:
    """Return {symbol, decimals} for the native gas token of the given chain.

    Falls back to EVM-style ETH/18 decimals for unlisted chains so at least the
    transaction building (which only needs `decimals`) still works.
    """
    for candidate in _candidate_chain_keys(chain):
        meta = _NATIVE_TOKEN_META.get(candidate)
        if meta:
            return meta
    return {"symbol": "ETH", "decimals": 18}


def _resolve_token_info(chain: str, address: str) -> Optional[Dict]:
    """
    Look up token metadata (symbol, decimals) from Redis multichain token data.
    Tries chain-key aliases in order and returns dict with address/symbol/decimals, or None.

    Native gas tokens (passed as empty string, `0x000...0`, or the OKX sentinel
    `0xEeee...`) are short-circuited to a static mapping because Redis token
    cache is keyed by ERC20 contract address and does not store them.
    """
    addr_raw = address or ""
    if is_native_token(addr_raw):
        meta = _resolve_native_token_meta(chain)
        # Preserve the original address string the frontend passed so downstream
        # code (OKX / Bitget / Jupiter / Panora adapters) can re-detect native via
        # their own `is_native_token` checks.
        return {
            "address": addr_raw,
            "symbol": meta["symbol"],
            "decimals": int(meta["decimals"]),
        }

    addr_lower = addr_raw.lower()
    for candidate in _candidate_chain_keys(chain):
        tokens = get_chain_tokens_with_prices(candidate)
        if not tokens:
            continue
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


def _safe_int_str(value) -> int:
    """
    Parse a string/int-like value into int (smallest units).
    Returns 0 on failure.
    """
    try:
        if value is None:
            return 0
        if isinstance(value, bool):
            return 0
        s = str(value).strip()
        if not s:
            return 0
        # Some providers may return decimal strings (readable) - we only support int smallest here.
        if "." in s:
            return 0
        return int(s)
    except (ValueError, TypeError):
        return 0


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


# ============================================================
# Two-stage cross-chain (pre-swap + NearIntents bridge)
# ============================================================
#
# Used when `tokenIn` on `fromChain` is NOT directly supported by any
# cross-chain provider (OmniBridge / NearIntents 1Click), but an
# intermediate bluechip token on the same fromChain (USDC / USDT / WETH)
# IS supported by 1Click to deliver `tokenOut` on `toChain`.
#
# Flow:
#   stage A (same chain, OKX):   tokenIn  -> intermediate on fromChain
#   stage B (cross chain, 1Click): intermediate on fromChain -> tokenOut on toChain
#
# At swap time we use OKX **exactOut** for stage A so that the exact
# intermediate amount promised to 1Click is delivered in a single
# user-signed transaction whose receiver is the 1Click depositAddress.

_PRESWAP_ROUTER_NAME = "preswap-nearintents"
_PRESWAP_INTERMEDIATE_SYMBOLS = ("USDC", "USDT", "WETH")


def _chain_id_int(chain) -> Optional[int]:
    try:
        return int(str(chain))
    except (ValueError, TypeError):
        return None


def _token_addr_eq(a: str, b: str) -> bool:
    if not a or not b:
        return False
    try:
        return normalize_evm_address(a) == normalize_evm_address(b)
    except Exception:
        return a.lower() == b.lower()


def _preswap_intermediate_candidates(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
) -> Tuple[list, Optional[str]]:
    """Return `(candidates, reason)` where `candidates` is an ordered list of
    intermediate-token candidates on `from_chain` that 1Click supports as source
    and can reach `token_out` on `to_chain`. When the list is empty, `reason`
    carries a human-readable explanation so the caller can surface it verbatim
    instead of reporting a generic "no intermediate" message.
    """
    chain_int = _chain_id_int(from_chain)
    if chain_int is None:
        return [], f"pre-swap route requires EVM fromChain (got {from_chain})"

    # Destination must be supported by 1Click, otherwise the bridge stage cannot succeed.
    dest_asset = resolve_1click_asset_id(to_chain, token_out.get("address", ""))
    if not dest_asset:
        return [], (
            f"destination token {token_out.get('address', '')} on chain {to_chain} "
            f"not supported by 1Click"
        )

    bluechip_cfg = BLUECHIP_TOKENS.get(chain_int) or {}
    if not bluechip_cfg:
        return [], f"no bluechip intermediate configured for chain {from_chain}"
    token_in_addr = token_in.get("address", "")

    candidates = []
    for sym in _PRESWAP_INTERMEDIATE_SYMBOLS:
        cfg = bluechip_cfg.get(sym)
        if not cfg:
            continue
        addr = cfg.get("address", "")
        if not addr:
            continue
        if _token_addr_eq(addr, token_in_addr):
            # tokenIn IS this intermediate -> pre-swap is pointless, direct route should handle it.
            continue
        asset_id = resolve_1click_asset_id(from_chain, addr)
        if not asset_id:
            continue
        candidates.append({
            "address": addr,
            "symbol": cfg.get("symbol", sym),
            "decimals": int(cfg.get("decimals", 18)),
            "oneClickAssetId": asset_id,
        })
    if not candidates:
        return [], f"no 1Click-supported intermediate (USDC/USDT/WETH) on chain {from_chain}"
    return candidates, None


def _preswap_cross_chain_quote(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
) -> Dict:
    """Try the two-stage pre-swap + NearIntents route. Returns a unified quote dict
    ({"success": True/False, ...}) shaped like a cross-chain quote, with added
    `preSwap` / `bridge` sub-objects describing each stage.
    """
    chain_int = _chain_id_int(from_chain)
    if chain_int is None:
        return {"success": False, "error": "pre-swap route requires EVM fromChain"}

    candidates, reason = _preswap_intermediate_candidates(from_chain, to_chain, token_in, token_out)
    if not candidates:
        return {"success": False, "error": reason or "no 1Click-supported intermediate on fromChain"}

    slippage_decimal = convert_slippage_to_decimal(slippage)
    # The intermediate-amount buffer protects stage A: we target a slightly lower amount than
    # the raw OKX estimate so exactOut at swap time is feasible even if price drifts slightly.
    mid_buffer = Decimal("1") - Decimal(str(slippage_decimal))
    token_out_decimals = int(token_out.get("decimals", 18))

    best = None
    best_amount = Decimal("-1")
    all_quotes = []
    errors = []

    for inter in candidates:
        try:
            # Stage A: OKX exactIn (tokenIn -> intermediate) to estimate mid amount.
            from swap_utils import okx_quote as _okx_quote_raw, _parse_okx_quote as _okx_parse
            raw = _okx_quote_raw(
                chain_id=chain_int,
                token_in=token_in,
                token_out=inter,
                amount_in=str(amount_in),
                slippage=slippage_decimal,
                user_address=sender,
            )
            if not raw.get("success"):
                errors.append(f"{inter['symbol']}: OKX quote failed: {raw.get('error')}")
                continue
            parsed = _okx_parse(raw["data"], inter, slippage_decimal)
            if not parsed:
                errors.append(f"{inter['symbol']}: OKX quote unparseable")
                continue

            mid_amount_raw = Decimal(parsed["amountOut"])  # estimated mid
            # Target a slightly lower mid amount so stage-A exactOut is still feasible.
            mid_amount_target = int(mid_amount_raw * mid_buffer)
            if mid_amount_target <= 0:
                errors.append(f"{inter['symbol']}: mid target <= 0")
                continue

            # Stage B: NearIntents 1Click quote (dry) with the target mid amount.
            near_res = nearintents_quote(
                from_chain=from_chain,
                to_chain=to_chain,
                token_in=inter,
                token_out=token_out,
                amount_in=str(mid_amount_target),
                sender=sender,
                recipient=recipient,
                slippage=slippage,
            )
            if not near_res.get("success"):
                errors.append(f"{inter['symbol']}: 1Click quote failed: {near_res.get('error')}")
                continue

            near_quote = near_res.get("quote", {}) or {}
            est_out_str = str(near_quote.get("estimatedOut", "0"))
            try:
                est_out_dec = Decimal(est_out_str)
            except (InvalidOperation, ValueError):
                est_out_dec = Decimal("0")

            quote_obj = {
                "router": _PRESWAP_ROUTER_NAME,
                "fromChain": str(from_chain),
                "toChain": str(to_chain),
                "tokenIn": token_in,
                "tokenOut": token_out,
                "amountIn": str(amount_in),
                "estimatedOut": str(int(est_out_dec)) if est_out_dec > 0 else est_out_str,
                "minAmountOut": str(near_quote.get("minAmountOut", "")),
                "estimatedOutFormatted": near_quote.get("estimatedOutFormatted", ""),
                "estimatedOutUsd": near_quote.get("estimatedOutUsd", ""),
                "amountInUsd": near_quote.get("amountInUsd", ""),
                "timeEstimate": near_quote.get("timeEstimate", ""),
                "sender": sender,
                "recipient": recipient or sender,
                # Stages — consumed verbatim by /api/swap/swap to lock the route.
                "preSwap": {
                    "router": "okx",
                    "chainId": str(from_chain),
                    "tokenIn": token_in,
                    "tokenOut": inter,
                    "amountIn": str(amount_in),
                    "estimatedAmountOut": str(int(mid_amount_raw)),
                    "amountOutTarget": str(mid_amount_target),
                    "slippage": str(slippage_decimal),
                },
                "bridge": {
                    "router": "nearintents",
                    "fromChain": str(from_chain),
                    "toChain": str(to_chain),
                    "tokenIn": inter,
                    "tokenOut": token_out,
                    "amountIn": str(mid_amount_target),
                    "estimatedOut": str(int(est_out_dec)) if est_out_dec > 0 else est_out_str,
                    "minAmountOut": str(near_quote.get("minAmountOut", "")),
                    "timeEstimate": near_quote.get("timeEstimate", ""),
                },
            }
            quote_obj["estimatedOutSmallest"] = str(int(est_out_dec))
            all_quotes.append(quote_obj)

            if est_out_dec > best_amount:
                best_amount = est_out_dec
                best = quote_obj
        except Exception as e:
            logger.error(f"_preswap_cross_chain_quote intermediate={inter.get('symbol')} error: {e}")
            errors.append(f"{inter.get('symbol')}: {e}")

    if not best:
        return {"success": False, "error": "pre-swap route failed: " + "; ".join(errors)}

    return {
        "success": True,
        "router": _PRESWAP_ROUTER_NAME,
        "quote": best,
        "allQuotes": all_quotes,
        "errors": errors or None,
    }


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
    - Cross chain: parallel OmniBridge + NearIntents, best price.
      If all direct providers fail and `tokenIn` is not 1Click-supported,
      falls back to the two-stage pre-swap (OKX) + 1Click bridge route.
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

    # If no direct provider returned a usable quote, try the two-stage pre-swap route
    # as a fallback. This is especially important when `tokenIn` is a long-tail token
    # (e.g. VELA) that 1Click does not list but USDC/USDT/WETH on `fromChain` does.
    if not best_quote:
        preswap_res = _preswap_cross_chain_quote(
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage=slippage,
            sender=sender,
            recipient=recipient,
        )
        if preswap_res.get("success"):
            best_quote = preswap_res.get("quote")
            ps_all = preswap_res.get("allQuotes") or []
            all_quotes = list(all_quotes) + ps_all
            if errors:
                logger.info(f"cross-chain direct providers all failed, falling back to pre-swap route. direct errors: {errors}")
        else:
            errors.append(f"preswap: {preswap_res.get('error')}")

    if not best_quote:
        error_detail = "; ".join(errors) if errors else "All providers failed"
        return {"code": -1, "msg": f"Cross-chain quote failed: {error_detail}"}

    # Log non-fatal provider failures but do not expose them to the caller on success,
    # otherwise a successful quote looks half-broken (e.g. OmniBridge returning 404
    # for EVM<->EVM pairs it never supports).
    if errors:
        logger.info(f"cross-chain quote partial provider failures (ignored on success): {errors}")

    return {
        "code": 0,
        "msg": "success",
        "data": {
            "isCrossChain": True,
            "bestQuote": best_quote,
            "allQuotes": all_quotes,
            "chainType": "cross-chain",
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
    quote_expected_out: str = "",
    quote_min_amount_out: str = "",
    pre_swap: Optional[Dict] = None,
    bridge: Optional[Dict] = None,
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
        return _same_chain_swap(
            from_chain,
            token_in_info,
            token_out_info,
            amount_in,
            slippage,
            sender,
            recipient,
            router,
            market,
            quote_expected_out=quote_expected_out,
            quote_min_amount_out=quote_min_amount_out,
        )
    else:
        return _cross_chain_swap(
            from_chain,
            to_chain,
            token_in_info,
            token_out_info,
            amount_in,
            slippage,
            sender,
            recipient,
            router,
            quote_expected_out=quote_expected_out,
            quote_min_amount_out=quote_min_amount_out,
            pre_swap=pre_swap,
            bridge=bridge,
        )


def _build_common_response_data(
    is_cross_chain: bool,
    source_chain_type: str,
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    router: str,
) -> Dict:
    """Common top-level response fields shared by same-chain / cross-chain."""
    return {
        "isCrossChain": is_cross_chain,
        "chainType": source_chain_type,
        "router": router,
        "fromChain": str(from_chain),
        "toChain": str(to_chain),
        "tokenIn": token_in,
        "tokenOut": token_out,
        "amountIn": str(amount_in),
        "tx": None,
        "approve": None,
        "needsApprove": False,
        "deposit": None,
    }


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
    quote_expected_out: str = "",
    quote_min_amount_out: str = "",
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

        source_chain_type = build_result.get("chainType") or detect_chain_type(chain_id_val)

        response_data = _build_common_response_data(
            is_cross_chain=False,
            source_chain_type=source_chain_type,
            from_chain=chain_id,
            to_chain=chain_id,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            router=router,
        )
        response_data["tx"] = build_result.get("tx", {})
        response_data["estimatedOut"] = build_result.get("estimatedOut", "")
        response_data["minAmountOut"] = build_result.get("minAmountOut", "")

        # Deviation check: if caller passes /quote results, ensure current build is not worse than quote's minAmountOut.
        # This prevents "quote shows 1U but swap silently builds for 0.81U" type confusion.
        quote_min_int = _safe_int_str(quote_min_amount_out)
        if quote_min_int > 0:
            build_est_int = _safe_int_str(response_data.get("estimatedOut", ""))
            if build_est_int > 0 and build_est_int < quote_min_int:
                return {
                    "code": -2,
                    "msg": "Price moved too much, please re-quote",
                    "data": {
                        **response_data,
                        "quoteExpectedOut": str(quote_expected_out or ""),
                        "quoteMinAmountOut": str(quote_min_amount_out or ""),
                        "buildEstimatedOut": str(response_data.get("estimatedOut", "")),
                    },
                }

        if source_chain_type == CHAIN_TYPE_EVM:
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
                    response_data["needsApprove"] = True
            else:
                response_data["approveError"] = approve_result.get("error", approve_result.get("msg", ""))

        return {"code": 0, "msg": "success", "data": response_data}
    except Exception as e:
        logger.error(f"_same_chain_swap error: {e}")
        return {"code": -1, "msg": str(e)}


def _preswap_cross_chain_swap(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
    pre_swap: Dict,
    bridge: Dict,
    quote_expected_out: str = "",
    quote_min_amount_out: str = "",
) -> Dict:
    """Build the two-stage cross-chain swap.

    Expects `pre_swap` and `bridge` sub-objects from the /api/swap/quote response
    (they describe the locked intermediate and amounts). The user signs ONE swap
    tx (stage A: OKX exactOut to 1Click depositAddress) plus one optional approve.

    Response shape matches direct cross-chain swap (`tx` + `approve` + `deposit`)
    with an additional `preSwap` block carrying stage-A metadata and route info.
    """
    if not pre_swap or not isinstance(pre_swap, dict):
        return {"code": -1, "msg": "preSwap is required for two-stage route (from /api/swap/quote response)"}
    if not bridge or not isinstance(bridge, dict):
        return {"code": -1, "msg": "bridge is required for two-stage route (from /api/swap/quote response)"}

    chain_int = _chain_id_int(from_chain)
    if chain_int is None:
        return {"code": -1, "msg": "pre-swap route requires EVM fromChain"}

    intermediate = pre_swap.get("tokenOut") or {}
    if not isinstance(intermediate, dict) or not intermediate.get("address"):
        return {"code": -1, "msg": "preSwap.tokenOut missing or invalid"}

    # Target intermediate amount we promised 1Click during quote; use the locked value so
    # quote/swap stay consistent.
    mid_target_str = str(pre_swap.get("amountOutTarget") or pre_swap.get("amountOut") or "")
    mid_target = _safe_int_str(mid_target_str)
    if mid_target <= 0:
        return {"code": -1, "msg": "preSwap.amountOutTarget is required and must be a positive integer"}

    try:
        # 1) Stage B: create the 1Click order (dry=false) so we have a depositAddress.
        #    Use EXACT_INPUT with `amount = mid_target` — this matches what stage A will deliver.
        near_res = nearintents_build_tx(
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=intermediate,
            token_out=token_out,
            amount_in=str(mid_target),
            sender=sender,
            recipient=recipient,
            slippage=slippage,
        )
        if not near_res.get("success"):
            return {"code": -1, "msg": f"Bridge order creation failed: {near_res.get('error')}"}

        cross_tx = near_res.get("tx", {}) or {}
        deposit_address = cross_tx.get("depositAddress", "")
        if not deposit_address:
            return {"code": -1, "msg": "1Click did not return depositAddress for pre-swap route"}
        deposit_memo = cross_tx.get("depositMemo", "")
        order_id = cross_tx.get("orderId", deposit_address)
        bridge_estimated_out = cross_tx.get("estimatedOut", "")
        bridge_min_out = cross_tx.get("minAmountOut", "")

        # 2) Stage A: OKX exactIn swap (tokenIn -> intermediate), delivered to depositAddress.
        #
        # We do NOT use OKX exactOut here because the OKX aggregator only supports exactOut on
        # Ethereum / Base / BSC / Arbitrum via Uni V3 pools. Long-tail tokens (VELA etc.) that
        # actually need the two-stage route almost never have Uni V3 liquidity, so exactOut
        # returns "Input value is too low". With exactIn + OKX contract-level slippage guard
        # (slippagePercent), the amount delivered to depositAddress is guaranteed to be
        # >= quote-time `amountOutTarget` (which already equals estimated * (1 - slippage)).
        # The 1Click order uses that same `amountOutTarget` as the EXACT_INPUT amount; any
        # extra delivered above the target is handled by 1Click (processed or refunded to
        # `refundTo`), so the user still receives at least the quoted output.
        stage_a = build_same_chain_swap_tx(
            chain_id=chain_int,
            router="okx",
            token_in=token_in,
            token_out=intermediate,
            amount_in=str(amount_in),
            slippage=slippage,
            sender=sender,
            recipient=deposit_address,
        )
        if not stage_a.get("success"):
            return {"code": -1, "msg": f"Pre-swap tx build failed: {stage_a.get('error')}"}

        stage_a_tx = stage_a.get("tx") or {}
        stage_a_estimated_out = stage_a.get("estimatedOut") or ""
        stage_a_min_out = stage_a.get("minAmountOut") or ""

        # Safety: OKX's swap-time min-out must be >= the bridge's expected mid amount
        # (otherwise the bridge could be under-delivered and refund). If OKX now quotes lower
        # than quote time, fail fast so the frontend re-quotes.
        stage_a_min_int = _safe_int_str(stage_a_min_out)
        if stage_a_min_int > 0 and stage_a_min_int < mid_target:
            return {
                "code": -2,
                "msg": "Price moved too much, please re-quote",
                "data": {
                    "preSwapMinOut": str(stage_a_min_out or ""),
                    "bridgeExpectedIn": str(mid_target),
                    "quoteMinAmountOut": str(quote_min_amount_out or ""),
                },
            }

        response_data = _build_common_response_data(
            is_cross_chain=True,
            source_chain_type=CHAIN_TYPE_EVM,
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            router=_PRESWAP_ROUTER_NAME,
        )
        # Top-level `tx` is the user-signed stage-A tx (it alone triggers both stages).
        response_data["tx"] = stage_a_tx
        response_data["estimatedOut"] = str(bridge_estimated_out or "")
        response_data["minAmountOut"] = str(bridge_min_out or "")
        response_data["deposit"] = {
            "depositAddress": deposit_address,
            "depositMemo": deposit_memo,
            "depositChain": cross_tx.get("depositChain", str(from_chain)),
            "orderId": order_id,
            "estimatedOut": bridge_estimated_out,
            "minAmountOut": bridge_min_out,
            "timeEstimate": cross_tx.get("timeEstimate", ""),
        }
        response_data["preSwap"] = {
            "router": "okx",
            "chainId": str(from_chain),
            "tokenIn": token_in,
            "tokenOut": intermediate,
            "swapMode": "exactIn",
            "amountIn": str(amount_in),
            "estimatedAmountOut": str(stage_a_estimated_out or ""),
            "minAmountOut": str(stage_a_min_out or ""),
            "amountOutTarget": str(mid_target),
            "receiver": deposit_address,
        }
        response_data["bridge"] = {
            "router": "nearintents",
            "fromChain": str(from_chain),
            "toChain": str(to_chain),
            "tokenIn": intermediate,
            "tokenOut": token_out,
            "amountIn": str(mid_target),
            "estimatedOut": str(bridge_estimated_out or ""),
            "minAmountOut": str(bridge_min_out or ""),
            "timeEstimate": cross_tx.get("timeEstimate", ""),
            "depositAddress": deposit_address,
            "orderId": order_id,
        }

        # 3) Approve info: user approves `amountIn` of tokenIn to OKX dexContractAddress
        #    (exactIn consumes the full amountIn).
        if not is_native_token(token_in.get("address", "")):
            approve_amount = str(amount_in)
            approve_spender = stage_a_tx.get("to", "")
            approve_res = build_same_chain_approve_tx(
                chain_id=chain_int,
                router="okx",
                token_address=token_in.get("address", ""),
                approve_amount=approve_amount,
                spender=approve_spender,
            )
            if approve_res.get("success"):
                approve_tx = approve_res.get("tx")
                if approve_tx:
                    response_data["approve"] = {
                        "tx": approve_tx,
                        "spender": approve_res.get("dexContractAddress", approve_spender),
                    }
                    response_data["needsApprove"] = True
            else:
                response_data["approveError"] = approve_res.get("error", approve_res.get("msg", ""))

        # Deviation check against /quote results.
        quote_min_int = _safe_int_str(quote_min_amount_out)
        if quote_min_int > 0:
            build_est_int = _safe_int_str(bridge_estimated_out)
            if build_est_int > 0 and build_est_int < quote_min_int:
                return {
                    "code": -2,
                    "msg": "Price moved too much, please re-quote",
                    "data": {
                        **response_data,
                        "quoteExpectedOut": str(quote_expected_out or ""),
                        "quoteMinAmountOut": str(quote_min_amount_out or ""),
                        "buildEstimatedOut": str(bridge_estimated_out or ""),
                    },
                }

        return {"code": 0, "msg": "success", "data": response_data}
    except Exception as e:
        logger.error(f"_preswap_cross_chain_swap error: {e}")
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
    quote_expected_out: str = "",
    quote_min_amount_out: str = "",
    pre_swap: Optional[Dict] = None,
    bridge: Optional[Dict] = None,
) -> Dict:
    """
    Build cross-chain swap via specified router.

    Returns the SAME top-level shape as same-chain swap, but `tx` contains a
    source-chain deposit transaction (ERC20/native transfer for EVM, Move
    entry function for Aptos, SPL transfer descriptor for Solana) that the
    user signs to send tokens to the `depositAddress`.

    Additional `deposit` field exposes cross-chain metadata
    (depositAddress, orderId, estimatedOut, minAmountOut, etc.) for UI.
    """
    if not router:
        return {"code": -1, "msg": "router is required for cross-chain swap (from quote response, e.g. 'omnibridge' or 'nearintents')"}

    # Two-stage route: user's tokenIn is not 1Click-supported, we pre-swap via OKX into a
    # bluechip intermediate and then bridge via 1Click. The quote response carries the
    # locked `preSwap` + `bridge` sub-objects which we consume here verbatim.
    if router == _PRESWAP_ROUTER_NAME:
        return _preswap_cross_chain_swap(
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage=slippage,
            sender=sender,
            recipient=recipient,
            pre_swap=pre_swap or {},
            bridge=bridge or {},
            quote_expected_out=quote_expected_out,
            quote_min_amount_out=quote_min_amount_out,
        )

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
            return {"code": -1, "msg": f"Unknown cross-chain router: {router}. Supported: omnibridge, nearintents, {_PRESWAP_ROUTER_NAME}"}

        if not result.get("success"):
            return {"code": -1, "msg": result.get("error", "Cross-chain swap failed"), "data": result}

        cross_tx = result.get("tx", {}) or {}
        deposit_address = cross_tx.get("depositAddress", "")
        deposit_memo = cross_tx.get("depositMemo", "")
        order_id = cross_tx.get("orderId", deposit_address)
        estimated_out = cross_tx.get("estimatedOut", "")
        min_amount_out = cross_tx.get("minAmountOut", "")

        if not deposit_address:
            return {"code": -1, "msg": "Cross-chain provider did not return depositAddress"}

        source_chain_type = detect_chain_type(from_chain)

        response_data = _build_common_response_data(
            is_cross_chain=True,
            source_chain_type=source_chain_type,
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            router=router,
        )

        if source_chain_type == CHAIN_TYPE_EVM:
            chain_id_val = from_chain
            try:
                chain_id_val = int(from_chain)
            except (ValueError, TypeError):
                pass
            response_data["tx"] = build_evm_deposit_tx(
                chain_id=chain_id_val,
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
            )
            response_data["approve"] = None
        elif source_chain_type == CHAIN_TYPE_APTOS:
            response_data["tx"] = build_aptos_deposit_tx(
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
            )
        elif source_chain_type == CHAIN_TYPE_SOLANA:
            response_data["tx"] = build_solana_deposit_tx(
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
                decimals=int(token_in.get("decimals", 6)),
                deposit_memo=deposit_memo,
            )
        else:
            response_data["tx"] = {
                "depositAddress": deposit_address,
                "depositAmount": cross_tx.get("depositAmount", amount_in),
                "depositMemo": deposit_memo,
            }

        response_data["deposit"] = {
            "depositAddress": deposit_address,
            "depositMemo": deposit_memo,
            "depositChain": cross_tx.get("depositChain", str(from_chain)),
            "orderId": order_id,
            "estimatedOut": estimated_out,
            "minAmountOut": min_amount_out,
            "timeEstimate": cross_tx.get("timeEstimate", ""),
        }
        response_data["estimatedOut"] = estimated_out
        response_data["minAmountOut"] = min_amount_out

        # Deviation check against /quote results (if provided).
        quote_min_int = _safe_int_str(quote_min_amount_out)
        if quote_min_int > 0:
            build_est_int = _safe_int_str(estimated_out)
            if build_est_int > 0 and build_est_int < quote_min_int:
                return {
                    "code": -2,
                    "msg": "Price moved too much, please re-quote",
                    "data": {
                        **response_data,
                        "quoteExpectedOut": str(quote_expected_out or ""),
                        "quoteMinAmountOut": str(quote_min_amount_out or ""),
                        "buildEstimatedOut": str(estimated_out or ""),
                    },
                }

        return {"code": 0, "msg": "success", "data": response_data}
    except Exception as e:
        logger.error(f"_cross_chain_swap error: {e}")
        return {"code": -1, "msg": str(e)}
