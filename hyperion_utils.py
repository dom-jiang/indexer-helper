"""
Hyperion CLMM swap integration for Aptos.

Mirrors the frontend `@hyperionxyz/sdk` flow:
  1. GET /base/rate/getSwapInfo (flag=in)  — estToAmount
  2. Local payload build                   — swapTransactionPayload

Only CLMM routes are used (not Hyperion Aggregator), matching production frontend.
"""

from __future__ import annotations

import hashlib
import time
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger

from config import Cfg

HYPERION_ROUTER_NAME = "hyperion"
APTOS_COIN_TYPE = "0x1::aptos_coin::AptosCoin"
APTOS_FA_NATIVE = "0xa"
APTOS_FA_DECIMALS_CACHE_TTL_SECONDS = 24 * 60 * 60
_APTOS_FA_DECIMALS_LOCAL_CACHE: Dict[str, Tuple[int, float]] = {}

APTOS_NATIVE_ALIASES = frozenset({
    "",
    "0xa",
    "apt",
    "aptos",
    "0x1::aptos_coin::aptoscoin",
    "0x1::aptos_coin::AptosCoin",
})

_session = requests.Session()
_session.headers.update({"Content-Type": "application/json"})


def _hyperion_api_host() -> str:
    return (getattr(Cfg, "HYPERION_API_HOST", "") or "https://api.hyperion.xyz").rstrip("/")


def _hyperion_contract() -> str:
    return (
        getattr(Cfg, "HYPERION_CONTRACT", "")
        or "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c"
    ).strip()


def _aptos_rpc_url() -> str:
    return (getattr(Cfg, "APTOS_RPC_URL", "") or "https://fullnode.mainnet.aptoslabs.com/v1").rstrip("/")


def _aptos_fa_decimals_cache_key(addr: str) -> str:
    return f"SWAP:APTOS_FA_DECIMALS:{addr.lower()}"


def _get_cached_aptos_fa_decimals(addr: str) -> Optional[int]:
    key = addr.lower()
    now = time.time()
    local = _APTOS_FA_DECIMALS_LOCAL_CACHE.get(key)
    if local and local[1] > now:
        return local[0]
    try:
        from redis_provider import RedisProvider

        r = RedisProvider().r
        cached = r.get(_aptos_fa_decimals_cache_key(addr))
        if cached is None:
            return None
        val = int(cached)
        _APTOS_FA_DECIMALS_LOCAL_CACHE[key] = (
            val,
            now + APTOS_FA_DECIMALS_CACHE_TTL_SECONDS,
        )
        return val
    except Exception as e:
        logger.debug(f"Aptos FA decimals Redis cache read failed for {addr}: {e}")
        return None


def _set_cached_aptos_fa_decimals(addr: str, decimals: int) -> None:
    key = addr.lower()
    _APTOS_FA_DECIMALS_LOCAL_CACHE[key] = (
        int(decimals),
        time.time() + APTOS_FA_DECIMALS_CACHE_TTL_SECONDS,
    )
    try:
        from redis_provider import RedisProvider

        RedisProvider().r.setex(
            _aptos_fa_decimals_cache_key(addr),
            APTOS_FA_DECIMALS_CACHE_TTL_SECONDS,
            str(int(decimals)),
        )
    except Exception as e:
        logger.debug(f"Aptos FA decimals Redis cache write failed for {addr}: {e}")


def is_valid_aptos_fa_address(address: str) -> bool:
    raw = (address or "").strip()
    if not raw.startswith("0x"):
        return False
    hex_body = raw[2:]
    if not hex_body or len(hex_body) > 64:
        return False
    try:
        int(hex_body, 16)
    except ValueError:
        return False
    return True


def fetch_aptos_fa_decimals(metadata_address: str, timeout: float = 8.0) -> Optional[int]:
    """Read FA `decimals` via Aptos view (0x1::fungible_asset::decimals)."""
    addr = normalize_hyperion_token(metadata_address)
    if not is_valid_aptos_fa_address(addr) or is_coin_token(addr):
        return None
    cached = _get_cached_aptos_fa_decimals(addr)
    if cached is not None:
        return cached
    try:
        resp = _session.post(
            f"{_aptos_rpc_url()}/view",
            json={
                "function": "0x1::fungible_asset::decimals",
                "type_arguments": ["0x1::fungible_asset::Metadata"],
                "arguments": [addr],
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            decimals = int(data[0])
            _set_cached_aptos_fa_decimals(addr, decimals)
            return decimals
    except Exception as e:
        logger.debug(f"Aptos FA decimals lookup failed for {metadata_address}: {e}")
    return None


def resolve_aptos_token_info(address: str) -> Optional[Dict]:
    """
    Resolve Aptos token metadata for long-tail FA tokens not in Redis.

    Uses on-chain FA decimals; symbol falls back to a short address label.
    """
    raw = (address or "").strip()
    if not raw:
        return None
    if normalize_hyperion_token(raw) == APTOS_FA_NATIVE or is_coin_token(raw):
        return None

    if not is_valid_aptos_fa_address(raw):
        return None

    decimals = fetch_aptos_fa_decimals(raw)
    if decimals is None:
        return None

    short = raw[-8:] if len(raw) > 10 else raw
    return {
        "address": raw,
        "symbol": f"APTOS-{short}",
        "decimals": int(decimals),
    }


def is_coin_token(address: str) -> bool:
    return bool(address) and address.count("::") >= 2


def normalize_hyperion_token(address: str) -> str:
    """Normalize token id for Hyperion API + payload (native APT → 0xa)."""
    raw = (address or "").strip()
    if not raw:
        return APTOS_FA_NATIVE
    lower = raw.lower()
    if lower in APTOS_NATIVE_ALIASES or "aptoscoin" in lower:
        return APTOS_FA_NATIVE
    return raw


def _uleb128_encode(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _normalize_move_address(addr: str) -> str:
    s = (addr or "").strip()
    if not s.startswith("0x"):
        s = "0x" + s
    body = s[2:]
    if not body:
        body = "0"
    # Aptos addresses are 32-byte; left-pad hex body.
    padded = body.zfill(64)
    return "0x" + padded


def coin_type_to_fa(coin_type: str) -> str:
    """Map legacy Coin type path to FA metadata address (SDK faTypeCalculate)."""
    raw = (coin_type or "").strip()
    if not raw:
        return APTOS_FA_NATIVE
    if normalize_hyperion_token(raw) == APTOS_FA_NATIVE:
        return APTOS_FA_NATIVE

    parts = raw.split("::")
    if len(parts) < 3:
        return raw

    normalized_type = "::".join([_normalize_move_address(parts[0]), parts[1], parts[2]])
    type_bytes = normalized_type.encode("utf-8")
    seed = _uleb128_encode(len(type_bytes)) + type_bytes

    creator = bytes.fromhex("00" * 31 + "0a")  # AccountAddress.from("0xa")
    digest = hashlib.sha3_256(creator + seed + b"\xfe").digest()
    return "0x" + digest.hex()


def _fa_addresses_for_payload(currency_a: str, currency_b: str) -> Tuple[str, str]:
    a = normalize_hyperion_token(currency_a)
    b = normalize_hyperion_token(currency_b)
    out_a = coin_type_to_fa(currency_a) if is_coin_token(currency_a) else a
    out_b = coin_type_to_fa(currency_b) if is_coin_token(currency_b) else b
    return out_a, out_b


def slippage_decimal_to_hyperion_percent(slippage_decimal: float) -> float:
    """Hyperion SDK expects percent units (0.5 = 0.5%)."""
    try:
        return float(Decimal(str(slippage_decimal)) * Decimal("100"))
    except (InvalidOperation, ValueError):
        return 0.5


def hyperion_min_amount_out(amount_out: str, slippage_percent: float) -> int:
    """SDK slippageCalculator: out - out * slippage / 100."""
    try:
        out = Decimal(str(amount_out))
        slip = Decimal(str(slippage_percent))
        min_out = out - (out * slip / Decimal("100"))
        return int(min_out.to_integral_value(rounding=ROUND_DOWN))
    except (InvalidOperation, ValueError):
        return 0


def hyperion_est_to_amount(
    amount: str,
    token_in: str,
    token_out: str,
    safe_mode: bool = False,
    timeout: float = 12.0,
) -> Dict:
    """
    Estimate output for exact-in swap (GET getSwapInfo flag=in).

    Returns {"success": True, "amountOut": str, "path": list, "amountIn": str, "fee": str}
    or {"success": False, "error": str}.
    """
    try:
        params = {
            "amount": str(amount),
            "from": normalize_hyperion_token(token_in),
            "to": normalize_hyperion_token(token_out),
            "safeMode": "true" if safe_mode else "false",
            "flag": "in",
        }
        url = f"{_hyperion_api_host()}/base/rate/getSwapInfo"
        resp = _session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            return {"success": False, "error": "Hyperion getSwapInfo returned non-object"}

        amount_out = str(data.get("amountOut") or "")
        path = data.get("path")
        if not amount_out or not path:
            return {"success": False, "error": "Hyperion estToAmount missing amountOut/path", "data": data}

        return {
            "success": True,
            "amountOut": amount_out,
            "path": path,
            "amountIn": str(data.get("amountIn") or amount),
            "fee": str(data.get("fee") or ""),
            "raw": data,
        }
    except requests.RequestException as e:
        logger.warning(f"Hyperion getSwapInfo failed: {e}")
        return {"success": False, "error": f"Hyperion API error: {e}"}
    except Exception as e:
        logger.error(f"Hyperion est_to_amount error: {e}")
        return {"success": False, "error": str(e)}


def _select_swap_entry(
    currency_a: str,
    currency_b: str,
) -> Tuple[str, List[str]]:
    """
    TokenPairTypeCheck from aptos-tool — pick router_v3 entry function.

    Returns (function_suffix, type_arguments).
    """
    contract = _hyperion_contract()
    a_coin = is_coin_token(currency_a)
    b_coin = is_coin_token(currency_b)

    if not a_coin and not b_coin:
        return f"{contract}::router_v3::swap_batch", []
    if a_coin and b_coin:
        return f"{contract}::router_v3::swap_batch_coin_entry", [currency_a]
    if a_coin and not b_coin:
        return f"{contract}::router_v3::swap_batch_coin_entry", [currency_a]
    # FA in, coin out
    return f"{contract}::router_v3::swap_batch", []


def hyperion_build_swap_payload(
    currency_a: str,
    currency_b: str,
    amount_in: str,
    amount_out: str,
    slippage_percent: float,
    pool_route: Any,
    recipient: str,
) -> Dict:
    """
    Build wallet-ready Aptos entry payload (snake_case keys for unified API).

    Mirrors sdk.Swap.swapTransactionPayload().
    """
    if slippage_percent <= 0:
        slippage_percent = 0.5
    if slippage_percent > 20:
        raise ValueError("Hyperion slippage must be <= 20 (percent units)")

    fa_a, fa_b = _fa_addresses_for_payload(currency_a, currency_b)
    min_out = hyperion_min_amount_out(amount_out, slippage_percent)
    fn, type_args = _select_swap_entry(currency_a, currency_b)

    arguments = [
        pool_route,
        fa_a,
        fa_b,
        str(amount_in),
        str(min_out),
        recipient or "",
    ]

    return {
        "function": fn,
        "type_arguments": type_args,
        "arguments": arguments,
    }


def hyperion_quote(
    token_in: str,
    token_out: str,
    amount_in: str,
    slippage_decimal: float,
    safe_mode: bool = False,
) -> Dict:
    """Quote-only helper returning parsed amounts."""
    est = hyperion_est_to_amount(amount_in, token_in, token_out, safe_mode=safe_mode)
    if not est.get("success"):
        return est

    slip_pct = slippage_decimal_to_hyperion_percent(slippage_decimal)
    amount_out = str(est["amountOut"])
    min_out = str(hyperion_min_amount_out(amount_out, slip_pct))

    return {
        "success": True,
        "router": HYPERION_ROUTER_NAME,
        "amountOut": amount_out,
        "minAmountOut": min_out,
        "poolRoute": est.get("path"),
        "slippagePercent": slip_pct,
    }


def hyperion_build_swap_tx(
    token_in: str,
    token_out: str,
    amount_in: str,
    slippage_decimal: float,
    recipient: str,
    safe_mode: bool = False,
) -> Dict:
    """Quote + build in one call (used by same-chain build and preswap stage-A)."""
    est = hyperion_est_to_amount(amount_in, token_in, token_out, safe_mode=safe_mode)
    if not est.get("success"):
        return est

    slip_pct = slippage_decimal_to_hyperion_percent(slippage_decimal)
    amount_out = str(est["amountOut"])
    min_out = str(hyperion_min_amount_out(amount_out, slip_pct))

    try:
        tx = hyperion_build_swap_payload(
            currency_a=token_in,
            currency_b=token_out,
            amount_in=str(amount_in),
            amount_out=amount_out,
            slippage_percent=slip_pct,
            pool_route=est.get("path"),
            recipient=recipient,
        )
    except Exception as e:
        return {"success": False, "error": f"Hyperion payload build failed: {e}"}

    return {
        "success": True,
        "router": HYPERION_ROUTER_NAME,
        "tx": tx,
        "estimatedOut": amount_out,
        "minAmountOut": min_out,
        "poolRoute": est.get("path"),
    }
