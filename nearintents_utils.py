"""
NearIntents 1Click API integration for cross-chain swaps.

Flow:
  1. Quote (dry=true):  POST /v0/quote  -> price estimate, no deposit address
  2. Build  (dry=false): POST /v0/quote  -> returns depositAddress for user to send funds
  3. Status: GET  /v0/status?depositAddress=<addr>

Docs: https://docs.near-intents.org/api-reference/oneclick/request-a-swap-quote
"""

import json
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple
from loguru import logger
from config import Cfg
from redis_provider import get_1click_tokens_cache, set_1click_tokens_cache

_session = requests.Session()

CHAIN_TO_1CLICK = {
    # chain ids and common aliases => 1Click blockchain short name (from /v0/tokens)
    "1": "eth", "eth": "eth", "ethereum": "eth",
    "56": "bsc", "bsc": "bsc", "bnb": "bsc",
    "42161": "arb", "arb": "arb", "arbitrum": "arb",
    "8453": "base", "base": "base",
    "137": "pol", "pol": "pol", "polygon": "pol", "matic": "pol",
    "10": "op", "op": "op", "optimism": "op",
    "43114": "avax", "avax": "avax", "avalanche": "avax",
    "100": "gnosis", "gnosis": "gnosis",
    "534352": "scroll", "scroll": "scroll",
    "80094": "bera", "bera": "bera", "berachain": "bera",
    "196": "xlayer", "xlayer": "xlayer",
    "143": "monad", "monad": "monad",
    "9745": "plasma", "plasma": "plasma",
    # non-EVM
    "solana": "sol", "solana-mainnet": "sol", "501": "sol", "sol": "sol",
    "near": "near",
    "aptos": "aptos", "aptos-mainnet": "aptos", "637": "aptos",
    "sui": "sui", "784": "sui",
    "tron": "tron", "195": "tron",
    "ton": "ton",
    "btc": "btc", "bitcoin": "btc",
    "doge": "doge",
    "ltc": "ltc", "litecoin": "ltc",
    "bch": "bch",
    "zec": "zec", "zcash": "zec",
    "dash": "dash",
    "xrp": "xrp", "ripple": "xrp",
    "stellar": "stellar", "xlm": "stellar",
    "cardano": "cardano", "ada": "cardano",
    "starknet": "starknet",
    "aleo": "aleo",
    "adi": "adi",
}

CHAIN_TO_OMNI = {
    "1": "ethereum",
    "56": "bsc",
    "42161": "arbitrum",
    "8453": "base",
    "137": "polygon",
    "10": "optimism",
    "near": "near",
}

_token_list_cache = None
_token_lookup_cache = None


def _get_headers() -> Dict:
    headers = {"Content-Type": "application/json"}
    jwt = (Cfg.ONECLICK_JWT_TOKEN or "").strip()
    # Strip accidental "Bearer " / "bearer " prefix from the configured token
    # so we don't end up sending "Authorization: Bearer Bearer <jwt>".
    if jwt.lower().startswith("bearer "):
        jwt = jwt[7:].strip()
    if jwt:
        headers["Authorization"] = f"Bearer {jwt}"
    return headers


def _fetch_token_list() -> list:
    """Fetch full token list from 1Click /v0/tokens, with Redis caching."""
    global _token_list_cache, _token_lookup_cache

    cached = get_1click_tokens_cache()
    if cached:
        tokens = json.loads(cached)
        _token_list_cache = tokens
        _token_lookup_cache = None
        return tokens

    try:
        resp = _session.get(f"{Cfg.ONECLICK_BASE_URL}/tokens", timeout=15)
        resp.raise_for_status()
        tokens = resp.json()
        set_1click_tokens_cache(json.dumps(tokens), ttl=600)
        _token_list_cache = tokens
        _token_lookup_cache = None
        return tokens
    except Exception as e:
        logger.error(f"Failed to fetch 1Click token list: {e}")
        if _token_list_cache:
            return _token_list_cache
        return []


def _build_token_lookup() -> Dict[str, str]:
    """Build (blockchain_lower, contractAddress_lower) -> assetId lookup."""
    global _token_lookup_cache

    if _token_lookup_cache is not None:
        return _token_lookup_cache

    tokens = _fetch_token_list()
    lookup = {}
    for t in tokens:
        blockchain = (t.get("blockchain") or "").lower()
        asset_id = t.get("assetId", "")
        contract = (t.get("contractAddress") or "").lower()
        if blockchain and asset_id:
            if contract:
                lookup[(blockchain, contract)] = asset_id
            lookup[(blockchain, asset_id)] = asset_id

    _token_lookup_cache = lookup
    return lookup


def resolve_1click_asset_id(chain: str, address: str) -> Optional[str]:
    """
    Map (chainId, tokenAddress) to 1Click assetId.
    Returns None if no mapping found.
    """
    chain_str = str(chain)
    oneclick_chain = CHAIN_TO_1CLICK.get(chain_str, chain_str).lower()
    lookup = _build_token_lookup()
    addr_lower = address.lower()
    asset_id = lookup.get((oneclick_chain, addr_lower))
    if asset_id:
        return asset_id
    for key, val in lookup.items():
        if key[0] == oneclick_chain and addr_lower in key[1]:
            return val
    # Debug aid: record what we actually tried so operators can compare against the 1Click token list.
    logger.warning(
        f"1click assetId lookup miss: chain={chain_str} mapped_to={oneclick_chain} address={addr_lower}"
    )
    return None


def resolve_omni_chain(chain: str) -> Optional[str]:
    """Map chainId to OmniBridge chain slug."""
    return CHAIN_TO_OMNI.get(str(chain))


def nearintents_quote(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    sender: str,
    recipient: str = "",
    slippage: float = 0.5,
) -> Dict:
    """
    Get cross-chain quote from NearIntents 1Click API (dry run).
    amount_in is in smallest units.
    """
    origin_asset = resolve_1click_asset_id(from_chain, token_in.get("address", ""))
    dest_asset = resolve_1click_asset_id(to_chain, token_out.get("address", ""))

    if not origin_asset:
        return {"success": False, "error": f"NearIntents: unsupported source token {token_in.get('address', '')} on chain {from_chain}"}
    if not dest_asset:
        return {"success": False, "error": f"NearIntents: unsupported destination token {token_out.get('address', '')} on chain {to_chain}"}

    slippage_bps = int(slippage * 100) if slippage < 1 else int(slippage)
    if slippage_bps < 1:
        slippage_bps = 50

    deadline = (datetime.now(timezone.utc) + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    body = {
        "dry": True,
        "swapType": "EXACT_INPUT",
        "slippageTolerance": slippage_bps,
        "originAsset": origin_asset,
        "depositType": "ORIGIN_CHAIN",
        "destinationAsset": dest_asset,
        "amount": str(amount_in),
        "recipient": recipient or sender,
        "recipientType": "DESTINATION_CHAIN",
        "refundTo": sender,
        "refundType": "ORIGIN_CHAIN",
        "deadline": deadline,
    }

    try:
        resp = _session.post(
            f"{Cfg.ONECLICK_BASE_URL}/quote",
            json=body,
            headers=_get_headers(),
            timeout=20,
        )
        if resp.status_code >= 300:
            error_msg = resp.text[:500]
            return {"success": False, "error": f"NearIntents quote failed ({resp.status_code}): {error_msg}"}

        data = resp.json()
        quote = data.get("quote", {})

        return {
            "success": True,
            "router": "nearintents",
            "quote": {
                "router": "nearintents",
                "fromChain": from_chain,
                "toChain": to_chain,
                "tokenIn": token_in,
                "tokenOut": token_out,
                "amountIn": str(amount_in),
                "estimatedOut": quote.get("amountOut", ""),
                "estimatedOutFormatted": quote.get("amountOutFormatted", ""),
                "estimatedOutUsd": quote.get("amountOutUsd", ""),
                "minAmountOut": quote.get("minAmountOut", ""),
                "amountInUsd": quote.get("amountInUsd", ""),
                "timeEstimate": quote.get("timeEstimate", ""),
                "sender": sender,
                "recipient": recipient or sender,
            },
            "raw": data,
        }
    except Exception as e:
        logger.error(f"nearintents_quote error: {e}")
        return {"success": False, "error": f"NearIntents quote error: {str(e)}"}


def nearintents_build_tx(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    sender: str,
    recipient: str = "",
    slippage: float = 0.5,
) -> Dict:
    """
    Build cross-chain swap via NearIntents 1Click API (dry=false).
    Returns depositAddress for the user to send funds to.
    """
    origin_asset = resolve_1click_asset_id(from_chain, token_in.get("address", ""))
    dest_asset = resolve_1click_asset_id(to_chain, token_out.get("address", ""))

    if not origin_asset:
        return {"success": False, "error": f"NearIntents: unsupported source token {token_in.get('address', '')} on chain {from_chain}"}
    if not dest_asset:
        return {"success": False, "error": f"NearIntents: unsupported destination token {token_out.get('address', '')} on chain {to_chain}"}

    slippage_bps = int(slippage * 100) if slippage < 1 else int(slippage)
    if slippage_bps < 1:
        slippage_bps = 50

    deadline = (datetime.now(timezone.utc) + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    body = {
        "dry": False,
        "swapType": "EXACT_INPUT",
        "slippageTolerance": slippage_bps,
        "originAsset": origin_asset,
        "depositType": "ORIGIN_CHAIN",
        "destinationAsset": dest_asset,
        "amount": str(amount_in),
        "recipient": recipient or sender,
        "recipientType": "DESTINATION_CHAIN",
        "refundTo": sender,
        "refundType": "ORIGIN_CHAIN",
        "deadline": deadline,
    }

    try:
        resp = _session.post(
            f"{Cfg.ONECLICK_BASE_URL}/quote",
            json=body,
            headers=_get_headers(),
            timeout=20,
        )
        if resp.status_code >= 300:
            error_msg = resp.text[:500]
            return {"success": False, "error": f"NearIntents build failed ({resp.status_code}): {error_msg}"}

        data = resp.json()
        quote = data.get("quote", {})

        deposit_address = quote.get("depositAddress", "")
        deposit_memo = quote.get("depositMemo", "")

        return {
            "success": True,
            "chainType": "cross-chain",
            "router": "nearintents",
            "tx": {
                "depositAddress": deposit_address,
                "depositMemo": deposit_memo,
                "depositAmount": quote.get("amountInFormatted", ""),
                "depositChain": from_chain,
                "orderId": deposit_address,
                "estimatedOut": quote.get("amountOut", ""),
                "minAmountOut": quote.get("minAmountOut", ""),
                "timeEstimate": quote.get("timeEstimate", ""),
            },
        }
    except Exception as e:
        logger.error(f"nearintents_build_tx error: {e}")
        return {"success": False, "error": f"NearIntents build error: {str(e)}"}


def nearintents_order_status(deposit_address: str) -> Dict:
    """Query NearIntents 1Click swap status by deposit address."""
    try:
        resp = _session.get(
            f"{Cfg.ONECLICK_BASE_URL}/status",
            params={"depositAddress": deposit_address},
            headers=_get_headers(),
            timeout=10,
        )
        if resp.status_code != 200:
            return {"success": False, "error": f"NearIntents status query failed ({resp.status_code})"}
        return {"success": True, "data": resp.json()}
    except Exception as e:
        logger.error(f"nearintents_order_status error: {e}")
        return {"success": False, "error": str(e)}
