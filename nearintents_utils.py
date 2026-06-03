"""
NearIntents 1Click API integration for cross-chain swaps.

Flow:
  1. Quote (dry=true):  POST /v0/quote  -> price estimate, no deposit address
  2. Build  (dry=false): POST /v0/quote  -> returns depositAddress for user to send funds
  3. Status: GET  /v0/status?depositAddress=<addr>

Docs: https://docs.near-intents.org/api-reference/oneclick/request-a-swap-quote
"""

import json
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from loguru import logger
from config import Cfg
from redis_provider import get_1click_tokens_cache, set_1click_tokens_cache
from swap_utils import is_native_token

# Sentinel used in our (chain, addr) lookup table to mark the native gas token
# of a chain. 1Click's `/v0/tokens` returns `contractAddress=null` for native
# tokens (e.g. ETH on op / base / arb), so we cannot index them by on-chain
# address. Callers pass the native token as empty string / `0x000...0` /
# the OKX sentinel `0xEeee...`; all of those collapse to this key via
# `is_native_token`.
_NATIVE_LOOKUP_KEY = "__native__"

# Chain-specific "wrapped native" marker addresses that aggregators (Jupiter on
# Solana, etc.) use as a stand-in for the chain's gas token. 1Click stores the
# gas token with `contractAddress=null`, so we collapse these marker addresses
# onto `_NATIVE_LOOKUP_KEY` at resolve time. Keys are 1Click blockchain short
# names (the values of `CHAIN_TO_1CLICK`); address values are lowercased.
_CHAIN_WRAPPED_NATIVE_MARKERS = {
    # Solana: Jupiter / OKX treat wSOL mint as native SOL. 1Click lists native
    # SOL with contractAddress=null under assetId `nep141:sol.omft.near`.
    "sol": {"so11111111111111111111111111111111111111112"},
    # Aptos: native APT is conventionally addressed as `0xa` (Aptos bluechip),
    # the legacy Move type path `0x1::aptos_coin::AptosCoin`, or the alias
    # "apt". 1Click lists native APT with contractAddress=null under assetId
    # `nep141:aptos.omft.near`, so we collapse all of these onto the native
    # lookup sentinel — keeping a single tokenIn/tokenOut value usable across
    # same-chain (Hyperion) and cross-chain (1Click) flows.
    "aptos": {"0xa", "0x1::aptos_coin::aptoscoin", "apt"},
    # NEAR: unlike Solana / Aptos, 1Click does NOT list a `contractAddress=null`
    # native NEAR entry — the canonical NEAR-side bridge asset is wNEAR. We
    # still mark `wrap.near` (and the empty-string / symbolic aliases) as
    # "native" so the deposit-tx builder can branch on it, but the lookup in
    # `resolve_1click_asset_id` will fall through to the regular contract
    # lookup when the native sentinel key is absent (see comment there).
    "near": {"wrap.near", "near", "wnear"},
    # SUI: Move type path `0x2::sui::SUI` is the canonical on-chain identifier
    # for native SUI (the wallet adapters all expect this exact form for coin
    # objects). 1Click lists native SUI with contractAddress=null under assetId
    # `nep141:sui.omft.near`, so we collapse the type path and the "sui" alias
    # onto the native lookup sentinel.
    "sui": {"0x2::sui::sui", "sui"},
    # TRON: native TRX has no contract address. 1Click lists it with
    # contractAddress=null under assetId `nep141:tron.omft.near`. Frontends
    # sometimes pass the symbol "trx" instead of the empty string, so we
    # accept both.
    "tron": {"trx", "tron"},
    # UTXO chains (BTC / ZEC / LTC / DOGE / BCH / DASH): the chain has no
    # smart contracts at all, so the native coin is the only asset 1Click
    # lists, again with contractAddress=null. We accept the chain symbol as
    # a marker so frontends can pass either "" or "btc" / "zec" / ... and
    # both resolve to the right 1Click assetId.
    "btc": {"btc", "bitcoin"},
    "zec": {"zec", "zcash"},
    "ltc": {"ltc", "litecoin"},
    "doge": {"doge", "dogecoin"},
    "bch": {"bch"},
    "dash": {"dash"},
}

_session = requests.Session()

# Optional keys merged into 1Click POST /v0/quote bodies (after defaults).
_ONECLICK_QUOTE_OPTIONAL_KEYS = frozenset({
    "customRecipientMsg",
    "appFees",
    "referral",
    "quoteWaitingTimeMs",
    "refundTo",
    "refundType",
    "recipient",
    "recipientType",
    "depositType",
    "swapType",
    "deadline",
})


def _apply_default_intents_app_fees_and_referral(body: Dict[str, Any]) -> None:
    """
    Apply INTENTS_APP_FEES_* / INTENTS_DEFAULT_REFERRAL when not already on the 1Click body.
    Fee is basis points (100 = 1%). Called for every POST /v0/quote from this module.
    """
    fees = body.get("appFees") or body.get("app_fees")
    if not (isinstance(fees, list) and len(fees) > 0):
        recipient = (getattr(Cfg, "INTENTS_APP_FEES_RECIPIENT", "") or "").strip()
        fee_val = getattr(Cfg, "INTENTS_APP_FEES", None)
        if recipient and fee_val is not None:
            try:
                fee_int = int(fee_val)
            except (TypeError, ValueError):
                fee_int = 2
            body["appFees"] = [{"recipient": recipient, "fee": fee_int}]
    if not body.get("referral"):
        referral = (getattr(Cfg, "INTENTS_DEFAULT_REFERRAL", "") or "").strip()
        if referral:
            body["referral"] = referral


def merge_oneclick_quote_extensions(
    body: Dict[str, Any],
    oneclick_extensions: Optional[Dict[str, Any]],
) -> None:
    """Merge whitelisted NearIntents 1Click fields into an outgoing quote body (mutates `body`)."""
    if oneclick_extensions and isinstance(oneclick_extensions, dict):
        for key, val in oneclick_extensions.items():
            if key in _ONECLICK_QUOTE_OPTIONAL_KEYS and val is not None:
                body[key] = val
    _apply_default_intents_app_fees_and_referral(body)


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
    "80094": "bera", "bera": "bera", "berachain": "bera", "1385": "bera",
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

# In-memory caches for the 1Click `/v0/tokens` payload.
# `_token_list_cache` is the last successfully-fetched raw list — used as a
# last-resort fallback when both Redis and the upstream API are unavailable.
# `_token_lookup_cache` is the derived (blockchain, addr) -> assetId dict.
#
# IMPORTANT: previously `_token_lookup_cache` was only built once per process
# and never invalidated, which meant a long-running backend would never pick
# up tokens that 1Click added after startup (the 600s Redis TTL was useless
# because `_build_token_lookup` short-circuited before touching Redis).
# `_TOKEN_LOOKUP_TTL_SECONDS` bounds the in-memory cache age so refreshes do
# happen, while still amortising the (potentially thousands of entries)
# rebuild cost across many quote requests.
_token_list_cache = None
_token_lookup_cache = None
_token_lookup_built_at = 0.0
_TOKEN_LIST_REDIS_TTL_SECONDS = 3600
_TOKEN_LOOKUP_TTL_SECONDS = 3600


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


def _oneclick_url(path: str) -> str:
    """Build a fully-qualified 1Click API URL from `Cfg.ONECLICK_BASE_URL`
    and a leading path segment (e.g. "tokens", "quote", "status").

    Defensive against common deploy-time misconfigurations of the base URL:
      * trailing slashes are stripped.
      * a trailing `/v0` or `/v0/` is stripped so the version segment is not
        duplicated. This lets operators write either of
            ONECLICK_BASE_URL = "https://1click.chaindefuser.com"
            ONECLICK_BASE_URL = "https://1click.chaindefuser.com/v0"
        without the call sites silently 404-ing.

    Always returns `<host>/v0/<path>`. If `Cfg.ONECLICK_BASE_URL` is empty,
    falls back to the production host so the backend keeps working in
    smoke-test environments where `db_info.py` was never set up.
    """
    base = (Cfg.ONECLICK_BASE_URL or "").strip().rstrip("/")
    if not base:
        base = "https://1click.chaindefuser.com"
    elif base.lower().endswith("/v0"):
        base = base[: -len("/v0")]
    return f"{base}/v0/{path.lstrip('/')}"


def _fetch_token_list() -> list:
    """Fetch full token list from 1Click /v0/tokens, with Redis caching."""
    global _token_list_cache

    cached = get_1click_tokens_cache()
    if cached:
        try:
            tokens = json.loads(cached)
            _token_list_cache = tokens
            return tokens
        except Exception as e:
            logger.warning(f"1Click tokens Redis cache decode failed, refetching: {e}")

    try:
        resp = _session.get(_oneclick_url("tokens"), timeout=15)
        resp.raise_for_status()
        tokens = resp.json()
        set_1click_tokens_cache(json.dumps(tokens), ttl=_TOKEN_LIST_REDIS_TTL_SECONDS)
        _token_list_cache = tokens
        return tokens
    except Exception as e:
        logger.error(f"Failed to fetch 1Click token list: {e}")
        if _token_list_cache:
            return _token_list_cache
        return []


def _build_token_lookup() -> Dict[str, str]:
    """Build (blockchain_lower, contractAddress_lower) -> assetId lookup.

    The derived lookup dict is cached in-process for `_TOKEN_LOOKUP_TTL_SECONDS`
    to avoid re-parsing the (thousands-of-entries) token list on every quote.
    Once the window elapses the next call refetches via `_fetch_token_list`
    (Redis-warm in steady state, otherwise hitting 1Click directly) and
    rebuilds the dict, so newly-listed tokens become resolvable without a
    process restart.
    """
    global _token_lookup_cache, _token_lookup_built_at

    now = time.monotonic()
    if (
        _token_lookup_cache is not None
        and now - _token_lookup_built_at < _TOKEN_LOOKUP_TTL_SECONDS
    ):
        return _token_lookup_cache

    tokens = _fetch_token_list()
    lookup: Dict = {}
    for t in tokens:
        blockchain = (t.get("blockchain") or "").lower()
        asset_id = t.get("assetId", "")
        contract = (t.get("contractAddress") or "").lower()
        if blockchain and asset_id:
            if contract:
                lookup[(blockchain, contract)] = asset_id
            else:
                # Native gas token on this blockchain (1Click leaves
                # contractAddress null). Index it under a sentinel key so
                # callers passing `0x000...0` / empty / OKX's `0xEeee...`
                # still resolve to the correct assetId.
                lookup.setdefault((blockchain, _NATIVE_LOOKUP_KEY), asset_id)
            lookup[(blockchain, asset_id)] = asset_id

    if not lookup and _token_lookup_cache:
        # Upstream returned empty (e.g. network blip): keep serving the
        # previous lookup but expire it sooner so we retry quickly instead
        # of pinning a broken empty map for the full TTL.
        _token_lookup_built_at = now - _TOKEN_LOOKUP_TTL_SECONDS + 30
        return _token_lookup_cache

    _token_lookup_cache = lookup
    _token_lookup_built_at = now
    return lookup


def _is_chain_native_address(oneclick_chain: str, addr_lower: str) -> bool:
    """Return True if `addr_lower` is a chain-specific wrapped-native marker
    (e.g. wSOL mint on Solana) that should be treated as the native gas token.
    """
    if not addr_lower:
        return False
    markers = _CHAIN_WRAPPED_NATIVE_MARKERS.get(oneclick_chain)
    return bool(markers and addr_lower in markers)


def is_chain_native_token(chain, address: str) -> bool:
    """Return True iff `address` represents the native gas token of `chain`.

    Considers both:
      * Generic EVM conventions handled by `swap_utils.is_native_token`
        (empty string, `0x000...0`, OKX sentinel `0xEeee...`).
      * Chain-specific markers (e.g. wSOL mint on Solana, `0xa` /
        `0x1::aptos_coin::AptosCoin` / `apt` on Aptos) declared in
        `_CHAIN_WRAPPED_NATIVE_MARKERS`.

    Accepts any chain identifier that `CHAIN_TO_1CLICK` knows about
    (numeric ids, short names, aliases). Used by token-info resolvers so
    the frontend can pass the same native-token marker across same-chain
    (Hyperion / Jupiter / OKX) and cross-chain (1Click) flows.
    """
    addr_raw = address or ""
    if is_native_token(addr_raw):
        return True
    if not addr_raw:
        return False
    chain_str = str(chain) if chain is not None else ""
    oneclick_chain = CHAIN_TO_1CLICK.get(chain_str, chain_str).lower()
    return _is_chain_native_address(oneclick_chain, addr_raw.lower())


def resolve_1click_asset_id(chain: str, address: str) -> Optional[str]:
    """
    Map (chainId, tokenAddress) to 1Click assetId.
    Returns None if no mapping found.

    Native gas tokens are looked up via the `_NATIVE_LOOKUP_KEY` sentinel
    because 1Click's token list records them with `contractAddress=null`,
    not a zero address. We treat the following as "native":
      * EVM conventions handled by `is_native_token`: empty string,
        `0x000...0`, and the OKX sentinel `0xEeee...`.
      * Chain-specific wrapped-native markers declared in
        `_CHAIN_WRAPPED_NATIVE_MARKERS` (e.g. wSOL mint on Solana, `0xa` on
        Aptos, `wrap.near` on NEAR), so the frontend can use the same
        address across same-chain (Jupiter / OKX / Hyperion) and cross-chain
        (1Click) flows without branching.

    Fall-through behaviour for chains whose "native" marker is itself a real
    contract address: NEAR is the canonical example — 1Click has no
    `contractAddress=null` entry for it; native NEAR is just wNEAR
    (`wrap.near`). For these chains the native-sentinel lookup will miss
    and we fall through to the regular contract lookup below, which finds
    the wNEAR entry by its actual contractAddress. For Solana / Aptos the
    native sentinel hits and we return immediately as before.
    """
    chain_str = str(chain)
    oneclick_chain = CHAIN_TO_1CLICK.get(chain_str, chain_str).lower()
    lookup = _build_token_lookup()
    addr_lower = (address or "").lower()

    is_native_marker = (
        is_native_token(address or "")
        or _is_chain_native_address(oneclick_chain, addr_lower)
    )
    if is_native_marker:
        native_id = lookup.get((oneclick_chain, _NATIVE_LOOKUP_KEY))
        if native_id:
            return native_id
        # No `contractAddress=null` entry on this chain. If the caller passed
        # one of the symbolic aliases (`near`, `wnear`, empty string) we have
        # nothing concrete to look up below, so give up here. If they passed
        # a real account ID (e.g. `wrap.near`), `addr_lower` is truthy and we
        # let the contract lookup below try to resolve it.
        if not addr_lower or is_native_token(address or ""):
            logger.warning(
                f"1click assetId lookup miss (native, no fallback): chain={chain_str} "
                f"mapped_to={oneclick_chain} address={address}"
            )
            return None

    asset_id = lookup.get((oneclick_chain, addr_lower))
    if asset_id:
        return asset_id
    for key, val in lookup.items():
        if key[0] == oneclick_chain and addr_lower and addr_lower in key[1]:
            return val
    # Debug aid: record what we actually tried so operators can compare against the 1Click token list.
    logger.warning(
        f"1click assetId lookup miss: chain={chain_str} mapped_to={oneclick_chain} address={addr_lower}"
    )
    return None


def resolve_1click_token_info(chain: str, address: str) -> Optional[Dict]:
    """Map (chain, contractAddress) to {address, symbol, decimals} from 1Click /v0/tokens."""
    chain_str = str(chain)
    oneclick_chain = CHAIN_TO_1CLICK.get(chain_str, chain_str).lower()
    addr_raw = address or ""
    addr_lower = addr_raw.lower()

    tokens = _fetch_token_list()
    for t in tokens:
        if (t.get("blockchain") or "").lower() != oneclick_chain:
            continue
        contract = (t.get("contractAddress") or "").lower()
        if contract and contract == addr_lower:
            return {
                "address": addr_raw,
                "symbol": str(t.get("symbol") or ""),
                "decimals": int(t.get("decimals") or 8),
            }
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
    oneclick_extensions: Optional[Dict[str, Any]] = None,
    swap_type: Optional[str] = None,
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
        "swapType": (swap_type or "EXACT_INPUT").strip().upper(),
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
    merge_oneclick_quote_extensions(body, oneclick_extensions)

    try:
        resp = _session.post(
            _oneclick_url("quote"),
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
    oneclick_extensions: Optional[Dict[str, Any]] = None,
    swap_type: Optional[str] = None,
) -> Dict:
    """
    Build cross-chain swap via NearIntents 1Click API (dry=false).
    Returns depositAddress for the user to send funds to.

    ``swap_type`` defaults to ``EXACT_INPUT`` (direct 1Click bridge). The
    two-stage pre-swap route passes ``FLEX_INPUT`` because Stage A delivers a
    variable on-chain amount to the deposit address, so the bridge must accept
    whatever actually arrives rather than a fixed input.
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
        "swapType": (swap_type or "EXACT_INPUT").strip().upper(),
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
    merge_oneclick_quote_extensions(body, oneclick_extensions)

    try:
        resp = _session.post(
            _oneclick_url("quote"),
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
            _oneclick_url("status"),
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
