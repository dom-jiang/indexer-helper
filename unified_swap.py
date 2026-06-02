"""
Unified Swap API dispatch layer.

Routes requests to same-chain or cross-chain handlers based on fromChain vs toChain.
For cross-chain: runs OmniBridge and NearIntents 1Click in parallel, picks best price.
For same-chain:  delegates to existing multi_chain_* functions.
"""

import base64
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple

import requests
from loguru import logger
from config import Cfg
from redis_provider import get_chain_tokens_with_prices
from swap_utils import (
    multi_chain_quote, multi_chain_build_tx, multi_chain_approve_tx,
    detect_chain_type, shrink_token, convert_slippage_to_decimal,
    SOLANA_CHAIN_IDS, APTOS_CHAIN_IDS, NEAR_CHAIN_IDS,
    SUI_CHAIN_IDS, TRON_CHAIN_IDS, UTXO_CHAIN_IDS,
    CHAIN_TYPE_EVM, CHAIN_TYPE_SOLANA, CHAIN_TYPE_APTOS, CHAIN_TYPE_NEAR,
    CHAIN_TYPE_SUI, CHAIN_TYPE_TRON, CHAIN_TYPE_UTXO,
    BLUECHIP_TOKENS, SOLANA_BLUECHIP_TOKENS, APTOS_BLUECHIP_TOKENS,
    is_native_token, normalize_evm_address,
    okx_quote_exact_out, build_okx_exact_out_swap_tx,
    build_swap_tx as build_same_chain_swap_tx,
    build_approve_tx as build_same_chain_approve_tx,
    simulate_preswap_evm_swap,
    convert_preswap_slippage_to_decimal,
    get_bluechip_tokens,
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
    is_chain_native_token,
)
from cross_chain_tx_builder import (
    build_evm_deposit_tx, build_aptos_deposit_tx, build_solana_deposit_tx,
    build_near_deposit_tx, NEAR_NATIVE_ALIASES,
    build_sui_deposit_tx, build_tron_deposit_tx, build_utxo_deposit_tx,
    SUI_NATIVE_TYPE,
)
from db_provider import add_multichain_lending_requests, insert_swap_transaction
from near_same_chain_mca import (
    near_same_chain_mca_deposit_intents_applies,
    near_same_chain_mca_withdraw_applies,
    near_same_chain_mca_withdraw_intents_applies,
    resolve_near_mca_deposit_receiver,
)
from near_mca_withdraw_tx import build_near_mca_withdraw_exec_tx_payload
from mca_withdraw_cross_intents import nep141_ft_transfer_amount_minus_one


def broadcast_near_signed_transaction(network_id: str, signed_tx_base64: str) -> Dict[str, Any]:
    """
    Broadcast a NEAR signed transaction (serialized SignedTransaction bytes, standard base64).

    Same payload NEAR wallets use with JSON-RPC `broadcast_tx_commit`.
    """
    oid = str(network_id or "").strip().upper() or str(getattr(Cfg, "NETWORK_ID", "") or "").strip().upper()
    raw_b64 = (signed_tx_base64 or "").strip()
    if not raw_b64:
        return {"code": -1, "msg": "signedTx must be non-empty base64", "data": None}

    try:
        base64.b64decode(raw_b64)
    except Exception as e:
        return {"code": -1, "msg": f"invalid base64: {e}", "data": None}

    urls = Cfg.NETWORK.get(oid, {}).get("NEAR_RPC_URL") or []
    if isinstance(urls, str):
        urls = [urls]
    urls = [str(u) for u in urls if u][:5]
    if not urls:
        return {"code": -1, "msg": "NEAR_RPC_URL not configured", "data": None}

    payload = {
        "jsonrpc": "2.0",
        "id": "swap-near-broadcast",
        "method": "broadcast_tx_commit",
        "params": [raw_b64],
    }
    last_err = None
    for url in urls:
        try:
            r = requests.post(url, json=payload, timeout=90)
            r.raise_for_status()
            j = r.json() if r.content else {}
            if j.get("error"):
                last_err = j["error"]
                continue
            return {"code": 0, "msg": "success", "data": {"result": j.get("result")}}
        except Exception as e:
            last_err = str(e)
            logger.warning(f"broadcast_near_signed_transaction node={url} err={e}")
    detail = last_err if last_err else "broadcast failed"
    return {"code": -1, "msg": str(detail), "data": None}


def _assemble_near_mca_withdraw_tx(
    mca_block: Dict,
    token_in: Dict,
    amount_in: str,
    recipient: str,
) -> Dict[str, Any]:
    """
    One NEAR tx: signer calls MCA `exec` with Burrow withdraw + token transfer to recipient.
    Aligns with src/services/lending/actions/withdraw.ts (near → near, non-Pyth).
    """
    if not mca_block or not isinstance(mca_block, dict):
        raise ValueError("mca block missing")
    mca_id = str(
        mca_block.get("mcaAccountId") or mca_block.get("mca_id") or ""
    ).strip()
    if not mca_id:
        raise ValueError("mca.mcaAccountId is required")

    exec_signer = str(
        mca_block.get("execSignerAccountId")
        or mca_block.get("exec_signer_near")
        or mca_block.get("nearSignerAccountId")
        or recipient
        or ""
    ).strip()
    if not exec_signer:
        raise ValueError(
            "Set recipient (NEAR receiving account) or mca.execSignerAccountId to the NEAR wallet that signs MCA exec"
        )

    rec = str(recipient or "").strip()
    if not rec:
        raise ValueError("recipient is required (NEAR account that receives withdrawn tokens)")

    tid = str((token_in or {}).get("address") or "").strip()
    if not tid:
        raise ValueError("tokenIn address required")

    from mca_burrow_auto import resolve_mca_withdraw_burrow_inner_amount

    amt_br, br_err = resolve_mca_withdraw_burrow_inner_amount(
        network_id=str(Cfg.NETWORK_ID),
        token_id=tid,
        amount_token_smallest=str(amount_in),
        mca_block=mca_block,
    )
    if br_err or not amt_br:
        raise ValueError(
            br_err
            or "could not resolve mca.amountBurrow (pass explicitly or rely on Burrow get_asset + amountIn)"
        )

    amt_ft = nep141_ft_transfer_amount_minus_one(str(amount_in))
    return build_near_mca_withdraw_exec_tx_payload(
        network_id=Cfg.NETWORK_ID,
        mca_account_id=mca_id,
        token_id=tid,
        amount_token_smallest=str(amt_ft),
        amount_burrow=amt_br,
        recipient_near=rec,
        exec_signer_near=exec_signer,
    )


# Same-chain NEAR wallet <-> Lending (no 1Click bridge leg)
ROUTER_NEAR_MCA_DEPOSIT = "near-mca-deposit"
ROUTER_NEAR_MCA_WITHDRAW = "near-mca-withdraw"

# Bluechip / common tokens on NEAR. These mirror the Solana / Aptos static
# tables so `_resolve_token_info` can short-circuit without depending on the
# multichain token-price Redis cache being warm for the `near` chain key.
# Addresses are taken verbatim from 1Click's `/v0/tokens` `contractAddress`
# field — they are NEAR account IDs (NEP-141 contracts), NOT 0x hex.
_NEAR_BLUECHIP_TOKENS = {
    "NEAR":   {"address": "wrap.near", "symbol": "NEAR", "decimals": 24},
    "wNEAR":  {"address": "wrap.near", "symbol": "wNEAR", "decimals": 24},
    "USDT":   {"address": "usdt.tether-token.near", "symbol": "USDT", "decimals": 6},
    "USDC":   {"address": "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1", "symbol": "USDC", "decimals": 6},
    "ETH":    {"address": "eth.bridge.near", "symbol": "ETH", "decimals": 18},
    "BTC":    {"address": "nbtc.bridge.near", "symbol": "BTC", "decimals": 8},
    "wBTC":   {"address": "2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near", "symbol": "wBTC", "decimals": 8},
    "AURORA": {"address": "aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near", "symbol": "AURORA", "decimals": 18},
    "FRAX":   {"address": "853d955acef822db058eb8505911ed77f175b99e.factory.bridge.near", "symbol": "FRAX", "decimals": 18},
}
_NEAR_BLUECHIP_LOOKUP = {
    v["address"].lower(): v for v in _NEAR_BLUECHIP_TOKENS.values()
}


# SUI bluechip tokens. Same idea as `_NEAR_BLUECHIP_TOKENS` — let
# `_resolve_token_info` short-circuit for common assets when the multichain
# token-price Redis cache is cold for the `sui` chain key. Addresses are SUI
# Move type tags taken from 1Click's `/v0/tokens` for `blockchain=sui`. The
# native SUI entry uses the canonical `0x2::sui::SUI` so the frontend can use
# the same address across same-chain (future Cetus integration) and
# cross-chain (1Click) flows; native detection in
# `nearintents_utils.is_chain_native_token` handles this case explicitly.
_SUI_BLUECHIP_TOKENS = {
    "SUI":  {"address": "0x2::sui::SUI", "symbol": "SUI", "decimals": 9},
    "USDC": {
        "address": "0xdba34672e30cb065b1f93e3ab55318768fd6fef66c15942c9f7cb846e2f900e7::usdc::USDC",
        "symbol": "USDC",
        "decimals": 6,
    },
}
_SUI_BLUECHIP_LOOKUP = {
    v["address"].lower(): v for v in _SUI_BLUECHIP_TOKENS.values()
}


# TRON bluechip tokens. TRC20 addresses are base58check (case-sensitive); we
# store the original form here and normalize to lowercase when building the
# lookup map (mirrors NEAR / SUI). 1Click only lists TRX and USDT on TRON
# today; we include both so cross-chain TRX/USDT -> EVM flows resolve
# without depending on Redis.
_TRON_BLUECHIP_TOKENS = {
    "TRX":  {"address": "", "symbol": "TRX", "decimals": 6},
    "USDT": {
        "address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
        "symbol": "USDT",
        "decimals": 6,
    },
}
_TRON_BLUECHIP_LOOKUP = {
    v["address"].lower(): v for v in _TRON_BLUECHIP_TOKENS.values() if v.get("address")
}

_APTOS_BLUECHIP_LOOKUP = {
    v["address"].lower(): v for v in APTOS_BLUECHIP_TOKENS.values()
}

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
    "1385": ["80094", "bera"],  # legacy/mis-encoded Berachain id (real id is 80094)
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


def _resolve_cfg_near_token(addr_raw: str) -> Optional[Dict]:
    """Resolve Ref / bridge NEP-141 ids from ``Cfg.TOKENS`` (long-tail DEX tokens)."""
    if not addr_raw:
        return None
    try:
        from config import Cfg

        network_id = getattr(Cfg, "NETWORK_ID", None) or "MAINNET"
        token_rows = (getattr(Cfg, "TOKENS", None) or {}).get(network_id) or []
    except Exception:
        return None
    addr_lower = addr_raw.lower()
    for row in token_rows:
        if not isinstance(row, dict):
            continue
        near_id = (row.get("NEAR_ID") or "").strip()
        if near_id and near_id.lower() == addr_lower:
            return {
                "address": addr_raw,
                "symbol": str(row.get("SYMBOL") or ""),
                "decimals": int(row.get("DECIMAL") or 18),
            }
    return None


def _token_in_supported_by_1click(from_chain: str, token_address: str) -> bool:
    """True when 1Click lists ``token_address`` as an origin asset on ``from_chain``."""
    return bool(resolve_1click_asset_id(from_chain, token_address or ""))


def _resolve_token_info(chain: str, address: str) -> Optional[Dict]:
    """
    Look up token metadata (symbol, decimals) from Redis multichain token data.
    Tries chain-key aliases in order and returns dict with address/symbol/decimals, or None.

    Native gas tokens are short-circuited to a static mapping because the
    Redis token cache is keyed by contract address and does not store them.
    "Native" here covers both:
      * Generic EVM conventions: empty string, `0x000...0`, OKX sentinel
        `0xEeee...`.
      * Chain-specific markers used by same-chain aggregators: wSOL mint on
        Solana, `0xa` / `0x1::aptos_coin::AptosCoin` / `apt` on Aptos,
        `wrap.near` / `near` / `wnear` on NEAR, etc. See
        `nearintents_utils.is_chain_native_token` for the full list.

    For NEAR specifically, an additional static bluechip-token table
    (`_NEAR_BLUECHIP_TOKENS`) is consulted before the Redis lookup so that
    common tokens (wNEAR / USDT / USDC / ETH / BTC / wBTC / AURORA / FRAX)
    resolve even when the multichain token-price job has not warmed up the
    `near` chain key in Redis. This keeps cross-chain quotes working
    without depending on a separate data pipeline.
    """
    addr_raw = address or ""
    if is_chain_native_token(chain, addr_raw):
        meta = _resolve_native_token_meta(chain)
        # Preserve the original address string the frontend passed so downstream
        # code (OKX / Bitget / Jupiter / Hyperion adapters) can re-detect native via
        # their own native-marker checks.
        return {
            "address": addr_raw,
            "symbol": meta["symbol"],
            "decimals": int(meta["decimals"]),
        }

    addr_lower = addr_raw.lower()

    # NEAR bluechip short-circuit (see docstring above).
    if str(chain).lower() in NEAR_CHAIN_IDS:
        bluechip = _NEAR_BLUECHIP_LOOKUP.get(addr_lower)
        if bluechip:
            return {
                "address": addr_raw,
                "symbol": bluechip["symbol"],
                "decimals": int(bluechip["decimals"]),
            }
        cfg_near = _resolve_cfg_near_token(addr_raw)
        if cfg_near:
            return cfg_near
        from nearintents_utils import resolve_1click_token_info

        oneclick_near = resolve_1click_token_info(chain, addr_raw)
        if oneclick_near:
            return oneclick_near

    # SUI bluechip short-circuit. SUI Move type paths are quite long and
    # rarely make it into the generic multichain token-price feed, so we
    # always check the static table first when fromChain/toChain is SUI.
    if chain in SUI_CHAIN_IDS or str(chain).lower() in {str(c).lower() for c in SUI_CHAIN_IDS}:
        bluechip = _SUI_BLUECHIP_LOOKUP.get(addr_lower)
        if bluechip:
            return {
                "address": addr_raw,
                "symbol": bluechip["symbol"],
                "decimals": int(bluechip["decimals"]),
            }

    # TRON bluechip short-circuit. TRC20 addresses are base58 (case-sensitive
    # on the wire) but we lowercase both sides of the lookup to be tolerant
    # of frontend casing inconsistencies.
    if chain in TRON_CHAIN_IDS or str(chain).lower() in {str(c).lower() for c in TRON_CHAIN_IDS}:
        bluechip = _TRON_BLUECHIP_LOOKUP.get(addr_lower)
        if bluechip:
            return {
                "address": addr_raw,
                "symbol": bluechip["symbol"],
                "decimals": int(bluechip["decimals"]),
            }

    # Aptos bluechip short-circuit (APT / USDC / USDT FA metadata addresses).
    if chain in APTOS_CHAIN_IDS or str(chain).lower() in {str(c).lower() for c in APTOS_CHAIN_IDS}:
        bluechip = _APTOS_BLUECHIP_LOOKUP.get(addr_lower)
        if bluechip:
            return {
                "address": addr_raw,
                "symbol": bluechip["symbol"],
                "decimals": int(bluechip["decimals"]),
            }

    # UTXO chains only have a single native asset that's already handled by
    # `is_chain_native_token` above. Anything else on a UTXO chain is not
    # supported by 1Click, so let the Redis lookup below (which will miss)
    # produce the standard "Token not found" error.

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

    # Aptos long-tail FA: 1Click list, then on-chain FA decimals (Hyperion preswap).
    if chain in APTOS_CHAIN_IDS or str(chain).lower() in {str(c).lower() for c in APTOS_CHAIN_IDS}:
        from nearintents_utils import resolve_1click_token_info
        from hyperion_utils import resolve_aptos_token_info

        oneclick_meta = resolve_1click_token_info(chain, addr_raw)
        if oneclick_meta:
            return oneclick_meta
        onchain_meta = resolve_aptos_token_info(addr_raw)
        if onchain_meta:
            return onchain_meta

    return None


def _is_cross_chain(from_chain: str, to_chain: str) -> bool:
    return str(from_chain) != str(to_chain)


# Legacy / mis-encoded chain ids some clients send, remapped to the canonical id.
# e.g. an old frontend config used 1385 for Berachain whose real chainId is 80094
# (0x138de). Normalizing here means all downstream resolution (Redis token lookup,
# 1Click asset id, EVM same-chain int) sees the correct chain.
_CHAIN_ID_REMAP = {
    "1385": "80094",  # Berachain — frontend config bug (1385 should be 80094)
}


def _normalize_chain_id(chain):
    """Normalize chain id to string, remapping known legacy/mis-encoded ids."""
    if chain is None:
        return ""
    s = str(chain).strip()
    return _CHAIN_ID_REMAP.get(s, s)


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
#   stage A (same chain):   tokenIn  -> intermediate on fromChain
#                           EVM: Bitget + OKX in parallel; Solana: Jupiter + Titan + OKX
#   stage B (cross chain, 1Click): intermediate on fromChain -> tokenOut on toChain
#
# At swap time Stage-A builders re-quote in parallel and pick the best executable route.

_PRESWAP_ROUTER_NAME = "preswap-nearintents"
_PRESWAP_EVM_INTERMEDIATE_SYMBOLS = ("USDC", "USDT", "WETH")
# Order matters: USDC first (best 1Click + Jupiter liquidity), USDT second,
# native SOL last (no ATA hop needed but typically smallest 1Click pool).
_PRESWAP_SOLANA_INTERMEDIATE_SYMBOLS = ("USDC", "USDT", "SOL")
# USDC/USDT first (best 1Click liquidity), wNEAR last (native wrap.near).
_PRESWAP_NEAR_INTERMEDIATE_SYMBOLS = ("USDC", "USDT", "NEAR", "ETH", "BTC")
_NEAR_NATIVE_ALIASES = frozenset({"", "near", "wnear", "wrap.near"})
# USDT first (matches frontend APTOS_MID_TOKENS order), then USDC.
_PRESWAP_APTOS_INTERMEDIATE_SYMBOLS = ("USDT", "USDC")
_APTOS_NATIVE_ALIASES = frozenset({"", "0xa", "apt", "aptos", "0x1::aptos_coin::aptoscoin"})


def _chain_id_int(chain) -> Optional[int]:
    try:
        return int(str(chain))
    except (ValueError, TypeError):
        return None


def _is_solana_chain(chain) -> bool:
    if chain is None:
        return False
    if chain in SOLANA_CHAIN_IDS:
        return True
    return str(chain).lower() in {str(x).lower() for x in SOLANA_CHAIN_IDS}


def _is_near_chain(chain) -> bool:
    if chain is None:
        return False
    if chain in NEAR_CHAIN_IDS:
        return True
    return str(chain).lower() in {str(x).lower() for x in NEAR_CHAIN_IDS}


def _is_aptos_chain(chain) -> bool:
    if chain is None:
        return False
    if chain in APTOS_CHAIN_IDS:
        return True
    return str(chain).lower() in {str(x).lower() for x in APTOS_CHAIN_IDS}


def _near_token_addr_eq(a: str, b: str) -> bool:
    a_l = (a or "").strip().lower()
    b_l = (b or "").strip().lower()
    if a_l == b_l:
        return True
    return a_l in _NEAR_NATIVE_ALIASES and b_l in _NEAR_NATIVE_ALIASES


def _aptos_token_addr_eq(a: str, b: str) -> bool:
    a_l = (a or "").strip().lower()
    b_l = (b or "").strip().lower()
    if a_l == b_l:
        return True
    return a_l in _APTOS_NATIVE_ALIASES and b_l in _APTOS_NATIVE_ALIASES


def _token_addr_eq(a: str, b: str) -> bool:
    if not a or not b:
        return False
    a_l, b_l = a.lower(), b.lower()
    if a_l == b_l:
        return True
    try:
        return normalize_evm_address(a) == normalize_evm_address(b)
    except Exception:
        return False


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

    Supports EVM, Solana, NEAR, and Aptos fromChain. Aptos / other non-EVM chains
    without preswap handlers fall through to a "not yet supported" message.
    """
    # Destination must be supported by 1Click, otherwise the bridge stage cannot succeed.
    dest_asset = resolve_1click_asset_id(to_chain, token_out.get("address", ""))
    if not dest_asset:
        return [], (
            f"destination token {token_out.get('address', '')} on chain {to_chain} "
            f"not supported by 1Click"
        )

    if _is_solana_chain(from_chain):
        return _solana_intermediate_candidates(from_chain, token_in)

    if _is_near_chain(from_chain):
        return _near_intermediate_candidates(from_chain, token_in)

    if _is_aptos_chain(from_chain):
        return _aptos_intermediate_candidates(from_chain, token_in)

    chain_int = _chain_id_int(from_chain)
    if chain_int is None:
        return [], f"pre-swap route does not yet support fromChain={from_chain}"

    return _evm_intermediate_candidates(from_chain, chain_int, token_in)


def _evm_intermediate_candidates(
    from_chain: str,
    chain_int: int,
    token_in: Dict,
) -> Tuple[list, Optional[str]]:
    bluechip_cfg = BLUECHIP_TOKENS.get(chain_int) or {}
    if not bluechip_cfg:
        return [], f"no bluechip intermediate configured for chain {from_chain}"
    token_in_addr = token_in.get("address", "")

    candidates = []
    for sym in _PRESWAP_EVM_INTERMEDIATE_SYMBOLS:
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


def _solana_intermediate_candidates(
    from_chain: str,
    token_in: Dict,
) -> Tuple[list, Optional[str]]:
    """Build the ordered Solana intermediate-token list. Native SOL uses the
    canonical wSOL mint as `address` because Jupiter/OKX both treat that as
    native SOL on input/output; the Solana tx assembler then routes via
    Jupiter's `nativeDestinationAccount` to skip the ATA hop.
    """
    token_in_addr = (token_in.get("address") or "").lower()
    candidates: list = []
    for sym in _PRESWAP_SOLANA_INTERMEDIATE_SYMBOLS:
        cfg = SOLANA_BLUECHIP_TOKENS.get(sym)
        if not cfg:
            continue
        addr = cfg.get("address", "")
        if not addr:
            continue
        if addr.lower() == token_in_addr:
            # tokenIn IS the intermediate -> direct 1Click route handles this.
            continue
        asset_id = resolve_1click_asset_id(from_chain, addr)
        if not asset_id:
            continue
        candidates.append({
            "address": addr,
            "symbol": cfg.get("symbol", sym),
            "decimals": int(cfg.get("decimals", 9)),
            "oneClickAssetId": asset_id,
        })
    if not candidates:
        return [], "no 1Click-supported intermediate (USDC/USDT/SOL) on chain solana"
    return candidates, None


def _near_intermediate_candidates(
    from_chain: str,
    token_in: Dict,
) -> Tuple[list, Optional[str]]:
    """Build the ordered NEAR intermediate-token list (Ref SmartRouter → 1Click)."""
    token_in_addr = token_in.get("address", "")
    candidates: list = []
    for sym in _PRESWAP_NEAR_INTERMEDIATE_SYMBOLS:
        cfg = _NEAR_BLUECHIP_TOKENS.get(sym)
        if not cfg:
            continue
        addr = cfg.get("address", "")
        if not addr:
            continue
        if _near_token_addr_eq(addr, token_in_addr):
            continue
        asset_id = resolve_1click_asset_id(from_chain, addr)
        if not asset_id:
            continue
        candidates.append({
            "address": addr,
            "symbol": cfg.get("symbol", sym),
            "decimals": int(cfg.get("decimals", 24)),
            "oneClickAssetId": asset_id,
        })
    if not candidates:
        return [], "no 1Click-supported intermediate (USDC/USDT/wNEAR) on chain near"
    return candidates, None


def _aptos_intermediate_candidates(
    from_chain: str,
    token_in: Dict,
) -> Tuple[list, Optional[str]]:
    """Build ordered Aptos intermediate list (Hyperion preswap → 1Click)."""
    token_in_addr = token_in.get("address", "")
    candidates: list = []
    for sym in _PRESWAP_APTOS_INTERMEDIATE_SYMBOLS:
        cfg = APTOS_BLUECHIP_TOKENS.get(sym)
        if not cfg:
            continue
        addr = cfg.get("address", "")
        if not addr:
            continue
        if _aptos_token_addr_eq(addr, token_in_addr):
            continue
        asset_id = resolve_1click_asset_id(from_chain, addr)
        if not asset_id:
            continue
        candidates.append({
            "address": addr,
            "symbol": cfg.get("symbol", sym),
            "decimals": int(cfg.get("decimals", 6)),
            "oneClickAssetId": asset_id,
        })
    if not candidates:
        return [], "no 1Click-supported intermediate (USDC/USDT) on chain aptos"
    return candidates, None


def _normalize_stage_a_errors(errors: Any) -> list:
    """Normalize aggregate quote `errors` into [{router, error}, ...]."""
    out = []
    if not errors:
        return out
    if not isinstance(errors, list):
        errors = [errors]
    for item in errors:
        if isinstance(item, dict):
            if item.get("router") is not None:
                out.append({
                    "router": str(item.get("router")),
                    "error": str(item.get("error", "")),
                })
            else:
                for router, err in item.items():
                    out.append({"router": str(router), "error": str(err)})
        else:
            out.append({"router": "unknown", "error": str(item)})
    return out


def _stage_a_aggregate_meta(aggregate_res: Dict) -> Dict[str, Any]:
    """Build preSwap transparency fields from aggregate_*_quote responses."""
    if not aggregate_res or not isinstance(aggregate_res, dict):
        return {}
    meta: Dict[str, Any] = {}
    winner = aggregate_res.get("quote") or {}
    if winner.get("router"):
        meta["stageAWinner"] = str(winner.get("router"))

    stage_all = []
    for q in aggregate_res.get("allQuotes") or []:
        if not isinstance(q, dict):
            continue
        entry = {
            "router": q.get("router"),
            "amountOut": str(q.get("amountOut") or ""),
            "minAmountOut": str(q.get("minAmountOut") or ""),
        }
        if q.get("amountOutReadable") is not None:
            entry["amountOutReadable"] = q.get("amountOutReadable")
        if q.get("market"):
            entry["market"] = q.get("market")
        entry["addressLookupTableAddresses"] = q.get("addressLookupTableAddresses") or []
        stage_all.append(entry)
    if stage_all:
        meta["stageAAllQuotes"] = stage_all
    meta["addressLookupTableAddresses"] = winner.get("addressLookupTableAddresses") or []

    stage_errors = _normalize_stage_a_errors(aggregate_res.get("errors"))
    if stage_errors:
        meta["stageAErrors"] = stage_errors
    return meta


def _merge_stage_a_meta_into_preswap(pre_swap: Dict, stage_a_meta: Optional[Dict]) -> None:
    """Attach stage-A aggregator breakdown to the preSwap block (in-place)."""
    if not isinstance(stage_a_meta, dict):
        return
    for key in ("stageAAllQuotes", "stageAErrors", "stageAWinner"):
        if stage_a_meta.get(key) is not None:
            pre_swap[key] = stage_a_meta[key]
    if stage_a_meta.get("market"):
        pre_swap["market"] = stage_a_meta["market"]
    if "addressLookupTableAddresses" in stage_a_meta:
        pre_swap["addressLookupTableAddresses"] = stage_a_meta.get(
            "addressLookupTableAddresses"
        ) or []


def _build_quote_provider_warnings(
    direct_errors: Optional[list] = None,
    preswap_errors: Optional[list] = None,
    stage_a_by_intermediate: Optional[list] = None,
) -> Optional[list]:
    """Non-fatal quote failures for QA (direct providers + Stage-A aggregators)."""
    warnings = []
    for msg in direct_errors or []:
        if msg:
            warnings.append({"source": "direct", "message": str(msg)})
    for msg in preswap_errors or []:
        if msg:
            warnings.append({"source": "preswap", "message": str(msg)})
    for block in stage_a_by_intermediate or []:
        if not isinstance(block, dict):
            continue
        inter = block.get("intermediate") or block.get("symbol") or ""
        for err in block.get("stageAErrors") or []:
            if not isinstance(err, dict):
                continue
            warnings.append({
                "source": "stageA",
                "intermediate": inter,
                "router": err.get("router"),
                "message": err.get("error", ""),
            })
    return warnings or None


# Short-lived verdict cache for the Bitget Stage-A executability check (B). Keyed by
# (chain, tokenIn, tokenOut); value is (is_executable, expiry_monotonic). Avoids
# rebuilding + re-simulating the Bitget route on every /quote for the same pair.
_BITGET_VERDICT_TTL = 90.0
_BITGET_VERDICT_CACHE: Dict[str, Tuple[bool, float]] = {}


def _bitget_verdict_key(chain_int: int, token_in: Dict, intermediate: Dict) -> str:
    ti = str((token_in or {}).get("address") or "").lower()
    to = str((intermediate or {}).get("address") or "").lower()
    return f"{int(chain_int)}:{ti}:{to}"


def _bitget_verdict_get(key: str) -> Optional[bool]:
    hit = _BITGET_VERDICT_CACHE.get(key)
    if not hit:
        return None
    executable, expiry = hit
    if time.monotonic() >= expiry:
        _BITGET_VERDICT_CACHE.pop(key, None)
        return None
    return executable


def _bitget_verdict_set(key: str, executable: bool) -> None:
    _BITGET_VERDICT_CACHE[key] = (executable, time.monotonic() + _BITGET_VERDICT_TTL)


def _erc20_balance_of(chain_int: int, token_addr: str, holder: str) -> Optional[int]:
    """Best-effort ERC20 balanceOf(holder) via eth_call. None if unavailable."""
    try:
        import requests
        from swap_utils import EVM_RPC_FALLBACK, normalize_evm_address
        rpc_urls = EVM_RPC_FALLBACK.get(int(chain_int)) or []
        data = "0x70a08231" + normalize_evm_address(holder).replace("0x", "").zfill(64)
        call_obj = {"to": normalize_evm_address(token_addr), "data": data}
        for url in rpc_urls:
            try:
                resp = requests.post(
                    url,
                    json={"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [call_obj, "latest"]},
                    timeout=6,
                )
                result = resp.json().get("result")
                if result:
                    return int(result, 16)
            except Exception:
                continue
    except Exception:
        return None
    return None


def _demote_stage_a_to_non_bitget(res: Dict, quote: Dict, reason: str = "") -> Dict:
    """Replace a Bitget Stage-A winner with the best non-Bitget quote (in-place on res).

    Returns the new winner quote (or the original if no alternative exists).
    """
    alts = [
        q for q in (res.get("allQuotes") or [])
        if isinstance(q, dict)
        and str(q.get("router") or "") != "bitget"
        and str(q.get("amountOut") or "")
    ]
    if not alts:
        return quote
    best = max(alts, key=lambda q: _safe_decimal(q.get("amountOut")))
    res["quote"] = best
    errs = res.get("errors")
    if not isinstance(errs, list):
        errs = []
    errs.append({"router": "bitget", "error": f"demoted (unexecutable): {reason}"})
    res["errors"] = errs
    return best


def _verify_evm_stage_a_winner(
    chain_int: int,
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    res: Dict,
    quote: Dict,
) -> Dict:
    """B — verify the EVM Stage-A winner can actually execute on-chain.

    Only Bitget winners are checked: Bitget sometimes quotes thin-liquidity paths
    it cannot execute (e.g. SolvBTC dust), winning on price but reverting on-chain
    (`TF`). On revert, demote to the best non-Bitget quote so /quote anchors the
    bridge to an executable route.

    Two verification strategies, in order:
      1) Plain eth_call from the sender. In the MCA flow the sender usually already
         holds tokenIn and has approved the router from a prior attempt, so a real
         call reveals `TF` with no special RPC support needed.
      2) Funded simulation (balance + allowance injected via state override) when
         the sender has not approved yet. Requires an override-capable RPC.

    Best-effort: if neither can verify (no approval AND no override support), the
    winner is kept unchanged.
    """
    try:
        if str(quote.get("router") or "") != "bitget":
            return quote

        # Short-circuit on a cached verdict for this pair (avoids rebuild+sim cost).
        vkey = _bitget_verdict_key(chain_int, token_in, intermediate)
        cached = _bitget_verdict_get(vkey)
        if cached is True:
            return quote
        if cached is False:
            logger.info("preswap quote: bitget cached as unexecutable; demoting (%s)", vkey)
            return _demote_stage_a_to_non_bitget(res, quote, reason="cached_unexecutable")

        from swap_utils import EVM_RPC_FALLBACK, build_swap_tx
        from evm_sim_utils import simulate_swap_funded

        built = build_swap_tx(
            chain_id=chain_int,
            router="bitget",
            token_in=token_in,
            token_out=intermediate,
            amount_in=str(amount_in),
            slippage=slippage,
            sender=sender,
            recipient=sender,
            market=str(quote.get("market") or ""),
        )
        if not built.get("success"):
            logger.warning(
                "preswap quote: bitget stage-A build failed (%s); demoting",
                built.get("error"),
            )
            return _demote_stage_a_to_non_bitget(res, quote, reason="build_failed")

        tx = built.get("tx") or {}
        spender = built.get("approveSpender") or tx.get("to", "")
        token_in_addr = token_in.get("address", "")

        # Strategy 1: plain sim using the sender's real on-chain state.
        sim = simulate_preswap_evm_swap(
            chain_int, sender, token_in_addr, str(amount_in), spender, tx,
        )
        if sim.get("success"):
            _bitget_verdict_set(vkey, True)
            return quote  # executes with real balance+allowance -> keep Bitget
        if not sim.get("skipped"):
            # A revert is only a route verdict when the sender actually holds the
            # tokens; otherwise it is a funding artifact (insufficient balance).
            need = int(str(amount_in))
            bal = _erc20_balance_of(chain_int, token_in_addr, sender)
            if bal is not None and bal >= need:
                logger.warning(
                    "preswap quote: bitget stage-A unexecutable (real sim: %s); demoting",
                    sim.get("error", ""),
                )
                _bitget_verdict_set(vkey, False)
                return _demote_stage_a_to_non_bitget(res, quote, reason=str(sim.get("error", "")))
            # balance insufficient/unknown -> fall through to funded sim

        # Strategy 2 (sim skipped — sender not approved): funded state-override sim.
        fsim = simulate_swap_funded(
            chain_int,
            EVM_RPC_FALLBACK.get(int(chain_int)) or [],
            sender,
            token_in_addr,
            spender,
            str(amount_in),
            tx,
        )
        if fsim.get("skipped"):
            return quote  # cannot verify -> keep Bitget (do not cache: unknown)
        if fsim.get("success"):
            _bitget_verdict_set(vkey, True)
            return quote
        logger.warning(
            "preswap quote: bitget stage-A unexecutable (funded sim: %s); demoting",
            fsim.get("error", ""),
        )
        _bitget_verdict_set(vkey, False)
        return _demote_stage_a_to_non_bitget(res, quote, reason=str(fsim.get("error", "")))
    except Exception as e:  # noqa: BLE001 — verification must never break quoting
        logger.warning("preswap quote bitget verify error: %s", e)
        return quote


def _stage_a_quote_evm(
    chain_int: int,
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
) -> Tuple[Optional[Decimal], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """Run a Bitget + OKX parallel Stage-A quote for the EVM leg.

    Returns ``(mid_amount, router, error, meta)``. `meta` may carry Bitget-only
    fields such as ``market`` (required for swap build).
    """
    from swap_utils import aggregate_quote

    res = aggregate_quote(
        chain_id=chain_int,
        token_in=token_in,
        token_out=intermediate,
        amount_in=str(amount_in),
        slippage=slippage,
        sender=sender,
        recipient=sender,
    )
    if not res.get("success"):
        return None, None, f"EVM aggregate quote failed: {res.get('error')}", None
    quote = res.get("quote") or {}
    out_str = str(quote.get("amountOut") or "")
    if not out_str:
        return None, None, "EVM aggregate quote missing amountOut", None

    # B — demote a Bitget winner that cannot execute on-chain (keeps Bitget when it works).
    quote = _verify_evm_stage_a_winner(
        chain_int, token_in, intermediate, str(amount_in), slippage, sender, res, quote,
    )
    out_str = str(quote.get("amountOut") or out_str)

    try:
        router = str(quote.get("router") or "okx")
        meta = _stage_a_aggregate_meta(res)
        if router == "bitget":
            market = str(quote.get("market") or "").strip()
            if market:
                meta["market"] = market
        return Decimal(out_str), router, None, meta
    except (InvalidOperation, ValueError) as e:
        return None, None, f"EVM amountOut invalid: {e}", None


def _stage_a_quote_solana(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
) -> Tuple[Optional[Decimal], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """Run a Jupiter + Titan + OKX parallel Stage-A quote for the Solana leg.

    Returns ``(mid_amount, router, error, meta)``. `meta` includes
    ``stageAAllQuotes`` / ``stageAErrors`` for API transparency.
    """
    from swap_utils import aggregate_solana_quote
    res = aggregate_solana_quote(
        token_in=token_in,
        token_out=intermediate,
        amount_in=str(amount_in),
        slippage=slippage,
        sender=sender,
        recipient=sender,
    )
    if not res.get("success"):
        err_detail = res.get("error", "Solana aggregate quote failed")
        details = res.get("details")
        if details:
            err_detail = f"{err_detail}: {details}"
        return None, None, err_detail, None
    quote = res.get("quote") or {}
    out_str = str(quote.get("amountOut") or "")
    if not out_str:
        return None, None, "Solana aggregate quote missing amountOut", None
    try:
        meta = _stage_a_aggregate_meta(res)
        return Decimal(out_str), str(quote.get("router") or "jupiter"), None, meta
    except (InvalidOperation, ValueError) as e:
        return None, None, f"Solana amountOut invalid: {e}", None


def _stage_a_quote_near(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage_decimal: float,
    sender: str,
) -> Tuple[Optional[Decimal], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """Run Ref SmartRouter Stage-A quote for the NEAR leg."""
    from near_smart_router_swap import near_same_chain_quote

    res = near_same_chain_quote(
        token_in=token_in,
        token_out=intermediate,
        amount_in=str(amount_in),
        slippage_decimal=float(slippage_decimal),
        sender=sender,
        recipient=sender,
    )
    if not res.get("success"):
        return None, None, f"NEAR SmartRouter quote failed: {res.get('error')}", None
    quote = res.get("quote") or {}
    out_str = str(quote.get("amountOut") or "")
    if not out_str:
        return None, None, "NEAR SmartRouter quote missing amountOut", None
    stage_router = str(quote.get("router") or "near-ref-smart")
    stage_meta: Dict[str, Any] = {"stageAWinner": stage_router}
    stage_all = []
    for q in res.get("allQuotes") or []:
        if not isinstance(q, dict):
            continue
        entry = {
            "router": q.get("router"),
            "amountOut": str(q.get("amountOut") or ""),
            "minAmountOut": str(q.get("minAmountOut") or q.get("amountOut") or ""),
        }
        if q.get("amountOutReadable") is not None:
            entry["amountOutReadable"] = q.get("amountOutReadable")
        stage_all.append(entry)
    if stage_all:
        stage_meta["stageAAllQuotes"] = stage_all
    try:
        return Decimal(out_str), stage_router, None, stage_meta
    except (InvalidOperation, ValueError) as e:
        return None, None, f"NEAR amountOut invalid: {e}", None


def _stage_a_quote_aptos(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
) -> Tuple[Optional[Decimal], Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """Run Hyperion Stage-A quote for Aptos preswap leg."""
    from swap_utils import aggregate_aptos_quote

    res = aggregate_aptos_quote(
        token_in=token_in,
        token_out=intermediate,
        amount_in=str(amount_in),
        slippage=slippage,
        sender=sender,
        recipient=sender,
    )
    if not res.get("success"):
        err_detail = res.get("error", "Aptos aggregate quote failed")
        details = res.get("details")
        if details:
            err_detail = f"{err_detail}: {details}"
        return None, None, err_detail, None
    quote = res.get("quote") or {}
    out_str = str(quote.get("amountOut") or "")
    if not out_str:
        return None, None, "Aptos aggregate quote missing amountOut", None
    try:
        meta = _stage_a_aggregate_meta(res)
        return Decimal(out_str), str(quote.get("router") or "hyperion"), None, meta
    except (InvalidOperation, ValueError) as e:
        return None, None, f"Aptos amountOut invalid: {e}", None


def _build_preswap_stage_block(
    is_solana_src: bool,
    is_near_src: bool,
    is_aptos_src: bool,
    from_chain: str,
    token_in: Dict,
    inter: Dict,
    amount_in: str,
    mid_amount_raw: Decimal,
    mid_amount_target: int,
    slippage_decimal: float,
    stage_a_router: Optional[str],
    stage_a_meta: Optional[Dict],
) -> Dict:
    """Assemble the preSwap object for a single intermediate candidate."""
    pre_swap = {
        "router": stage_a_router or (
            "solana-aggregate" if is_solana_src
            else ("near-ref-smart" if is_near_src
                  else ("aptos-aggregate" if is_aptos_src else "evm-aggregate"))
        ),
        "chainType": (
            CHAIN_TYPE_SOLANA if is_solana_src
            else (CHAIN_TYPE_NEAR if is_near_src
                  else (CHAIN_TYPE_APTOS if is_aptos_src else CHAIN_TYPE_EVM))
        ),
        "chainId": str(from_chain),
        "tokenIn": token_in,
        "tokenOut": inter,
        "amountIn": amount_in,
        "estimatedAmountOut": str(int(mid_amount_raw)),
        "amountOutTarget": str(mid_amount_target),
        "slippage": str(slippage_decimal),
    }
    _merge_stage_a_meta_into_preswap(pre_swap, stage_a_meta)
    return pre_swap


def _preswap_cross_chain_quote(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
    oneclick_extensions: Optional[Dict] = None,
) -> Dict:
    """Try the two-stage pre-swap + NearIntents route. Returns a unified quote dict
    ({"success": True/False, ...}) shaped like a cross-chain quote, with added
    `preSwap` / `bridge` sub-objects describing each stage.

    Supports EVM, Solana, NEAR, and Aptos fromChain. Stage-A uses chain-appropriate
    aggregators (Bitget+OKX for EVM, Jupiter+Titan+OKX for Solana, Ref
    SmartRouter for NEAR, Hyperion for Aptos); Stage-B is always NearIntents 1Click.
    """
    is_solana_src = _is_solana_chain(from_chain)
    is_near_src = _is_near_chain(from_chain)
    is_aptos_src = _is_aptos_chain(from_chain)
    chain_int = None if (is_solana_src or is_near_src or is_aptos_src) else _chain_id_int(from_chain)
    if not is_solana_src and not is_near_src and not is_aptos_src and chain_int is None:
        return {"success": False, "error": f"pre-swap route does not yet support fromChain={from_chain}"}

    candidates, reason = _preswap_intermediate_candidates(from_chain, to_chain, token_in, token_out)
    if not candidates:
        return {"success": False, "error": reason or "no 1Click-supported intermediate on fromChain"}

    slippage_decimal = convert_slippage_to_decimal(slippage)
    # The intermediate-amount buffer protects stage A: we target a slightly lower amount than
    # the raw aggregator estimate so the exact intermediate amount the bridge expects is
    # actually deliverable at swap time even if price drifts slightly.
    mid_buffer = Decimal("1") - Decimal(str(slippage_decimal))

    best = None
    best_amount = Decimal("-1")
    all_quotes = []
    errors = []
    stage_a_by_intermediate = []

    for inter in candidates:
        try:
            stage_a_meta = None
            if is_solana_src:
                mid_amount_raw, stage_a_router, err, stage_a_meta = _stage_a_quote_solana(
                    token_in, inter, str(amount_in), slippage, sender,
                )
            elif is_near_src:
                mid_amount_raw, stage_a_router, err, stage_a_meta = _stage_a_quote_near(
                    token_in, inter, str(amount_in), slippage_decimal, sender,
                )
            elif is_aptos_src:
                mid_amount_raw, stage_a_router, err, stage_a_meta = _stage_a_quote_aptos(
                    token_in, inter, str(amount_in), slippage, sender,
                )
            else:
                mid_amount_raw, stage_a_router, err, stage_a_meta = _stage_a_quote_evm(
                    chain_int, token_in, inter, str(amount_in), slippage, sender,
                )
            if isinstance(stage_a_meta, dict) and stage_a_meta.get("stageAErrors"):
                stage_a_by_intermediate.append({
                    "intermediate": inter.get("symbol") or "",
                    "intermediateAddress": inter.get("address") or "",
                    "stageAErrors": stage_a_meta.get("stageAErrors"),
                    "stageAWinner": stage_a_meta.get("stageAWinner"),
                })
            if err or mid_amount_raw is None:
                errors.append(f"{inter['symbol']}: {err}")
                continue

            # Target a slightly lower mid amount so stage-A is still feasible at swap time.
            mid_amount_target = int(mid_amount_raw * mid_buffer)
            if mid_amount_target <= 0:
                errors.append(f"{inter['symbol']}: mid target <= 0")
                continue

            # Stage B: NearIntents 1Click dry quote. With a Stage-A pre-swap the
            # exact amount delivered to the deposit address is only known at
            # execution time, so quote the bridge as FLEX_INPUT (and the `/swap`
            # build below uses FLEX_INPUT too, keeping quote/build consistent).
            near_res = nearintents_quote(
                from_chain=from_chain,
                to_chain=to_chain,
                token_in=inter,
                token_out=token_out,
                amount_in=str(mid_amount_target),
                sender=sender,
                recipient=recipient,
                slippage=slippage,
                oneclick_extensions=oneclick_extensions,
                swap_type="FLEX_INPUT",
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
                "preSwap": _build_preswap_stage_block(
                    is_solana_src=is_solana_src,
                    is_near_src=is_near_src,
                    is_aptos_src=is_aptos_src,
                    from_chain=from_chain,
                    token_in=token_in,
                    inter=inter,
                    amount_in=str(amount_in),
                    mid_amount_raw=mid_amount_raw,
                    mid_amount_target=mid_amount_target,
                    slippage_decimal=slippage_decimal,
                    stage_a_router=stage_a_router,
                    stage_a_meta=stage_a_meta,
                ),
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
        "stageAAggregateErrors": stage_a_by_intermediate or None,
    }


def _mca_flow(mca_block: Optional[Dict]) -> str:
    if not mca_block or not isinstance(mca_block, dict):
        return ""
    return str(mca_block.get("flow") or mca_block.get("mcaFlow") or "").strip().lower()


def _mca_deposit_extensions(mca_block: Optional[Dict]) -> Optional[Dict]:
    """Map optional `mca` block (flow=deposit) into 1Click /quote extra fields."""
    if _mca_flow(mca_block) != "deposit" or not mca_block:
        return None
    ext: Dict = {}
    crm = mca_block.get("customRecipientMsg") or mca_block.get("custom_recipient_msg")
    if crm:
        ext["customRecipientMsg"] = crm
    fees = mca_block.get("appFees") or mca_block.get("app_fees")
    if isinstance(fees, list) and fees:
        ext["appFees"] = fees
    if mca_block.get("referral"):
        ext["referral"] = str(mca_block["referral"])
    rt = mca_block.get("refundTo") or mca_block.get("refund_to")
    if rt:
        ext["refundTo"] = rt
    rf_type = mca_block.get("refundType") or mca_block.get("refund_type")
    if rf_type:
        ext["refundType"] = rf_type
    qwm = mca_block.get("quoteWaitingTimeMs")
    if qwm is None:
        qwm = mca_block.get("quote_waiting_time_ms")
    if qwm is not None:
        try:
            ext["quoteWaitingTimeMs"] = int(qwm)
        except (TypeError, ValueError):
            pass
    ir = mca_block.get("intentsRecipient") or mca_block.get("recipientOverride") or mca_block.get("recipient_override")
    if ir:
        ext["recipient"] = ir
    return ext if ext else None


def _mca_context_for_quote(
    mca_block: Optional[Dict],
    *,
    deposit_extras_scheduled: bool,
    is_cross_chain: bool,
    near_same_chain_mca_withdraw: bool = False,
    near_same_chain_mca_deposit_intents: bool = False,
    near_same_chain_mca_withdraw_intents: bool = False,
    near_withdraw_use_near_exec: bool = True,
) -> Optional[Dict]:
    flow = _mca_flow(mca_block)
    if not flow:
        return None
    ctx: Dict = {"flow": flow}
    if mca_block:
        mca_acc = (
            mca_block.get("mcaAccountId")
            or mca_block.get("mca_id")
            or mca_block.get("mca")
        )
        if mca_acc:
            ctx["mcaAccountId"] = str(mca_acc)
    if flow == "deposit":
        deposit_via_1click = bool(is_cross_chain) or bool(near_same_chain_mca_deposit_intents)
        ctx["depositOneClickConfigured"] = bool(deposit_extras_scheduled) and deposit_via_1click
        if near_same_chain_mca_deposit_intents:
            ctx["nearIntentsToLending"] = True
            ctx["routerForSwap"] = "nearintents or preswap-nearintents (from quote bestQuote.router)"
        if isinstance(mca_block, dict) and mca_block.get("depositCrmAutoFilled"):
            ctx["customRecipientMsgAutoFilled"] = True
        if near_same_chain_mca_deposit_intents:
            ctx["note"] = (
                "Same-chain NEAR deposit to Lending uses Near Intents 1Click (CRM in order). "
                "When tokenIn ≠ tokenOut (e.g. FLX→USDT), quote returns preswap-nearintents: Ref Stage-A "
                "must use 1Click depositAddress as swap_out_recipient (not mcaAccountId). "
                "When tokenIn equals tokenOut (USDT→USDT), router may be nearintents (direct ft_transfer to depositAddress). "
                "POST /api/swap/swap with quote router only; pass preSwap+bridge for preswap-nearintents."
            )
        else:
            ctx["note"] = (
                "Near Intents 1Click quotes always include appFees and referral from server config "
                "(INTENTS_APP_FEES_* / INTENTS_DEFAULT_REFERRAL). Optional `mca` fields (e.g. "
                "customRecipientMsg) merge when flow=deposit. OmniBridge-only quotes ignore 1Click extras."
            )
    elif flow == "withdraw":
        if ctx.get("mcaAccountId"):
            ctx["intentsRefundToSuggested"] = ctx["mcaAccountId"]
        if near_same_chain_mca_withdraw:
            ctx["nearDirectFromLending"] = True
            ctx["routerForSwap"] = ROUTER_NEAR_MCA_WITHDRAW
            if near_withdraw_use_near_exec:
                ctx["withdrawExecutionPlan"] = [
                    {
                        "step": 1,
                        "action": "sign_and_send_near",
                        "description": "User signs ONE NEAR tx to MCA `exec` (see quote `nearMcaWithdrawTx`; same shape as in-app call_on_near)",
                    },
                    {
                        "step": 2,
                        "action": "optional_broadcast_api",
                        "description": "POST /api/swap/swap with `nearMcaDepositSignedTx` if relaying a pre-signed tx (same as deposit broadcast).",
                    },
                ]
                ctx["note"] = (
                    "Same-chain NEAR Lending withdraw: `/quote` returns `nearMcaWithdrawTx` (MCA exec: Burrow Withdraw + token transfer to recipient). "
                    "`/swap` with router near-mca-withdraw rebuilds the same `tx`. "
                    "Optional: `mca.amountBurrow` and `mca.execSignerAccountId` when defaults differ from amountIn/recipient."
                )
            else:
                ctx["withdrawExecutionPlan"] = [
                    {
                        "step": 1,
                        "action": "sign_message",
                        "description": "MCA has no bound NEAR wallet: sign `mcaWithdrawToIntents.messageToSign` with `mca.signer` (same as Solana).",
                    },
                    {
                        "step": 2,
                        "action": "mcaRelayer",
                        "description": "POST /api/swap/swap with `mcaRelayer` (not NEAR `exec` from connected wallet).",
                    },
                ]
                ctx["note"] = (
                    "Same-chain NEAR Lending withdraw without bound NEAR controller: "
                    "`mcaWithdrawToIntents.submissionMode` is `multichain_relayer` (identical to Solana withdraw). "
                    "Bind NEAR on the MCA to use `near_exec` + `nearMcaWithdrawTx`."
                )
        elif near_same_chain_mca_withdraw_intents:
            ctx["nearWithdrawSwapViaIntents"] = True
            ctx["routerForSwap"] = "use quote mcaWithdrawToIntents (not DEX bestQuote.router)"
            ctx["withdrawExecutionPlan"] = [
                {
                    "step": 1,
                    "action": "sign_message_or_near_exec",
                    "description": "Use `data.mcaWithdrawToIntents`: `near_exec` → sign `data.nearMcaWithdrawTx`; `multichain_relayer` → sign `messageToSign` with `mca.signer`.",
                },
                {
                    "step": 2,
                    "action": "mcaRelayer_or_near_broadcast",
                    "description": "Relayer: POST /api/swap/swap with `mcaRelayer`. NEAR exec: `call_on_near` with `nearMcaWithdrawTx`, then poll order-status on `mcaWithdrawToIntents.depositAddress`.",
                },
            ]
            ctx["note"] = (
                "Same-chain NEAR Lending withdraw with token change (e.g. USDT→USDC): Burrow withdraw + ft to 1Click, then 1Click delivers tokenOut to recipient. "
                "`bestQuote` reflects 1Click estimate; sign using `mcaWithdrawToIntents` / `nearMcaWithdrawTx`, not Ref DEX swap."
            )
        elif is_cross_chain:
            ctx["withdrawExecutionPlan"] = [
                {
                    "step": 1,
                    "action": "sign_message_or_near_exec",
                    "description": "If `mcaWithdrawToIntents.submissionMode` is `multichain_relayer`: sign `messageToSign` with `mca.signer`. If `near_exec`: sign & send NEAR tx from `nearExecWalletPreview` (MCA `exec`, same as in-app `call_on_near`).",
                },
                {
                    "step": 2,
                    "action": "mcaRelayer",
                    "description": "Relayer path only: POST /api/swap/swap with `mcaRelayer`. NEAR-exec path broadcasts the signed `exec` from the wallet (no multichain relayer).",
                },
            ]
            ctx["note"] = (
                "Cross-chain Lending withdraw + 1Click: `/quote` may include `data.mcaWithdrawToIntents` with "
                "`business` + `depositAddress` (NearIntents `dry:false`). "
                "`submissionMode`: `near_exec` (NEAR wallet signs MCA `exec`) or `multichain_relayer` (off-chain `sign_message` + relayer)."
            )
        else:
            ctx["note"] = (
                "MCA withdraw block present but route not recognized; ensure fromChain/toChain, tokenIn/tokenOut, and mca.flow=withdraw."
            )
    return ctx


def _unified_chain_to_oneclick_slug(chain_val: str) -> str:
    c = str(chain_val or "").strip()
    return str(CHAIN_TO_1CLICK.get(c, CHAIN_TO_1CLICK.get(c.lower(), c.lower())))


def _try_attach_mca_withdraw_near_to_intents_quote(
    *,
    data: Dict[str, Any],
    from_chain: str,
    to_chain: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
    mca_block: Optional[Dict[str, Any]],
    oneclick_extensions: Optional[Dict[str, Any]],
    skip_1click_order: bool = False,
    direct_deposit_address: str = "",
) -> None:
    """
    MCA Burrow withdraw on NEAR + ft_transfer to 1Click `depositAddress` or NEAR `recipient`.

    NearIntents quote / `minAmountIn` must use the same NEP-141 smallest-unit amount as the MCA
    ``ft_transfer`` leg (Lending ``amountToken`` = ``max(0, amountIn - 1)``); otherwise the relay
    succeeds but 1Click stays ``INCOMPLETE_DEPOSIT`` while ``depositedAmount < minAmountIn``.

    - MCA has bound NEAR matching ``mca.signer``: `submissionMode=near_exec`, `nearMcaWithdrawTx`.
    - Otherwise: `submissionMode=multichain_relayer`, `messageToSign` (same JSON shape as Solana).

    When ``skip_1click_order`` is True (same-chain same-token withdraw to NEAR), funds go to
    ``direct_deposit_address`` / ``recipient`` without calling 1Click build.

    Writes `data['mcaWithdrawToIntents']`.
    """
    try:
        if not mca_block or not isinstance(mca_block, dict):
            return
        if _mca_flow(mca_block) != "withdraw":
            return
        oc_from = _unified_chain_to_oneclick_slug(from_chain)
        if oc_from != "near":
            return

        signer_obj = mca_block.get("signer") or mca_block.get("depositSigner") or {}
        if not isinstance(signer_obj, dict):
            return
        sign_chain = str(
            signer_obj.get("chain") or signer_obj.get("signerChain") or signer_obj.get("signer_chain") or ""
        ).strip().lower()
        if not sign_chain:
            return

        mca_acc = (
            mca_block.get("mcaAccountId")
            or mca_block.get("mca_id")
            or mca_block.get("mca")
        )
        if not mca_acc:
            return

        from mca_burrow_auto import resolve_mca_withdraw_near_exec_eligible

        nw_early = str(Cfg.NETWORK_ID)
        use_near_exec, _exec_near_hint = resolve_mca_withdraw_near_exec_eligible(
            mca_block,
            mca_account_id=str(mca_acc),
            network_id=nw_early,
        )

        tid = str((token_in or {}).get("address") or "").strip()
        if not tid:
            return

        from mca_burrow_auto import resolve_mca_withdraw_burrow_inner_amount

        amt_borrow_inner, br_err_note = resolve_mca_withdraw_burrow_inner_amount(
            network_id=str(Cfg.NETWORK_ID),
            token_id=tid,
            amount_token_smallest=str(amount_in),
            mca_block=mca_block,
        )
        if not amt_borrow_inner:
            logger.warning(
                "mcaWithdrawToIntents: omitting attach — %s",
                br_err_note
                or "could not resolve Burrow inner amount (explicit mca.amountBurrow or get_asset derive)",
            )
            return

        # Must match `assemble_mca_withdraw_to_intents_business` ft_transfer amount (Lending UI minus 1).
        # If we quote 1Click with full `amount_in` but only transfer `amount_in - 1`, status stays INCOMPLETE_DEPOSIT.
        amt_ft_for_intents = nep141_ft_transfer_amount_minus_one(str(amount_in))

        front_target = (
            "near"
            if _unified_chain_to_oneclick_slug(to_chain) == "near"
            else str(to_chain)
        )

        dep = ""
        deposit_memo = ""
        if skip_1click_order:
            dep = str(direct_deposit_address or recipient or "").strip()
            if not dep:
                logger.warning(
                    "mcaWithdrawToIntents: skip_1click_order requires recipient/direct_deposit_address"
                )
                return
        else:
            build_res = nearintents_build_tx(
                from_chain=from_chain,
                to_chain=to_chain,
                token_in=token_in,
                token_out=token_out,
                amount_in=str(amt_ft_for_intents),
                sender=sender,
                recipient=recipient,
                slippage=slippage,
                oneclick_extensions=oneclick_extensions,
            )
            if not build_res.get("success"):
                logger.warning(
                    f"mcaWithdrawToIntents: nearintents_build_tx failed: {build_res.get('error')}"
                )
                return
            tx_wrap = build_res.get("tx") or {}
            dep = str(tx_wrap.get("depositAddress") or "").strip()
            deposit_memo = str(tx_wrap.get("depositMemo") or "")
            if not dep:
                logger.warning("mcaWithdrawToIntents: empty depositAddress from nearintents_build_tx")
                return

        from mca_withdraw_cross_intents import (
            assemble_mca_withdraw_to_intents_business,
            attach_deposit_yocto_for_relayer,
            build_mca_register_token_tx_requests,
            build_near_exec_wallet_preview_for_business,
            message_to_sign_for_business,
        )

        nw = str(Cfg.NETWORK_ID)
        mca_s = str(mca_acc)

        relay_near_recipient = ""
        if not use_near_exec and isinstance(mca_block, dict):
            relay_near_recipient = str(
                mca_block.get("relayerNearRecipient")
                or mca_block.get("relayer_near_recipient")
                or mca_block.get("multichainRelayerNearAccount")
                or ""
            ).strip()
        if not use_near_exec and not relay_near_recipient:
            relay_near_recipient = str(
                getattr(Cfg, "MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID", "") or ""
            ).strip()

        prepay_inner = ""
        if isinstance(mca_block, dict):
            prepay_inner = str(
                mca_block.get("relayerPrepayBurrowInner")
                or mca_block.get("relayer_prepay_burrow_inner")
                or mca_block.get("relayerPrepaySimpleWithdrawInner")
                or ""
            ).strip()

        business = assemble_mca_withdraw_to_intents_business(
            network_id=nw,
            mca_account_id=mca_s,
            token_id_nep141=tid,
            amount_token_smallest=str(amount_in),
            amount_burrow_inner=str(amt_borrow_inner),
            intents_deposit_address=dep,
            frontend_target_chain=front_target,
            sign_chain_is_near=use_near_exec,
            simple_withdraw_tx=None,
            simple_withdraw_recipient_for_relayer=(
                relay_near_recipient if not use_near_exec else None
            ),
            relayer_prepay_simple_withdraw_inner=(prepay_inner or None),
        )

        reg_txs = build_mca_register_token_tx_requests(nw, tid, mca_s)

        submission_mode = "near_exec" if use_near_exec else "multichain_relayer"
        out: Dict[str, Any] = {
            "version": 1,
            "submissionMode": submission_mode,
            "depositAddress": dep,
            "depositMemo": deposit_memo,
            "business": business,
            "mcaAccountId": mca_s,
            "tokenId": tid,
            "amountIn": str(amount_in),
            # Same string passed to NearIntents build + ft_transfer (`max(0, amountIn - 1)` smallest units).
            "amountFtTransferSmallest": str(amt_ft_for_intents),
            "amountBurrowInner": str(amt_borrow_inner),
        }

        if use_near_exec:
            exec_signer_near = str(
                mca_block.get("execSignerAccountId")
                or mca_block.get("exec_signer_near")
                or mca_block.get("nearSignerAccountId")
                or signer_obj.get("identityKey")
                or signer_obj.get("identity_key")
                or _exec_near_hint
                or ""
            ).strip()
            if not exec_signer_near:
                logger.warning(
                    "mcaWithdrawToIntents: near_exec requires NEAR signer account "
                    "(mca.signer.identityKey or mca.execSignerAccountId)"
                )
                return
            preview = build_near_exec_wallet_preview_for_business(
                mca_account_id=mca_s,
                exec_signer_near=exec_signer_near,
                business=business,
                token_id=tid,
                intents_deposit_address=dep,
                amount_token_smallest=str(amt_ft_for_intents),
                amount_burrow_inner=str(amt_borrow_inner),
            )
            out["nearExecWalletPreview"] = preview
            out["signer"] = {"chain": sign_chain, "identityKey": exec_signer_near}
            data["nearMcaWithdrawTx"] = preview
        else:
            attach_yocto = attach_deposit_yocto_for_relayer(len(reg_txs) > 0)

            from mca_burrow_auto import format_wallet_wallet_object

            signer_key = str(
                signer_obj.get("identityKey") or signer_obj.get("identity_key") or ""
            ).strip()
            if not signer_key:
                logger.warning("mcaWithdrawToIntents: multichain_relayer requires mca.signer.identityKey")
                return
            if sign_chain in ("near", "near-mainnet"):
                logger.warning(
                    "mcaWithdrawToIntents: multichain_relayer with mca.signer.chain=near but NEAR "
                    "is not bound on MCA %s — quote with a bound non-NEAR wallet (EVM/Solana/Passkey).",
                    mca_s,
                )
                return
            try:
                w_obj = format_wallet_wallet_object(sign_chain, signer_key)
            except ValueError as e:
                logger.warning(f"mcaWithdrawToIntents: {e}")
                return
            wallet_json = json.dumps(w_obj, separators=(",", ":"), ensure_ascii=False)

            msg = message_to_sign_for_business(business)
            out["messageToSign"] = msg
            out["attachDepositYocto"] = attach_yocto
            out["signer"] = {"chain": sign_chain, "identityKey": signer_key}
            out["signerWalletJson"] = wallet_json

        data["mcaWithdrawToIntents"] = out
    except Exception as e:
        logger.warning(f"_try_attach_mca_withdraw_near_to_intents_quote: {e}")


def _synthetic_near_same_chain_mca_quote(
    from_chain: str,
    to_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
    mca_block: Dict,
    *,
    include_near_tx_preview: bool = True,
) -> Dict:
    """Synthetic quote for same-chain NEAR Lending withdraw (MCA exec or relayer via attach)."""
    flow = _mca_flow(mca_block)
    router = ROUTER_NEAR_MCA_WITHDRAW
    est = str(amount_in)
    bq = {
        "router": router,
        "fromChain": str(from_chain),
        "toChain": str(to_chain),
        "tokenIn": token_in,
        "tokenOut": token_out,
        "amountIn": est,
        "amountOut": est,
        "estimatedOut": est,
        "minAmountOut": est,
        "slippage": slippage,
        "sender": sender,
        "recipient": recipient,
        "timeEstimate": "NEAR account TX (<1 block)",
    }

    data: Dict = {
        "isCrossChain": False,
        "chainType": "near",
        "bestQuote": bq,
        "allQuotes": [bq],
    }

    if flow == "withdraw":
        if include_near_tx_preview:
            try:
                data["nearMcaWithdrawTx"] = _assemble_near_mca_withdraw_tx(
                    mca_block or {},
                    token_in,
                    str(amount_in),
                    str(recipient or "").strip(),
                )
                bq["txPreviewAvailable"] = True
                data["nearMcaWithdraw"] = {
                    "mode": "mca-exec",
                    "router": ROUTER_NEAR_MCA_WITHDRAW,
                    "note": (
                        "Sign ONE tx on MCA contract (`exec`): `nearMcaWithdrawTx` matches in-app withdrawFromMca NEAR→NEAR. "
                        "Pass accurate `mca.amountBurrow` when it differs from `amountIn` (Burrow internal units)."
                    ),
                }
            except Exception as e:
                logger.warning(f"_synthetic_near_same_chain_mca_quote withdraw tx: {e}")
                data["nearMcaWithdrawTxError"] = str(e)
                data["nearMcaWithdraw"] = {
                    "mode": "error",
                    "router": ROUTER_NEAR_MCA_WITHDRAW,
                    "detail": str(e),
                }
        else:
            bq["txPreviewAvailable"] = False
            data["nearMcaWithdraw"] = {
                "mode": "multichain-relayer",
                "router": ROUTER_NEAR_MCA_WITHDRAW,
                "note": (
                    "No bound NEAR on MCA: use `mcaWithdrawToIntents` (`submissionMode=multichain_relayer`, "
                    "sign `messageToSign` with bound Passkey/EVM/Solana — same as other chains)."
                ),
            }

    return {"code": 0, "msg": "success", "data": data}


def _near_same_chain_mca_deposit_swap(
    from_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    sender: str,
    recipient: str,
    router: str,
    mca_oc: Dict,
    *,
    quote_expected_out: str = "",
    quote_min_amount_out: str = "",
) -> Dict:
    if (router or "").strip() != ROUTER_NEAR_MCA_DEPOSIT:
        return {
            "code": -1,
            "msg": f"NEAR same-chain MCA deposit requires router '{ROUTER_NEAR_MCA_DEPOSIT}' from quote.",
            "data": None,
        }
    crm = str(mca_oc.get("customRecipientMsg") or mca_oc.get("custom_recipient_msg") or "").strip()
    if not crm:
        return {
            "code": -1,
            "msg": "customRecipientMsg missing (use mca.signer server autofill or pass customRecipientMsg).",
            "data": None,
        }
    dep = resolve_near_mca_deposit_receiver(mca_oc, recipient)
    if not dep:
        return {"code": -1, "msg": "Set mcaAccountId or recipient as NEAR deposit receiver.", "data": None}

    tx_payload = build_near_deposit_tx(
        token_address=token_in.get("address", ""),
        deposit_address=dep,
        amount_smallest=str(amount_in),
        sender=sender,
        deposit_memo=crm,
        network_id=str(Cfg.NETWORK_ID),
    )
    response_data = _build_common_response_data(
        is_cross_chain=False,
        source_chain_type=CHAIN_TYPE_NEAR,
        from_chain=from_chain,
        to_chain=from_chain,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        router=ROUTER_NEAR_MCA_DEPOSIT,
    )
    _set_near_source_tx(response_data, tx_payload, sender, router=ROUTER_NEAR_MCA_DEPOSIT)
    response_data["estimatedOut"] = str(amount_in)
    response_data["minAmountOut"] = str(amount_in)
    response_data["deposit"] = None

    qmin = _safe_int_str(quote_min_amount_out)
    if qmin > 0:
        build_est_int = _safe_int_str(str(amount_in))
        if build_est_int > 0 and build_est_int < qmin:
            return {
                "code": -2,
                "msg": "Price moved too much, please re-quote",
                "data": {
                    "quoteExpectedOut": quote_expected_out,
                    "quoteMinAmountOut": quote_min_amount_out,
                    "buildEstimatedOut": str(amount_in),
                },
            }
    return {"code": 0, "msg": "success", "data": response_data}


def _near_same_chain_mca_withdraw_swap(
    from_chain: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    sender: str,
    recipient: str,
    router: str,
    mca_oc: Dict,
    *,
    quote_expected_out: str = "",
    quote_min_amount_out: str = "",
) -> Dict:
    if (router or "").strip() != ROUTER_NEAR_MCA_WITHDRAW:
        return {
            "code": -1,
            "msg": f"NEAR same-chain Lending withdraw requires router '{ROUTER_NEAR_MCA_WITHDRAW}' from quote.",
            "data": None,
        }
    try:
        tx_payload = _assemble_near_mca_withdraw_tx(
            mca_oc,
            token_in,
            str(amount_in),
            str(recipient or "").strip(),
        )
    except Exception as e:
        return {"code": -1, "msg": str(e), "data": None}

    base = _build_common_response_data(
        is_cross_chain=False,
        source_chain_type=CHAIN_TYPE_NEAR,
        from_chain=from_chain,
        to_chain=from_chain,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        router=ROUTER_NEAR_MCA_WITHDRAW,
    )
    base["tx"] = tx_payload
    base["estimatedOut"] = str(amount_in)
    base["minAmountOut"] = str(amount_in)
    base["deposit"] = None
    base["mcaWithdrawNote"] = (
        "Sign `tx` (MCA exec withdraw → transfer). Optional: `mca.amountBurrow` for Burrow max_amount when it differs from amountIn."
    )

    qmin = _safe_int_str(quote_min_amount_out)
    if qmin > 0:
        build_est_int = _safe_int_str(str(amount_in))
        if build_est_int > 0 and build_est_int < qmin:
            return {
                "code": -2,
                "msg": "Price moved too much, please re-quote",
                "data": {
                    "quoteExpectedOut": quote_expected_out,
                    "quoteMinAmountOut": quote_min_amount_out,
                    "buildEstimatedOut": str(amount_in),
                },
            }
    return {"code": 0, "msg": "success", "data": base}


def _normalize_swap_history_context(ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge optional swap POST fields; missing keys are fine (frontend may not send them yet)."""
    if not isinstance(ctx, dict):
        return {}
    return dict(ctx)


def _try_insert_mca_relayer_swap_history(
    *,
    mca_id: str,
    batch_id: str,
    relayer_payload: Dict[str, Any],
    history_context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Best-effort swap_transactions row after multichain relayer enqueue.
    Never raises — relayer submit must succeed even if history insert fails.
    """
    try:
        from mca_relayer_payload import (
            extract_intents_deposit_from_relayer_payload,
            looks_like_1click_deposit_address,
            parse_swap_history_hints_from_relayer_payload,
        )

        ctx = _normalize_swap_history_context(history_context)
        hints = parse_swap_history_hints_from_relayer_payload(relayer_payload)

        mca_s = str(mca_id or "").strip()
        if not mca_s:
            return

        sender = str(
            ctx.get("multi_addr")
            or ctx.get("multiAddr")
            or ctx.get("sender")
            or mca_s
        ).strip() or mca_s
        multi_addr = str(ctx.get("multi_addr") or ctx.get("multiAddr") or mca_s).strip() or mca_s

        deposit = str(
            ctx.get("deposit_address")
            or ctx.get("depositAddress")
            or hints.get("deposit_address")
            or extract_intents_deposit_from_relayer_payload(relayer_payload)
            or ""
        ).strip()

        from_token = str(
            ctx.get("from_token")
            or ctx.get("fromToken")
            or ctx.get("token_in")
            or ctx.get("tokenIn")
            or hints.get("from_token")
            or ""
        ).strip()
        to_token = str(
            ctx.get("to_token")
            or ctx.get("toToken")
            or ctx.get("token_out")
            or ctx.get("tokenOut")
            or ""
        ).strip()

        amount_in = ctx.get("amount_in") or ctx.get("amountIn") or hints.get("amount_in")
        if amount_in is not None:
            amount_in = str(amount_in).strip() or None
        else:
            amount_in = None

        estimated_out = ctx.get("estimated_out") or ctx.get("estimatedOut") or ctx.get("expectedOut")
        if estimated_out is not None:
            estimated_out = str(estimated_out).strip() or None
        else:
            estimated_out = None

        from_chain = str(ctx.get("from_chain") or ctx.get("fromChain") or "near").strip() or "near"
        to_chain = str(
            ctx.get("to_chain") or ctx.get("toChain") or from_chain
        ).strip() or from_chain

        recipient = str(
            ctx.get("recipient")
            or ctx.get("receiver")
            or ""
        ).strip()
        if not recipient and deposit and not looks_like_1click_deposit_address(deposit):
            recipient = deposit
        elif not recipient:
            rh = str(hints.get("recipient_hint") or "").strip()
            if rh and not looks_like_1click_deposit_address(rh):
                recipient = rh

        router = str(ctx.get("router") or "").strip().lower()
        if deposit and looks_like_1click_deposit_address(deposit):
            if router in ("", "mca_relayer", "mca-withdraw-intents"):
                router = "nearintents"
        elif not router:
            router = "mca_relayer"

        tx_type = str(
            ctx.get("tx_type") or ctx.get("txType") or "mca-withdraw-relayer"
        ).strip() or "mca-withdraw-relayer"

        is_cross_chain = ctx.get("is_cross_chain")
        if is_cross_chain is None:
            is_cross_chain = ctx.get("isCrossChain")
        if is_cross_chain is None:
            if deposit and looks_like_1click_deposit_address(deposit):
                is_cross_chain = True
            elif from_chain and to_chain:
                is_cross_chain = str(from_chain) != str(to_chain)
            else:
                is_cross_chain = False
        else:
            is_cross_chain = bool(is_cross_chain)

        bid = str(batch_id or "").strip()
        if not bid:
            return
        from_hash = f"mca_relayer:{bid}"

        insert_swap_transaction(
            Cfg.NETWORK_ID,
            sender=sender,
            recipient=recipient or None,
            from_hash=from_hash,
            deposit_address=deposit or None,
            from_token=from_token,
            to_token=to_token,
            from_chain=from_chain,
            to_chain=to_chain,
            amount_in=amount_in,
            estimated_out=estimated_out,
            router=router,
            tx_type=tx_type,
            is_cross_chain=is_cross_chain,
            multi_addr=multi_addr,
            swap_id=bid,
            status="PENDING",
        )
        logger.info(
            f"mca_relayer swap history inserted from_hash={from_hash} mca={mca_s} "
            f"deposit={deposit[:16] + '...' if len(deposit) > 16 else deposit or '(none)'}"
        )
    except Exception as e:
        logger.warning(f"mca_relayer swap history insert skipped (non-fatal): {e}")


def _unified_mca_relayer_submit(
    payload: Dict,
    history_context: Optional[Dict[str, Any]] = None,
) -> Dict:
    """Forward a signed MCA relayer package to multichain_lending_requests (DB queue)."""
    try:
        from mca_relayer_payload import canonicalize_mca_relayer_block

        if not payload.get("mcaAccountId") and not payload.get("mca_id"):
            return {"code": -1, "msg": "mcaRelayer requires mcaAccountId (or mca_id)", "data": None}
        if not payload.get("wallet"):
            return {"code": -1, "msg": "mcaRelayer requires wallet", "data": None}
        page_display_data = str(
            payload.get("page_display_data") or payload.get("pageDisplayData") or ""
        )
        try:
            canon = canonicalize_mca_relayer_block(payload)
        except ValueError as ve:
            return {"code": -1, "msg": str(ve), "data": None}
        mca_id = canon.get("mcaAccountId") or canon.get("mca_id")
        requests_list = canon.get("request")
        if not isinstance(requests_list, list) or not requests_list:
            return {"code": -1, "msg": "mcaRelayer.request must be a non-empty array", "data": None}
        wallet = canon.get("wallet")
        batch_id = add_multichain_lending_requests(
            Cfg.NETWORK_ID, mca_id, wallet, requests_list, page_display_data
        )
        bid_str = str(batch_id)
        deposit_hint = ""
        try:
            from mca_relayer_payload import extract_intents_deposit_from_relayer_payload

            deposit_hint = extract_intents_deposit_from_relayer_payload(dict(payload)) or ""
        except Exception:
            deposit_hint = ""
        _try_insert_mca_relayer_swap_history(
            mca_id=str(mca_id or ""),
            batch_id=bid_str,
            relayer_payload=dict(payload),
            history_context=history_context,
        )
        data_out: Dict[str, Any] = {
            "batchId": bid_str,
            "orderId": bid_str,
            "statusRouter": "mca_relayer",
            "submissionType": "mca_relayer",
        }
        if deposit_hint:
            data_out["intentsDepositAddress"] = deposit_hint
        return {
            "code": 0,
            "msg": "success",
            "data": data_out,
        }
    except Exception as e:
        logger.exception(f"_unified_mca_relayer_submit error: {e}")
        return {"code": -1, "msg": str(e), "data": None}


def build_mca_relayer_swap_history_context(
    *,
    from_chain: str = "",
    to_chain: str = "",
    token_in_address: str = "",
    token_out_address: str = "",
    amount_in: str = "",
    sender: str = "",
    recipient: str = "",
    router: str = "",
    quote_expected_out: str = "",
    deposit_address: str = "",
    is_cross_chain: Optional[bool] = None,
    tx_type: str = "",
    multi_addr: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Collect optional swap POST fields for relayer history (all may be empty)."""
    ctx: Dict[str, Any] = {
        "from_chain": from_chain,
        "to_chain": to_chain,
        "token_in": token_in_address,
        "token_out": token_out_address,
        "amount_in": amount_in,
        "sender": sender,
        "recipient": recipient,
        "router": router,
        "expectedOut": quote_expected_out,
        "deposit_address": deposit_address,
        "tx_type": tx_type,
        "multi_addr": multi_addr,
    }
    if is_cross_chain is not None:
        ctx["is_cross_chain"] = is_cross_chain
    if isinstance(extra, dict):
        for k, v in extra.items():
            if v is not None and k not in ctx:
                ctx[k] = v
    return ctx


def unified_quote(
    from_chain: str,
    to_chain: str,
    token_in_address: str,
    token_out_address: str,
    amount_in: str,
    slippage: float = 0.5,
    sender: str = "",
    recipient: str = "",
    mca: Optional[Dict] = None,
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
    # NOTE: we deliberately allow empty-string tokenIn / tokenOut here. For
    # several chains (EVM native ETH, UTXO chains like BTC/ZEC where there's
    # no contract address at all, SUI / TRON / NEAR where empty string is one
    # of the accepted native markers), an empty string is the canonical way
    # the frontend signals "native gas token". `_resolve_token_info` handles
    # that case via `is_chain_native_token`; if the chain genuinely doesn't
    # have a native asset listed by our metadata table the resolver will
    # return None and the standard "Token not found" message fires below.
    if token_in_address is None:
        return {"code": -1, "msg": "tokenIn is required"}
    if token_out_address is None:
        return {"code": -1, "msg": "tokenOut is required"}
    if not amount_in:
        return {"code": -1, "msg": "amountIn is required"}
    if not sender:
        return {"code": -1, "msg": "sender is required"}
    if not recipient:
        recipient = sender

    token_in_info = _resolve_token_info(from_chain, token_in_address)
    token_out_info = _resolve_token_info(to_chain, token_out_address)

    if not token_in_info:
        return {"code": -1, "msg": f"Token {token_in_address!r} not found on chain {from_chain}. Check address and chain."}
    if not token_out_info:
        return {"code": -1, "msg": f"Token {token_out_address!r} not found on chain {to_chain}. Check address and chain."}

    mca_enriched = mca
    if isinstance(mca, dict) and mca:
        from mca_burrow_auto import enrich_mca_deposit_block

        mca_enriched, crm_err = enrich_mca_deposit_block(dict(mca), Cfg.NETWORK_ID)
        if crm_err:
            return {"code": -1, "msg": crm_err, "data": None}

    oneclick_ext = _mca_deposit_extensions(mca_enriched)

    near_dep_intents = (not _is_cross_chain(from_chain, to_chain)) and near_same_chain_mca_deposit_intents_applies(
        from_chain, to_chain, token_in_info, token_out_info, mca_enriched
    )
    near_withdraw_dir = (not _is_cross_chain(from_chain, to_chain)) and near_same_chain_mca_withdraw_applies(
        from_chain, to_chain, token_in_info, token_out_info, mca_enriched
    )
    near_withdraw_intents = (not _is_cross_chain(from_chain, to_chain)) and near_same_chain_mca_withdraw_intents_applies(
        from_chain, to_chain, token_in_info, token_out_info, mca_enriched
    )

    near_withdraw_use_near_exec = False
    if near_withdraw_dir and isinstance(mca_enriched, dict):
        mca_id_for_exec = str(
            mca_enriched.get("mcaAccountId")
            or mca_enriched.get("mca_id")
            or ""
        ).strip()
        if mca_id_for_exec:
            from mca_burrow_auto import resolve_mca_withdraw_near_exec_eligible

            near_withdraw_use_near_exec, _ = resolve_mca_withdraw_near_exec_eligible(
                mca_enriched,
                mca_account_id=mca_id_for_exec,
                network_id=str(Cfg.NETWORK_ID),
            )

    if not _is_cross_chain(from_chain, to_chain):
        if near_dep_intents:
            crm_chk = (mca_enriched or {}).get("customRecipientMsg") or (mca_enriched or {}).get("custom_recipient_msg")
            if not (isinstance(crm_chk, str) and crm_chk.strip()):
                return {
                    "code": -1,
                    "msg": "NEAR same-chain Lending deposit requires customRecipientMsg (use mca.signer + mcaAccountId for server CRM).",
                    "data": None,
                }
            resp = _cross_chain_quote(
                from_chain,
                to_chain,
                token_in_info,
                token_out_info,
                amount_in,
                slippage,
                sender,
                recipient,
                mca_block=mca_enriched,
            )
        elif near_withdraw_dir:
            if not ((mca_enriched or {}).get("mcaAccountId") or (mca_enriched or {}).get("mca_id")):
                return {
                    "code": -1,
                    "msg": "NEAR same-chain withdraw quote requires mca.mcaAccountId.",
                    "data": None,
                }
            resp = _synthetic_near_same_chain_mca_quote(
                from_chain,
                to_chain,
                token_in_info,
                token_out_info,
                amount_in,
                slippage,
                sender,
                recipient,
                mca_enriched if isinstance(mca_enriched, dict) else {},
                include_near_tx_preview=near_withdraw_use_near_exec,
            )
        elif near_withdraw_intents:
            if not ((mca_enriched or {}).get("mcaAccountId") or (mca_enriched or {}).get("mca_id")):
                return {
                    "code": -1,
                    "msg": "NEAR same-chain withdraw (swap via 1Click) requires mca.mcaAccountId.",
                    "data": None,
                }
            resp = _cross_chain_quote(
                from_chain,
                to_chain,
                token_in_info,
                token_out_info,
                amount_in,
                slippage,
                sender,
                recipient,
                mca_block=mca_enriched,
            )
        else:
            resp = _same_chain_quote(from_chain, token_in_info, token_out_info, amount_in, slippage, sender, recipient)
    else:
        resp = _cross_chain_quote(
            from_chain,
            to_chain,
            token_in_info,
            token_out_info,
            amount_in,
            slippage,
            sender,
            recipient,
            mca_block=mca_enriched,
        )

    mc_ctx = _mca_context_for_quote(
        mca_enriched if isinstance(mca_enriched, dict) else None,
        deposit_extras_scheduled=bool(oneclick_ext),
        is_cross_chain=_is_cross_chain(from_chain, to_chain) or near_withdraw_intents,
        near_same_chain_mca_withdraw=near_withdraw_dir,
        near_same_chain_mca_deposit_intents=near_dep_intents,
        near_same_chain_mca_withdraw_intents=near_withdraw_intents,
        near_withdraw_use_near_exec=near_withdraw_use_near_exec,
    )
    if mc_ctx is not None and isinstance(resp, dict) and resp.get("code") == 0:
        dat = resp.get("data")
        if isinstance(dat, dict):
            dat["mcaContext"] = mc_ctx

    attach_mca_withdraw_intents = (
        isinstance(mca_enriched, dict)
        and _mca_flow(mca_enriched) == "withdraw"
        and (
            _is_cross_chain(from_chain, to_chain)
            or near_withdraw_intents
            or (near_withdraw_dir and not near_withdraw_use_near_exec)
        )
    )
    if (
        isinstance(resp, dict)
        and resp.get("code") == 0
        and isinstance(resp.get("data"), dict)
        and attach_mca_withdraw_intents
    ):
        _try_attach_mca_withdraw_near_to_intents_quote(
            data=resp["data"],
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in_info,
            token_out=token_out_info,
            amount_in=str(amount_in),
            slippage=float(slippage),
            sender=str(sender),
            recipient=str(recipient),
            mca_block=mca_enriched if isinstance(mca_enriched, dict) else None,
            oneclick_extensions=oneclick_ext,
            skip_1click_order=bool(near_withdraw_dir and not near_withdraw_intents),
            direct_deposit_address=str(recipient or "").strip(),
        )
        if near_withdraw_intents:
            dat = resp.get("data")
            if isinstance(dat, dict) and dat.get("mcaWithdrawToIntents"):
                dat["mcaWithdrawRoute"] = "near-same-chain-withdraw-intents"
                bq = dat.get("bestQuote")
                if isinstance(bq, dict):
                    bq["mcaWithdrawViaIntents"] = True
        elif near_withdraw_dir and not near_withdraw_use_near_exec:
            dat = resp.get("data")
            if isinstance(dat, dict) and dat.get("mcaWithdrawToIntents"):
                dat["mcaWithdrawRoute"] = "near-same-chain-withdraw-relayer"

    return resp


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
            quote_data = dict(result.get("quote", {}) or {})
            all_quotes = result.get("allQuotes", [])
            if not all_quotes and quote_data:
                all_quotes = [quote_data]
            stage_errors = _normalize_stage_a_errors(result.get("errors"))
            if stage_errors:
                quote_data["stageAErrors"] = stage_errors
            if result.get("allQuotes"):
                quote_data["stageAAllQuotes"] = [
                    {
                        "router": q.get("router"),
                        "amountOut": str(q.get("amountOut") or ""),
                        "minAmountOut": str(q.get("minAmountOut") or ""),
                    }
                    for q in result.get("allQuotes", [])
                    if isinstance(q, dict)
                ]
            data = {
                "isCrossChain": False,
                "bestQuote": quote_data,
                "allQuotes": all_quotes,
                "chainType": result.get("chainType", "evm"),
            }
            warnings = _build_quote_provider_warnings(
                stage_a_by_intermediate=(
                    [{"intermediate": "same-chain", "stageAErrors": stage_errors}]
                    if stage_errors else None
                ),
            )
            if warnings:
                data["quoteProviderWarnings"] = warnings
            return {"code": 0, "msg": "success", "data": data}
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
    mca_block: Optional[Dict] = None,
) -> Dict:
    """Run OmniBridge + NearIntents quotes in parallel, return best.

    When ``tokenIn`` is not listed by 1Click on ``fromChain`` (e.g. Ref-only
    NEP-141), skip the direct 1Click leg and run the two-stage preswap route
    (Ref SmartRouter / SmartX → intermediate → 1Click) in parallel with OmniBridge.
    """
    oneclick_extensions = _mca_deposit_extensions(mca_block)
    omni_result = None
    near_result = None
    errors = []

    token_in_addr = token_in.get("address", "")
    token_in_oneclick = _token_in_supported_by_1click(from_chain, token_in_addr)

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
    if (oneclick_from or oneclick_to) and token_in_oneclick:

        def _run_near_quote():
            return nearintents_quote(
                from_chain=from_chain,
                to_chain=to_chain,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                sender=sender,
                recipient=recipient,
                slippage=slippage,
                oneclick_extensions=oneclick_extensions,
            )

        f = _executor.submit(_run_near_quote)
        futures[f] = "nearintents"
    elif (oneclick_from or oneclick_to) and not token_in_oneclick:
        errors.append(
            f"nearintents: skipped direct quote — tokenIn {token_in_addr!r} not supported by 1Click on {from_chain}; use preswap"
        )

    preswap_future = None
    if not token_in_oneclick:
        preswap_future = _executor.submit(
            _preswap_cross_chain_quote,
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage=slippage,
            sender=sender,
            recipient=recipient,
            oneclick_extensions=oneclick_extensions,
        )

    if not futures and not preswap_future:
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

    preswap_res = None
    preswap_stage_a_errors = None
    preswap_route_errors = None

    if preswap_future is not None:
        try:
            preswap_res = preswap_future.result(timeout=45)
        except Exception as e:
            preswap_res = {"success": False, "error": str(e)}
        if preswap_res.get("success"):
            ps_quote = preswap_res.get("quote")
            ps_all = preswap_res.get("allQuotes") or []
            preswap_stage_a_errors = preswap_res.get("stageAAggregateErrors")
            preswap_route_errors = preswap_res.get("errors")
            if not best_quote:
                best_quote = ps_quote
                all_quotes = list(all_quotes) + ps_all
                if errors:
                    logger.info(
                        "cross-chain: tokenIn not 1Click-listed, using preswap route. direct errors: %s",
                        errors,
                    )
            else:
                try:
                    ps_out = Decimal(str((ps_quote or {}).get("estimatedOut") or "0"))
                    direct_out = Decimal(str(best_quote.get("estimatedOut") or "0"))
                    if ps_out > direct_out:
                        best_quote = ps_quote
                        all_quotes = list(all_quotes) + ps_all
                except (InvalidOperation, ValueError):
                    pass
        elif not token_in_oneclick:
            errors.append(f"preswap: {preswap_res.get('error')}")
    elif not best_quote:
        preswap_res = _preswap_cross_chain_quote(
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage=slippage,
            sender=sender,
            recipient=recipient,
            oneclick_extensions=oneclick_extensions,
        )
        if preswap_res.get("success"):
            best_quote = preswap_res.get("quote")
            ps_all = preswap_res.get("allQuotes") or []
            all_quotes = list(all_quotes) + ps_all
            preswap_stage_a_errors = preswap_res.get("stageAAggregateErrors")
            preswap_route_errors = preswap_res.get("errors")
            if errors:
                logger.info(
                    "cross-chain direct providers all failed, falling back to pre-swap route. direct errors: %s",
                    errors,
                )
        else:
            errors.append(f"preswap: {preswap_res.get('error')}")

    if not best_quote:
        error_detail = "; ".join(errors) if errors else "All providers failed"
        return {"code": -1, "msg": f"Cross-chain quote failed: {error_detail}"}

    quote_warnings = _build_quote_provider_warnings(
        direct_errors=errors,
        preswap_errors=preswap_route_errors if preswap_res and preswap_res.get("success") else None,
        stage_a_by_intermediate=preswap_stage_a_errors,
    )
    if errors:
        logger.info(f"cross-chain quote partial provider failures: {errors}")

    data = {
        "isCrossChain": True,
        "bestQuote": best_quote,
        "allQuotes": all_quotes,
        "chainType": "cross-chain",
    }
    if quote_warnings:
        data["quoteProviderWarnings"] = quote_warnings
    if preswap_stage_a_errors:
        data["stageAAggregateErrors"] = preswap_stage_a_errors

    return {
        "code": 0,
        "msg": "success",
        "data": data,
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
    mca_relayer: Optional[Dict] = None,
    mca_oneclick: Optional[Dict] = None,
    deposit_address: str = "",
    is_cross_chain: Optional[bool] = None,
    tx_type: str = "",
    multi_addr: str = "",
) -> Dict:
    """
    Unified swap (build tx) entry point.
    - Same chain: build tx + approve info
    - Cross chain: build via specified router (omnibridge / nearintents)
    - Optional `mca_relayer`: forward signed relayer payloads (no ordinary swap tx)
    """
    if mca_relayer is not None and isinstance(mca_relayer, dict) and mca_relayer:
        history_ctx = build_mca_relayer_swap_history_context(
            from_chain=from_chain,
            to_chain=to_chain,
            token_in_address=token_in_address,
            token_out_address=token_out_address,
            amount_in=amount_in,
            sender=sender,
            recipient=recipient,
            router=router,
            quote_expected_out=quote_expected_out,
            deposit_address=deposit_address,
            is_cross_chain=is_cross_chain,
            tx_type=tx_type,
            multi_addr=multi_addr,
        )
        return _unified_mca_relayer_submit(mca_relayer, history_context=history_ctx)

    from_chain = _normalize_chain_id(from_chain)
    to_chain = _normalize_chain_id(to_chain)

    if not from_chain or not to_chain:
        return {"code": -1, "msg": "fromChain and toChain are required"}
    # See note in `unified_quote`: empty string is a valid native-token
    # marker (UTXO chains have no contract address, EVM native is ""), so we
    # only reject genuinely missing fields here.
    if token_in_address is None:
        return {"code": -1, "msg": "tokenIn is required"}
    if token_out_address is None:
        return {"code": -1, "msg": "tokenOut is required"}
    if not amount_in:
        return {"code": -1, "msg": "amountIn is required"}
    if not sender:
        return {"code": -1, "msg": "sender is required"}
    if not recipient:
        recipient = sender

    token_in_info = _resolve_token_info(from_chain, token_in_address)
    token_out_info = _resolve_token_info(to_chain, token_out_address)

    if not token_in_info:
        return {"code": -1, "msg": f"Token {token_in_address!r} not found on chain {from_chain}"}
    if not token_out_info:
        return {"code": -1, "msg": f"Token {token_out_address!r} not found on chain {to_chain}"}

    mca_oc = mca_oneclick
    if isinstance(mca_oc, dict) and mca_oc:
        from mca_burrow_auto import enrich_mca_deposit_block

        mca_oc, crm_err = enrich_mca_deposit_block(dict(mca_oc), Cfg.NETWORK_ID)
        if crm_err:
            return {"code": -1, "msg": crm_err, "data": None}

    if not _is_cross_chain(from_chain, to_chain):
        if isinstance(mca_oc, dict) and mca_oc and near_same_chain_mca_deposit_intents_applies(
            from_chain, to_chain, token_in_info, token_out_info, mca_oc
        ):
            r_norm = (router or "").strip()
            if r_norm == ROUTER_NEAR_MCA_DEPOSIT:
                return {
                    "code": -1,
                    "msg": (
                        "router near-mca-deposit is no longer used for same-chain NEAR Lending deposit. "
                        "Re-quote and use router from bestQuote (nearintents or preswap-nearintents)."
                    ),
                    "data": None,
                }
            if r_norm in ("near-ref-smart", "near-smartx", "near-ref", "ref"):
                return {
                    "code": -1,
                    "msg": (
                        "Same-chain MCA deposit must not use DEX router near-ref-smart/near-smartx. "
                        "Re-quote with mca.flow=deposit; use bestQuote.router preswap-nearintents or nearintents "
                        "and pass preSwap+bridge from the quote response."
                    ),
                    "data": None,
                }
            if not r_norm:
                return {
                    "code": -1,
                    "msg": "router is required (from quote bestQuote: nearintents or preswap-nearintents).",
                    "data": None,
                }
            return _cross_chain_swap(
                from_chain=from_chain,
                to_chain=to_chain,
                token_in=token_in_info,
                token_out=token_out_info,
                amount_in=amount_in,
                slippage=slippage,
                sender=sender,
                recipient=recipient,
                router=router,
                quote_expected_out=quote_expected_out,
                quote_min_amount_out=quote_min_amount_out,
                pre_swap=pre_swap,
                bridge=bridge,
                oneclick_extensions=_mca_deposit_extensions(mca_oc),
            )
        if isinstance(mca_oc, dict) and mca_oc and near_same_chain_mca_withdraw_applies(
            from_chain, to_chain, token_in_info, token_out_info, mca_oc
        ):
            return _near_same_chain_mca_withdraw_swap(
                from_chain,
                token_in_info,
                token_out_info,
                amount_in,
                sender,
                recipient,
                router,
                mca_oc,
                quote_expected_out=quote_expected_out,
                quote_min_amount_out=quote_min_amount_out,
            )
        if isinstance(mca_oc, dict) and mca_oc and near_same_chain_mca_withdraw_intents_applies(
            from_chain, to_chain, token_in_info, token_out_info, mca_oc
        ):
            return {
                "code": -1,
                "msg": (
                    "Same-chain MCA withdraw with token swap uses quote `data.mcaWithdrawToIntents` "
                    "(near_exec: sign `nearMcaWithdrawTx`; multichain_relayer: POST /api/swap/swap with `mcaRelayer`). "
                    "Do not call unified swap build with DEX router."
                ),
                "data": None,
            }
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
            oneclick_extensions=_mca_deposit_extensions(mca_oc),
        )


def _is_near_mca_withdraw_router(router: str) -> bool:
    """MCA withdraw (funds leave Lending) keeps legacy single-object ``tx``."""
    return (router or "").strip() == ROUTER_NEAR_MCA_WITHDRAW


def _set_near_source_tx(
    response_data: Dict[str, Any],
    tx: Any,
    sender: str,
    *,
    router: str = "",
) -> None:
    """Normalize NEAR source-chain ``tx`` to an ordered array and attach sign hints.

    Skipped only for ``near-mca-withdraw`` (MCA → wallet). All other NEAR-source
    flows use an array, including ``near-mca-deposit`` (wallet → MCA) and cross-chain
    nearintents deposits.
    """
    if not isinstance(response_data, dict):
        return
    if _is_near_mca_withdraw_router(router):
        response_data["tx"] = tx
        return
    from near_smart_router_swap import near_source_tx_to_array

    response_data["tx"] = near_source_tx_to_array(tx, sender=sender)
    _attach_near_sign_transactions(response_data, response_data["tx"], sender)


def _attach_near_sign_transactions(
    response_data: Dict[str, Any],
    tx: Any,
    sender: str,
) -> None:
    """Attach ordered NEAR wallet batch to unified swap API responses."""
    if not isinstance(response_data, dict) or tx is None:
        return
    from near_smart_router_swap import near_tx_to_sign_transactions

    sign_txs: list = []
    if isinstance(tx, list):
        for item in tx:
            sign_txs.extend(near_tx_to_sign_transactions(item, sender))
    else:
        sign_txs = near_tx_to_sign_transactions(tx, sender)
    if not sign_txs:
        return
    response_data["nearSignTransactions"] = sign_txs
    response_data["nearSignMode"] = "batch" if len(sign_txs) > 1 else "single"


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
        if source_chain_type == CHAIN_TYPE_NEAR:
            _set_near_source_tx(
                response_data, build_result.get("tx", {}), sender, router=router,
            )
        else:
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


# Minimum Stage-A slippage for long-tail tokenIn (bps). MOG-class tokens need more than 10 bps.
_MIN_PRESWAP_STAGE_A_SLIPPAGE_BPS = 50


def _is_evm_bluechip_token(chain_int: int, token: Dict) -> bool:
    addr = (token.get("address") or "").lower()
    if not addr or is_native_token(addr):
        return True
    for meta in get_bluechip_tokens(chain_int).values():
        if (meta.get("address") or "").lower() == addr:
            return True
    return False


def _resolve_stage_a_slippage(pre_swap: Dict, slippage: float, chain_int: int, token_in: Dict) -> float:
    """Pick the looser of top-level and preSwap slippage; floor long-tail tokens."""
    decimals = [convert_slippage_to_decimal(slippage)]
    ps = pre_swap.get("slippage") if isinstance(pre_swap, dict) else None
    if ps not in (None, ""):
        try:
            decimals.append(convert_preswap_slippage_to_decimal(ps))
        except (TypeError, ValueError):
            pass
    max_decimal = max(decimals)
    max_bps = max_decimal * 10000
    if chain_int and token_in and not _is_evm_bluechip_token(chain_int, token_in):
        max_bps = max(max_bps, _MIN_PRESWAP_STAGE_A_SLIPPAGE_BPS)
    return max_bps


def _stage_a_build_evm(
    chain_int: int,
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    deposit_address: str,
    preferred_router: str = "",
    preferred_market: str = "",
    return_all: bool = False,
) -> Dict:
    """Build the EVM Stage-A swap tx (exactIn delivered to depositAddress).

    Re-quotes Bitget and OKX at build time and picks the route with the best
    ``minAmountOut`` so quote-time router hints stay consistent with the
    signed transaction.

    When ``return_all`` is True, returns ``{"success": bool, "candidates":
    [...]}`` with every successful build sorted by ``minAmountOut`` descending,
    so the caller can fall back to the next-best route when the top one would
    revert on-chain. Otherwise returns the single best build (legacy shape).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from swap_utils import (
        BITGET_CHAIN_MAP,
        bitget_quote,
        _parse_bitget_quote,
        build_swap_tx,
    )

    slippage_decimal = convert_slippage_to_decimal(slippage)

    def _build_okx() -> Dict:
        res = build_same_chain_swap_tx(
            chain_id=chain_int,
            router="okx",
            token_in=token_in,
            token_out=intermediate,
            amount_in=str(amount_in),
            slippage=slippage,
            sender=sender,
            recipient=deposit_address,
        )
        if not res.get("success"):
            return {"success": False, "router": "okx", "error": res.get("error")}
        return {
            "success": True,
            "router": "okx",
            "tx": res.get("tx") or {},
            "estimatedOut": str(res.get("estimatedOut") or ""),
            "minAmountOut": str(res.get("minAmountOut") or ""),
            "spender": (res.get("tx") or {}).get("to", ""),
        }

    def _build_bitget() -> Dict:
        if chain_int not in BITGET_CHAIN_MAP:
            return {"success": False, "router": "bitget", "error": f"Bitget does not support chain {chain_int}"}

        market = str(preferred_market or "").strip()
        # Always refresh Bitget quote at build time (market hint from /quote may be stale).
        raw = bitget_quote(
            chain_id=chain_int,
            token_in=token_in,
            token_out=intermediate,
            amount_in=str(amount_in),
            slippage=slippage_decimal,
            user_address=sender,
        )
        if not raw.get("success"):
            return {"success": False, "router": "bitget", "error": raw.get("error")}
        parsed = _parse_bitget_quote(raw["data"], intermediate, slippage_decimal)
        if not parsed:
            return {"success": False, "router": "bitget", "error": "Bitget quote unparseable"}
        if parsed.get("estimateRevert"):
            return {"success": False, "router": "bitget", "error": "Bitget estimateRevert"}
        market = str(parsed.get("market") or market).strip()
        if not market:
            return {"success": False, "router": "bitget", "error": "Bitget market missing from quote"}

        res = build_swap_tx(
            chain_id=chain_int,
            router="bitget",
            token_in=token_in,
            token_out=intermediate,
            amount_in=str(amount_in),
            slippage=slippage,
            sender=sender,
            recipient=deposit_address,
            market=market,
        )
        if not res.get("success"):
            return {"success": False, "router": "bitget", "error": res.get("error")}
        return {
            "success": True,
            "router": "bitget",
            "tx": res.get("tx") or {},
            "estimatedOut": str(res.get("estimatedOut") or ""),
            "minAmountOut": str(res.get("minAmountOut") or ""),
            "spender": res.get("approveSpender") or (res.get("tx") or {}).get("to", ""),
            "market": market,
        }

    builders: Dict[str, Any] = {"okx": _build_okx}
    # ===== TEMP TEST START: EVM OKX-only — skip Bitget stage-A build =====
    from swap_utils import _evm_test_okx_only
    if not _evm_test_okx_only():
        if chain_int in BITGET_CHAIN_MAP:
            builders["bitget"] = _build_bitget
    # Original (restore when reverting EVM_TEST_OKX_ONLY):
    # if chain_int in BITGET_CHAIN_MAP:
    #     builders["bitget"] = _build_bitget
    # ===== TEMP TEST END =====

    router_order = [preferred_router] if preferred_router in builders else []
    if _evm_test_okx_only():
        router_order = ["okx"]
    else:
        router_order.extend(r for r in ("bitget", "okx") if r in builders and r not in router_order)

    successful = []
    errors = []
    with ThreadPoolExecutor(max_workers=len(router_order)) as pool:
        futures = {pool.submit(builders[r]): r for r in router_order}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                errors.append(f"{name}: {e}")
                continue
            if not result.get("success"):
                errors.append(f"{name}: {result.get('error')}")
                continue
            successful.append(result)

    # Order candidates so the caller falls back in the right order: the quote's
    # chosen router (preferred_router) first — it was already vetted at /quote as
    # the best EXECUTABLE route — then the rest by min-out descending.
    def _candidate_sort_key(r):
        is_preferred = bool(preferred_router) and r.get("router") == preferred_router
        return (0 if is_preferred else 1, -_safe_decimal(r.get("minAmountOut") or r.get("estimatedOut") or "0"))

    successful.sort(key=_candidate_sort_key)

    detail = "; ".join(errors) if errors else "all EVM stage-A builders failed"

    if return_all:
        return {
            "success": bool(successful),
            "candidates": successful,
            "error": "" if successful else detail,
        }

    if not successful:
        return {"success": False, "error": detail, "router": preferred_router or "evm-aggregate"}

    return successful[0]


def _stage_a_build_solana(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    deposit_address: str,
    preferred_router: str = "",
) -> Dict:
    """Build the Solana Stage-A swap tx delivering output to the 1Click deposit.

    Re-quotes Jupiter, Titan, and OKX at build time (with deposit routing) and
    picks the best executable route so quote-time router hints stay consistent
    with the signed transaction.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from jupiter_utils import jupiter_build
    from titan_utils import titan_order
    from swap_utils import okx_swap
    from solana_tx_assembler import (
        assemble_jupiter_preswap_tx,
        assemble_titan_preswap_tx,
        derive_destination_token_account,
        NATIVE_SOL_MINT,
    )

    intermediate_mint = intermediate.get("address", "")
    is_native_sol = (intermediate_mint or "").lower() == NATIVE_SOL_MINT.lower()

    destination_token_account: Optional[str] = None
    native_destination_account: Optional[str] = None
    if is_native_sol:
        native_destination_account = deposit_address
    else:
        try:
            destination_token_account = derive_destination_token_account(
                deposit_address, intermediate_mint,
            )
        except Exception as e:
            return {"success": False, "error": f"Solana ATA derivation failed: {e}", "router": "jupiter"}

    slippage_decimal = convert_slippage_to_decimal(slippage)
    slippage_bps = max(1, int(Decimal(str(slippage_decimal)) * Decimal("10000")))

    def _build_jupiter() -> Dict:
        build_res = jupiter_build(
            input_mint=token_in.get("address", ""),
            output_mint=intermediate_mint,
            amount=str(amount_in),
            slippage_bps=slippage_bps,
            taker=sender,
            destination_token_account=destination_token_account,
            native_destination_account=native_destination_account,
        )
        if not build_res.get("success"):
            return {"success": False, "router": "jupiter", "error": build_res.get("error")}
        data = build_res.get("data") or {}
        out_amount_str = str(data.get("outAmount") or "")
        other_threshold = str(data.get("otherAmountThreshold") or "")
        if not out_amount_str:
            return {"success": False, "router": "jupiter", "error": "Jupiter build missing outAmount"}
        assembled = assemble_jupiter_preswap_tx(
            sender=sender,
            deposit_address=deposit_address,
            intermediate_mint=intermediate_mint,
            build_resp=data,
        )
        return {
            "success": True,
            "router": "jupiter",
            "tx": assembled,
            "estimatedOut": out_amount_str,
            "minAmountOut": other_threshold or out_amount_str,
            "spender": "",
        }

    def _build_titan() -> Dict:
        if not destination_token_account:
            return {"success": False, "router": "titan", "error": "Titan preswap requires SPL intermediate ATA"}
        res = titan_order(
            input_mint=token_in.get("address", ""),
            output_mint=intermediate_mint,
            amount=str(amount_in),
            slippage_bps=slippage_bps,
            taker=sender,
            destination_token_account=destination_token_account,
        )
        if not res.get("success"):
            return {"success": False, "router": "titan", "error": res.get("error")}
        data = res.get("data") or {}
        out_amount_str = str(data.get("outAmount") or "")
        if not out_amount_str:
            return {"success": False, "router": "titan", "error": "Titan quote missing outAmount"}
        assembled = assemble_titan_preswap_tx(
            sender=sender,
            deposit_address=deposit_address,
            intermediate_mint=intermediate_mint,
            titan_data=data,
        )
        min_out = int(Decimal(out_amount_str) * (Decimal("1") - Decimal(str(slippage_decimal))))
        return {
            "success": True,
            "router": "titan",
            "tx": assembled,
            "estimatedOut": out_amount_str,
            "minAmountOut": str(max(min_out, 0)),
            "spender": "",
        }

    def _build_okx() -> Dict:
        res = okx_swap(
            chain_id=501,
            token_in=token_in,
            token_out=intermediate,
            amount_in=str(amount_in),
            slippage=slippage_decimal,
            from_address=sender,
            to_address=deposit_address,
        )
        if not res.get("success"):
            return {"success": False, "router": "okx", "error": res.get("error")}
        data = res.get("data") or {}
        if str(data.get("code")) != "0" or not data.get("data"):
            return {"success": False, "router": "okx", "error": data.get("msg", "OKX Solana swap error")}
        tx_data = data["data"][0] if isinstance(data["data"], list) else data["data"]
        tx = tx_data.get("tx") or tx_data
        router_result = tx_data.get("routerResult") or {}
        out_amount_str = str(router_result.get("toTokenAmount") or router_result.get("toAmount") or "")
        from solana_tx_assembler import okx_solana_tx_to_base64

        from solana_tx_assembler import enrich_solana_tx_envelope
        from swap_utils import _solana_alt_pubkeys_from_provider

        tx_b64 = okx_solana_tx_to_base64(tx if isinstance(tx, dict) else None)
        if not tx_b64:
            return {"success": False, "router": "okx", "error": "OKX Solana swap missing tx.data"}
        tx_envelope = enrich_solana_tx_envelope(
            {"transaction": tx_b64, "format": "base64"},
            alt_pubkeys=_solana_alt_pubkeys_from_provider("okx", data),
        )
        return {
            "success": True,
            "router": "okx",
            "tx": tx_envelope,
            "estimatedOut": out_amount_str,
            "minAmountOut": out_amount_str,
            "spender": "",
        }

    builders = {
        "jupiter": _build_jupiter,
        "titan": _build_titan,
        "okx": _build_okx,
    }
    router_order = [preferred_router] if preferred_router in builders else []
    router_order.extend(r for r in ("titan", "jupiter", "okx") if r not in router_order)

    best = None
    best_min = Decimal("-1")
    errors = []
    with ThreadPoolExecutor(max_workers=len(router_order)) as pool:
        futures = {pool.submit(builders[r]): r for r in router_order}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                errors.append(f"{name}: {e}")
                continue
            if not result.get("success"):
                errors.append(f"{name}: {result.get('error')}")
                continue
            try:
                min_val = Decimal(str(result.get("minAmountOut") or result.get("estimatedOut") or "0"))
            except (InvalidOperation, ValueError):
                min_val = Decimal("0")
            if min_val > best_min:
                best_min = min_val
                best = result

    if not best:
        detail = "; ".join(errors) if errors else "all Solana stage-A builders failed"
        return {"success": False, "error": detail, "router": preferred_router or "solana-aggregate"}

    return best


def _stage_a_build_near(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    deposit_address: str,
    preferred_router: str = "",
) -> Dict:
    """Build the NEAR Stage-A Ref SmartRouter tx delivering output to 1Click deposit."""
    from near_smart_router_swap import (
        near_same_chain_build_tx,
        ROUTER_NEAR_REF_SMARTROUTER,
        ROUTER_NEAR_SMARTX,
    )

    router = (preferred_router or ROUTER_NEAR_REF_SMARTROUTER).strip()
    if router not in (ROUTER_NEAR_REF_SMARTROUTER, ROUTER_NEAR_SMARTX):
        router = ROUTER_NEAR_REF_SMARTROUTER
    slippage_decimal = convert_slippage_to_decimal(slippage)
    res = near_same_chain_build_tx(
        router=router,
        token_in=token_in,
        token_out=intermediate,
        amount_in=str(amount_in),
        slippage_decimal=float(slippage_decimal),
        sender=sender,
        recipient=deposit_address,
    )
    if not res.get("success"):
        return {"success": False, "error": res.get("error"), "router": router}

    return {
        "success": True,
        "router": res.get("router") or router,
        "tx": res.get("tx") or {},
        "estimatedOut": res.get("estimatedOut") or "",
        "minAmountOut": res.get("minAmountOut") or "",
        "spender": "",
    }


def _stage_a_build_aptos(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    deposit_address: str,
    preferred_router: str = "",
) -> Dict:
    """Build Aptos Stage-A Hyperion swap tx with output to 1Click deposit."""
    from hyperion_utils import hyperion_build_swap_tx
    from swap_utils import convert_slippage_to_decimal

    slippage_decimal = convert_slippage_to_decimal(slippage)
    # Panora removed — Aptos Stage-A always routes through Hyperion CLMM.
    router = "hyperion"

    res = hyperion_build_swap_tx(
        token_in=token_in.get("address", ""),
        token_out=intermediate.get("address", ""),
        amount_in=str(amount_in),
        slippage_decimal=float(slippage_decimal),
        recipient=deposit_address,
        safe_mode=False,
    )

    if not res.get("success"):
        return {"success": False, "error": res.get("error"), "router": router}

    tx = res.get("tx") or {}
    # Hyperion `router_v3::swap_batch` uses arguments[5] as recipient (string).
    args = tx.get("arguments")
    if isinstance(args, list) and len(args) >= 6:
        args[5] = deposit_address

    return {
        "success": True,
        "router": res.get("router") or router,
        "tx": tx,
        "estimatedOut": str(res.get("estimatedOut") or ""),
        "minAmountOut": str(res.get("minAmountOut") or ""),
        "spender": "",
    }


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
    oneclick_extensions: Optional[Dict] = None,
) -> Dict:
    """Build the two-stage cross-chain swap.

    Expects `pre_swap` and `bridge` sub-objects from the /api/swap/quote response
    (they describe the locked intermediate and amounts). The user signs ONE swap
    tx (Stage A delivered to the 1Click depositAddress) plus one optional approve
    (EVM only — Solana has no ERC-20-style approve).

    Supports EVM, Solana, NEAR, and Aptos fromChain. Stage-A uses chain-appropriate
    aggregators; the user signs ONE swap tx delivering intermediate tokens to
    the 1Click depositAddress (plus optional EVM approve).

    Response shape matches the direct cross-chain swap (`tx` + `approve` + `deposit`)
    with an additional `preSwap` block carrying Stage-A metadata and route info.
    """
    if not pre_swap or not isinstance(pre_swap, dict):
        return {"code": -1, "msg": "preSwap is required for two-stage route (from /api/swap/quote response)"}
    if not bridge or not isinstance(bridge, dict):
        return {"code": -1, "msg": "bridge is required for two-stage route (from /api/swap/quote response)"}

    is_solana_src = _is_solana_chain(from_chain)
    is_near_src = _is_near_chain(from_chain)
    is_aptos_src = _is_aptos_chain(from_chain)
    chain_int = None if (is_solana_src or is_near_src or is_aptos_src) else _chain_id_int(from_chain)
    if not is_solana_src and not is_near_src and not is_aptos_src and chain_int is None:
        return {"code": -1, "msg": f"pre-swap route does not yet support fromChain={from_chain}"}

    intermediate = pre_swap.get("tokenOut") or {}
    if not isinstance(intermediate, dict) or not intermediate.get("address"):
        return {"code": -1, "msg": "preSwap.tokenOut missing or invalid"}

    # Target intermediate amount we promised 1Click during quote; use the locked value so
    # quote/swap stay consistent.
    mid_target_str = str(pre_swap.get("amountOutTarget") or pre_swap.get("amountOut") or "")
    mid_target = _safe_int_str(mid_target_str)
    if mid_target <= 0:
        return {"code": -1, "msg": "preSwap.amountOutTarget is required and must be a positive integer"}

    stage_a_slippage = slippage
    if not is_solana_src and not is_near_src and not is_aptos_src and chain_int is not None:
        stage_a_slippage = _resolve_stage_a_slippage(pre_swap, slippage, chain_int, token_in)

    try:
        # 1) Stage B: create the 1Click order (dry=false) so we have a depositAddress.
        #    Use FLEX_INPUT because Stage A delivers a variable on-chain amount to
        #    the deposit address (`amount = mid_target` is the target); the bridge
        #    must accept whatever actually arrives. Matches the FLEX_INPUT quote.
        near_res = nearintents_build_tx(
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=intermediate,
            token_out=token_out,
            amount_in=str(mid_target),
            sender=sender,
            recipient=recipient,
            slippage=slippage,
            oneclick_extensions=oneclick_extensions,
            swap_type="FLEX_INPUT",
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

        # 2) Stage A candidates (best-first). EVM returns the full Bitget+OKX
        #    list so we can fall back to the next route when the top one would
        #    revert on-chain (thin-liquidity tokens that Bitget quotes but cannot
        #    execute). Non-EVM chains have a single builder and no downstream
        #    eth_call simulation, so their candidate list has one entry.
        if is_solana_src:
            single = _stage_a_build_solana(
                token_in=token_in,
                intermediate=intermediate,
                amount_in=str(amount_in),
                slippage=stage_a_slippage,
                sender=sender,
                deposit_address=deposit_address,
                preferred_router=str(pre_swap.get("router") or ""),
            )
            stage_a_candidates = [single] if single.get("success") else []
            build_err = single.get("error")
        elif is_near_src:
            single = _stage_a_build_near(
                token_in=token_in,
                intermediate=intermediate,
                amount_in=str(amount_in),
                slippage=stage_a_slippage,
                sender=sender,
                deposit_address=deposit_address,
                preferred_router=str(pre_swap.get("router") or ""),
            )
            stage_a_candidates = [single] if single.get("success") else []
            build_err = single.get("error")
        elif is_aptos_src:
            single = _stage_a_build_aptos(
                token_in=token_in,
                intermediate=intermediate,
                amount_in=str(amount_in),
                slippage=stage_a_slippage,
                sender=sender,
                deposit_address=deposit_address,
                preferred_router=str(pre_swap.get("router") or ""),
            )
            stage_a_candidates = [single] if single.get("success") else []
            build_err = single.get("error")
        else:
            evm_build = _stage_a_build_evm(
                chain_int=chain_int,
                token_in=token_in,
                intermediate=intermediate,
                amount_in=str(amount_in),
                slippage=stage_a_slippage,
                sender=sender,
                deposit_address=deposit_address,
                preferred_router=str(pre_swap.get("router") or ""),
                preferred_market=str(pre_swap.get("market") or ""),
                return_all=True,
            )
            stage_a_candidates = evm_build.get("candidates") or []
            build_err = evm_build.get("error")

        if not stage_a_candidates:
            return {"code": -1, "msg": f"Pre-swap tx build failed: {build_err}"}

        source_chain_type = (
            CHAIN_TYPE_SOLANA if is_solana_src
            else (CHAIN_TYPE_NEAR if is_near_src
                  else (CHAIN_TYPE_APTOS if is_aptos_src else CHAIN_TYPE_EVM))
        )

        is_evm_src = not is_solana_src and not is_near_src and not is_aptos_src

        # Phase 1 — select an executable Stage-A route best-first. EVM routes are
        # simulated; if the top route would revert on-chain (e.g. Bitget quotes a
        # thin-liquidity SolvBTC path it cannot execute), fall back to the next.
        # Response assembly is deferred to Phase 2 so we can re-anchor the bridge
        # to whichever route is actually chosen (see C below).
        chosen_stage_a = None
        chosen_stage_a_router = ""
        chosen_sim_skipped = None
        fallback_failure = None
        tried_routers = []
        for stage_a in stage_a_candidates:
            stage_a_tx = stage_a.get("tx") or {}
            stage_a_estimated_out = stage_a.get("estimatedOut") or ""
            stage_a_min_out = stage_a.get("minAmountOut") or ""
            stage_a_router = stage_a.get("router") or (
                "jupiter" if is_solana_src
                else ("near-ref-smart" if is_near_src
                      else ("hyperion" if is_aptos_src else "okx"))
            )
            tried_routers.append(stage_a_router)

            stage_a_min_int = _safe_int_str(stage_a_min_out)
            # Non-EVM legs cannot re-anchor (single builder, no eth_call sim), so keep
            # the strict guard: stage-A min-out must cover the bridge's expected input.
            if not is_evm_src and stage_a_min_int > 0 and stage_a_min_int < mid_target:
                fallback_failure = {
                    "code": -2,
                    "msg": "Price moved too much, please re-quote",
                    "data": {
                        "preSwapMinOut": str(stage_a_min_out or ""),
                        "bridgeExpectedIn": str(mid_target),
                        "quoteMinAmountOut": str(quote_min_amount_out or ""),
                        "stageARouter": stage_a_router,
                    },
                }
                continue

            # EVM: simulate Stage-A swap (skips when allowance missing — user signs
            # approve first). On revert, fall back to the next candidate route.
            if is_evm_src and stage_a_tx.get("to") and stage_a_tx.get("data"):
                sim_spender = stage_a.get("spender") or stage_a_tx.get("to", "")
                sim = simulate_preswap_evm_swap(
                    chain_int,
                    sender,
                    token_in.get("address", ""),
                    str(amount_in),
                    sim_spender,
                    stage_a_tx,
                )
                if sim.get("skipped") and sim.get("reason") == "approve_required":
                    # Allowance missing -> a plain eth_call would falsely revert. For
                    # Bitget (which can win on price yet quote an unexecutable route),
                    # verify the route with balance+allowance injected (state override)
                    # and fall back on revert. Other routers are accepted pending
                    # approval (user signs approve first, then swap).
                    if stage_a_router == "bitget":
                        from evm_sim_utils import simulate_swap_funded as _sim_funded
                        from swap_utils import EVM_RPC_FALLBACK as _RPC_FB
                        fsim = _sim_funded(
                            chain_int,
                            _RPC_FB.get(int(chain_int)) or [],
                            sender,
                            token_in.get("address", ""),
                            sim_spender,
                            str(amount_in),
                            stage_a_tx,
                        )
                        if not fsim.get("success") and not fsim.get("skipped"):
                            fallback_failure = {
                                "code": -2,
                                "msg": "Pre-swap would revert on-chain; please re-quote",
                                "data": {
                                    "simulateError": fsim.get("error", ""),
                                    "amountOutTarget": str(mid_target),
                                    "stageAEstimatedOut": str(stage_a_estimated_out or ""),
                                    "stageARouter": stage_a_router,
                                    "verifiedFunded": True,
                                    "hint": "Bitget route is not executable on-chain for this token; re-quote (will route via OKX).",
                                },
                            }
                            logger.warning(
                                "preswap stage-A bitget unexecutable (funded sim: %s); trying next route",
                                fsim.get("error", ""),
                            )
                            continue
                    chosen_sim_skipped = "approve_required"
                    chosen_stage_a = stage_a
                    chosen_stage_a_router = stage_a_router
                    break
                if not sim.get("success") and not sim.get("skipped"):
                    slip_bps = convert_slippage_to_decimal(stage_a_slippage) * 10000
                    est_int = _safe_int_str(stage_a_estimated_out)
                    min_feasible = (
                        int(est_int * (1 - convert_slippage_to_decimal(stage_a_slippage)))
                        if est_int > 0 else 0
                    )
                    hint = (
                        "Stale quote or insufficient DEX liquidity for amountOutTarget; "
                        "call /quote again and submit /swap immediately (do not reuse old preSwap/bridge)"
                    )
                    if slip_bps >= 100 and est_int > 0 and min_feasible >= mid_target:
                        hint = (
                            "Slippage is already wide enough; likely stale preSwap/bridge snapshot or "
                            "thin pool liquidity. Re-quote and swap within ~1 minute."
                        )
                    fallback_failure = {
                        "code": -2,
                        "msg": "Pre-swap would revert on-chain; please re-quote",
                        "data": {
                            "simulateError": sim.get("error", ""),
                            "stageASlippageBps": int(slip_bps),
                            "amountOutTarget": str(mid_target),
                            "stageAEstimatedOut": str(stage_a_estimated_out or ""),
                            "stageAMinFeasibleOut": str(min_feasible),
                            "allowance": sim.get("allowance", ""),
                            "stageARouter": stage_a_router,
                            "hint": hint,
                        },
                    }
                    logger.warning(
                        "preswap stage-A %s would revert (%s); trying next route",
                        stage_a_router, sim.get("error", ""),
                    )
                    continue
                # Simulation passed.
                chosen_stage_a = stage_a
                chosen_stage_a_router = stage_a_router
                break

            # Non-EVM (or no simulatable tx): accept the first successful build.
            chosen_stage_a = stage_a
            chosen_stage_a_router = stage_a_router
            break

        if chosen_stage_a is None:
            # Every candidate failed the safety/simulation gate.
            if fallback_failure is not None:
                if isinstance(fallback_failure.get("data"), dict):
                    fallback_failure["data"]["triedRouters"] = tried_routers
                return fallback_failure
            return {
                "code": -2,
                "msg": "Pre-swap would revert on-chain; please re-quote",
                "data": {"triedRouters": tried_routers},
            }

        stage_a = chosen_stage_a
        stage_a_router = chosen_stage_a_router
        stage_a_tx = stage_a.get("tx") or {}
        stage_a_estimated_out = stage_a.get("estimatedOut") or ""
        stage_a_min_out = stage_a.get("minAmountOut") or ""

        # C — re-anchor the bridge to the chosen route (EVM only). The quote-locked
        # `mid_target` reflects the quote winner (often Bitget). When we fall back to
        # a lower-output route (e.g. OKX), its guaranteed min-out can be below that
        # target, which would under-fill the bridge. Re-create the 1Click order
        # anchored to the chosen route's deliverable and rebuild its Stage-A tx to the
        # new deposit address, so quote/swap stay self-consistent. The final deviation
        # check below then honestly compares against the user's quoteMinAmountOut.
        reanchored_router = ""
        chosen_min_int = _safe_int_str(stage_a_min_out)
        if is_evm_src and chosen_min_int > 0 and chosen_min_int < mid_target:
            new_mid = chosen_min_int
            near_res2 = nearintents_build_tx(
                from_chain=from_chain,
                to_chain=to_chain,
                token_in=intermediate,
                token_out=token_out,
                amount_in=str(new_mid),
                sender=sender,
                recipient=recipient,
                slippage=slippage,
                oneclick_extensions=oneclick_extensions,
                swap_type="FLEX_INPUT",
            )
            if not near_res2.get("success"):
                return {"code": -1, "msg": f"Bridge re-anchor failed: {near_res2.get('error')}"}
            cross_tx = near_res2.get("tx", {}) or {}
            new_deposit = cross_tx.get("depositAddress", "")
            if not new_deposit:
                return {"code": -1, "msg": "1Click did not return depositAddress on re-anchor"}
            rebuilt = _stage_a_build_evm(
                chain_int=chain_int,
                token_in=token_in,
                intermediate=intermediate,
                amount_in=str(amount_in),
                slippage=stage_a_slippage,
                sender=sender,
                deposit_address=new_deposit,
                preferred_router=stage_a_router,
                preferred_market=str(stage_a.get("market") or ""),
                return_all=False,
            )
            if not rebuilt.get("success"):
                return {"code": -1, "msg": f"Stage-A re-anchor build failed: {rebuilt.get('error')}"}
            stage_a = rebuilt
            stage_a_router = rebuilt.get("router") or stage_a_router
            stage_a_tx = rebuilt.get("tx") or {}
            stage_a_estimated_out = rebuilt.get("estimatedOut") or ""
            stage_a_min_out = rebuilt.get("minAmountOut") or ""
            deposit_address = new_deposit
            deposit_memo = cross_tx.get("depositMemo", "")
            order_id = cross_tx.get("orderId", deposit_address)
            bridge_estimated_out = cross_tx.get("estimatedOut", "")
            bridge_min_out = cross_tx.get("minAmountOut", "")
            mid_target = new_mid
            reanchored_router = stage_a_router

        # Phase 2 — assemble the response for the chosen (possibly re-anchored) route.
        response_data = _build_common_response_data(
            is_cross_chain=True,
            source_chain_type=source_chain_type,
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            router=_PRESWAP_ROUTER_NAME,
        )
        # Top-level `tx` is the user-signed stage-A tx (it alone triggers both stages).
        if is_near_src:
            _set_near_source_tx(
                response_data, stage_a_tx, sender, router=_PRESWAP_ROUTER_NAME,
            )
        else:
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
        pre_swap_resp = {
            "router": stage_a_router,
            "chainType": source_chain_type,
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
        if isinstance(stage_a_tx, dict):
            pre_swap_resp["addressLookupTableAddresses"] = (
                stage_a_tx.get("addressLookupTableAddresses") or []
            )
        if len(tried_routers) > 1:
            pre_swap_resp["routersTried"] = tried_routers
        if reanchored_router:
            pre_swap_resp["bridgeReanchored"] = True
        response_data["preSwap"] = pre_swap_resp
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
        if chosen_sim_skipped == "approve_required":
            response_data["simulateSkipped"] = "approve_required"
            response_data["simulateNote"] = (
                "Allowance to the swap router is insufficient; sign approve first, then swap"
            )

        # 3) Approve info: EVM only. Solana and NEAR have no ERC-20-style approve.
        #    Built once for the chosen route (after fallback selection).
        if not is_solana_src and not is_near_src and not is_native_token(token_in.get("address", "")):
            approve_amount = str(amount_in)
            approve_spender = stage_a.get("spender") or stage_a_tx.get("to", "")
            # Must match Stage-A router: Bitget swap pulls via tx.to; OKX uses dexContractAddress.
            approve_router = stage_a_router if stage_a_router in ("okx", "bitget") else "okx"
            approve_res = build_same_chain_approve_tx(
                chain_id=chain_int,
                router=approve_router,
                token_address=token_in.get("address", ""),
                approve_amount=approve_amount,
                spender=approve_spender,
            )
            if approve_res.get("success"):
                approve_tx = approve_res.get("tx")
                if approve_tx:
                    approve_spender_out = approve_res.get("dexContractAddress", approve_spender)
                    if approve_router == "bitget":
                        swap_executor = (stage_a_tx.get("to") or "").lower()
                        if swap_executor and normalize_evm_address(approve_spender_out).lower() != swap_executor:
                            logger.warning(
                                "preswap bitget approve spender %s != swap tx.to %s",
                                approve_spender_out,
                                swap_executor,
                            )
                    response_data["approve"] = {
                        "tx": approve_tx,
                        "spender": approve_spender_out,
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
    oneclick_extensions: Optional[Dict] = None,
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
            oneclick_extensions=oneclick_extensions,
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
                oneclick_extensions=oneclick_extensions,
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
        elif source_chain_type == CHAIN_TYPE_NEAR:
            tx_payload = build_near_deposit_tx(
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
                sender=sender,
                deposit_memo=deposit_memo,
                network_id=str(Cfg.NETWORK_ID),
                skip_implicit_bootstrap=True,
            )
            _set_near_source_tx(response_data, tx_payload, sender, router=router)
            response_data["approve"] = None
        elif source_chain_type == CHAIN_TYPE_SOLANA:
            response_data["tx"] = build_solana_deposit_tx(
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
                decimals=int(token_in.get("decimals", 6)),
                deposit_memo=deposit_memo,
                sender=sender,
            )
        elif source_chain_type == CHAIN_TYPE_SUI:
            response_data["tx"] = build_sui_deposit_tx(
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
                decimals=int(token_in.get("decimals", 9)),
                sender=sender,
                deposit_memo=deposit_memo,
            )
            response_data["approve"] = None
        elif source_chain_type == CHAIN_TYPE_TRON:
            response_data["tx"] = build_tron_deposit_tx(
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
                decimals=int(token_in.get("decimals", 6)),
                sender=sender,
                deposit_memo=deposit_memo,
            )
            response_data["approve"] = None
        elif source_chain_type == CHAIN_TYPE_UTXO:
            response_data["tx"] = build_utxo_deposit_tx(
                chain_id=from_chain,
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
                decimals=int(token_in.get("decimals", 8)),
                symbol=token_in.get("symbol", ""),
                deposit_memo=deposit_memo,
            )
            response_data["approve"] = None
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
