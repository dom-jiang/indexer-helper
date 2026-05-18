"""
Unified Swap API dispatch layer.

Routes requests to same-chain or cross-chain handlers based on fromChain vs toChain.
For cross-chain: runs OmniBridge and NearIntents 1Click in parallel, picks best price.
For same-chain:  delegates to existing multi_chain_* functions.
"""

import base64
import json
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
    BLUECHIP_TOKENS, SOLANA_BLUECHIP_TOKENS,
    is_native_token, normalize_evm_address,
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
    is_chain_native_token,
)
from cross_chain_tx_builder import (
    build_evm_deposit_tx, build_aptos_deposit_tx, build_solana_deposit_tx,
    build_near_deposit_tx, NEAR_NATIVE_ALIASES,
    build_sui_deposit_tx, build_tron_deposit_tx, build_utxo_deposit_tx,
    SUI_NATIVE_TYPE,
)
from db_provider import add_multichain_lending_requests
from near_same_chain_mca import near_same_chain_mca_applies, resolve_near_mca_deposit_receiver
from near_mca_withdraw_tx import build_near_mca_withdraw_exec_tx_payload


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

    ab = (
        mca_block.get("amountBurrow")
        or mca_block.get("amount_with_inner_decimal")
        or mca_block.get("amount_burrow")
    )
    amt_br = str(ab).strip() if ab is not None and str(ab).strip() else str(amount_in)

    tid = str((token_in or {}).get("address") or "").strip()
    if not tid:
        raise ValueError("tokenIn address required")

    return build_near_mca_withdraw_exec_tx_payload(
        network_id=Cfg.NETWORK_ID,
        mca_account_id=mca_id,
        token_id=tid,
        amount_token_smallest=str(amount_in),
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
        # code (OKX / Bitget / Jupiter / Panora adapters) can re-detect native via
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
_PRESWAP_EVM_INTERMEDIATE_SYMBOLS = ("USDC", "USDT", "WETH")
# Order matters: USDC first (best 1Click + Jupiter liquidity), USDT second,
# native SOL last (no ATA hop needed but typically smallest 1Click pool).
_PRESWAP_SOLANA_INTERMEDIATE_SYMBOLS = ("USDC", "USDT", "SOL")


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

    Supports both EVM and Solana fromChain. Aptos / other non-EVM chains fall
    through to a "not yet supported" message — extend here when adding more.
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


def _stage_a_quote_evm(
    chain_int: int,
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage_decimal: float,
    sender: str,
) -> Tuple[Optional[Decimal], Optional[str], Optional[str]]:
    """Run an OKX exactIn quote for the EVM Stage-A leg.

    Returns ``(mid_amount, router, error)``. `mid_amount` is the unbuffered
    estimated output (Decimal); the caller applies the slippage buffer.
    """
    from swap_utils import okx_quote as _okx_quote_raw, _parse_okx_quote as _okx_parse
    raw = _okx_quote_raw(
        chain_id=chain_int,
        token_in=token_in,
        token_out=intermediate,
        amount_in=str(amount_in),
        slippage=slippage_decimal,
        user_address=sender,
    )
    if not raw.get("success"):
        return None, None, f"OKX quote failed: {raw.get('error')}"
    parsed = _okx_parse(raw["data"], intermediate, slippage_decimal)
    if not parsed:
        return None, None, "OKX quote unparseable"
    try:
        return Decimal(parsed["amountOut"]), "okx", None
    except (InvalidOperation, ValueError) as e:
        return None, None, f"OKX amountOut invalid: {e}"


def _stage_a_quote_solana(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage_decimal: float,
    sender: str,
) -> Tuple[Optional[Decimal], Optional[str], Optional[str]]:
    """Run a Jupiter + OKX parallel Stage-A quote for the Solana leg.

    Returns ``(mid_amount, router, error)``. `router` identifies which
    aggregator won the price competition so Stage-B build can match it.
    """
    from swap_utils import aggregate_solana_quote
    res = aggregate_solana_quote(
        token_in=token_in,
        token_out=intermediate,
        amount_in=str(amount_in),
        slippage=slippage_decimal,
        sender=sender,
        recipient=sender,
    )
    if not res.get("success"):
        return None, None, f"Solana aggregate quote failed: {res.get('error')}"
    quote = res.get("quote") or {}
    out_str = str(quote.get("amountOut") or "")
    if not out_str:
        return None, None, "Solana aggregate quote missing amountOut"
    try:
        return Decimal(out_str), str(quote.get("router") or "jupiter"), None
    except (InvalidOperation, ValueError) as e:
        return None, None, f"Solana amountOut invalid: {e}"


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

    Supports EVM and Solana fromChain. Stage-A uses chain-appropriate
    aggregators (OKX for EVM, Jupiter+OKX for Solana); Stage-B is always
    NearIntents 1Click.
    """
    is_solana_src = _is_solana_chain(from_chain)
    chain_int = None if is_solana_src else _chain_id_int(from_chain)
    if not is_solana_src and chain_int is None:
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

    for inter in candidates:
        try:
            if is_solana_src:
                mid_amount_raw, stage_a_router, err = _stage_a_quote_solana(
                    token_in, inter, str(amount_in), slippage_decimal, sender,
                )
            else:
                mid_amount_raw, stage_a_router, err = _stage_a_quote_evm(
                    chain_int, token_in, inter, str(amount_in), slippage_decimal, sender,
                )
            if err or mid_amount_raw is None:
                errors.append(f"{inter['symbol']}: {err}")
                continue

            # Target a slightly lower mid amount so stage-A is still feasible at swap time.
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
                oneclick_extensions=oneclick_extensions,
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
                    # `router` is informational here; the build step picks the
                    # actual aggregator at swap time (parallel quote on Solana,
                    # OKX-only on EVM). Persist what won quote-time so logs
                    # and the frontend can display the chosen path.
                    "router": stage_a_router or ("solana-aggregate" if is_solana_src else "okx"),
                    "chainType": CHAIN_TYPE_SOLANA if is_solana_src else CHAIN_TYPE_EVM,
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
    near_same_chain_mca: bool = False,
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
        ctx["depositOneClickConfigured"] = bool(deposit_extras_scheduled) and bool(is_cross_chain)
        if near_same_chain_mca:
            ctx["nearDirectToLending"] = True
            ctx["routerForSwap"] = ROUTER_NEAR_MCA_DEPOSIT
        if isinstance(mca_block, dict) and mca_block.get("depositCrmAutoFilled"):
            ctx["customRecipientMsgAutoFilled"] = True
        if near_same_chain_mca:
            ctx["note"] = (
                "Same-chain NEAR deposit to Lending: `/swap` with `router=near-mca-deposit` returns NEAR `ft_transfer_call` "
                "(`customRecipientMsg` as `msg`). No DEX and no 1Click bridge leg."
            )
        else:
            ctx["note"] = (
                "When NearIntents 1Click is selected (two-stage Stage-B counts), extras from "
                "`mca` are merged into 1Click /v0/quote. OmniBridge-only quotes ignore them. "
                "Same-chain quotes never hit 1Click — deposit extras are not applied."
            )
    elif flow == "withdraw":
        if ctx.get("mcaAccountId"):
            ctx["intentsRefundToSuggested"] = ctx["mcaAccountId"]
        if near_same_chain_mca:
            ctx["nearDirectFromLending"] = True
            ctx["routerForSwap"] = ROUTER_NEAR_MCA_WITHDRAW
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
) -> None:
    """
    MCA Burrow withdraw on NEAR + ft_transfer to 1Click `depositAddress` (matches `withdrawFromMca` without simpleWithdrawData).

    - `mca.signer.chain` in (`near`, `near-mainnet`): `submissionMode=near_exec`, `nearExecWalletPreview` for MCA `exec`.
    - Otherwise: `submissionMode=multichain_relayer`, `messageToSign` + `mcaRelayer` fields.

    Requires `nearintents_build_tx` to succeed. Writes `data['mcaWithdrawToIntents']`.
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

        is_near_signer = sign_chain in ("near", "near-mainnet")

        mca_acc = (
            mca_block.get("mcaAccountId")
            or mca_block.get("mca_id")
            or mca_block.get("mca")
        )
        if not mca_acc:
            return

        amt_borrow = (
            mca_block.get("amountBurrow")
            or mca_block.get("amount_burrow")
            or mca_block.get("amount_with_inner_decimal")
            or amount_in
        )
        tid = str((token_in or {}).get("address") or "").strip()
        if not tid:
            return

        front_target = (
            "near"
            if _unified_chain_to_oneclick_slug(to_chain) == "near"
            else str(to_chain)
        )

        build_res = nearintents_build_tx(
            from_chain=from_chain,
            to_chain=to_chain,
            token_in=token_in,
            token_out=token_out,
            amount_in=str(amount_in),
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

        business = assemble_mca_withdraw_to_intents_business(
            network_id=nw,
            mca_account_id=mca_s,
            token_id_nep141=tid,
            amount_token_smallest=str(amount_in),
            amount_burrow_inner=str(amt_borrow),
            intents_deposit_address=dep,
            frontend_target_chain=front_target,
            sign_chain_is_near=is_near_signer,
            simple_withdraw_tx=None,
        )

        reg_txs = build_mca_register_token_tx_requests(nw, tid, mca_s)

        submission_mode = "near_exec" if is_near_signer else "multichain_relayer"
        out: Dict[str, Any] = {
            "version": 1,
            "submissionMode": submission_mode,
            "depositAddress": dep,
            "depositMemo": tx_wrap.get("depositMemo") or "",
            "business": business,
            "mcaAccountId": mca_s,
            "tokenId": tid,
            "amountIn": str(amount_in),
            "amountBurrowInner": str(amt_borrow),
        }

        if is_near_signer:
            exec_signer_near = str(
                mca_block.get("execSignerAccountId")
                or mca_block.get("exec_signer_near")
                or mca_block.get("nearSignerAccountId")
                or signer_obj.get("identityKey")
                or signer_obj.get("identity_key")
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
                amount_token_smallest=str(amount_in),
                amount_burrow_inner=str(amt_borrow),
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
) -> Dict:
    """Synthetic quote when NEAR wallet moves NEP-141 to/from Lending without DEX/1Click."""
    flow = _mca_flow(mca_block)
    router = ROUTER_NEAR_MCA_DEPOSIT if flow == "deposit" else ROUTER_NEAR_MCA_WITHDRAW
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

    # Signable NEAR actions for MCA *deposit* — mirrors `_near_same_chain_mca_deposit_swap`
    # / `build_near_deposit_tx` (wallet-selector / near-api-js shape).
    if flow == "deposit":
        crm = str(
            (mca_block or {}).get("customRecipientMsg")
            or (mca_block or {}).get("custom_recipient_msg")
            or ""
        ).strip()
        dep = resolve_near_mca_deposit_receiver(mca_block or {}, recipient)
        if crm and dep and (sender or "").strip():
            try:
                data["nearDepositTx"] = build_near_deposit_tx(
                    token_address=token_in.get("address", ""),
                    deposit_address=dep,
                    amount_smallest=str(amount_in),
                    sender=str(sender).strip(),
                    deposit_memo=crm,
                )
                bq["txPreviewAvailable"] = True
            except Exception as e:
                logger.warning(f"_synthetic_near_same_chain_mca_quote build_near_deposit_tx: {e}")
                data["nearDepositTxError"] = str(e)
        else:
            data["nearDepositTxError"] = (
                "missing customRecipientMsg, deposit receiver, or sender — cannot assemble ft_transfer_call preview"
            )

    elif flow == "withdraw":
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
    response_data["tx"] = tx_payload
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


def _unified_mca_relayer_submit(payload: Dict) -> Dict:
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
        return {
            "code": 0,
            "msg": "success",
            "data": {
                "batchId": str(batch_id),
                "submissionType": "mca_relayer",
            },
        }
    except Exception as e:
        logger.exception(f"_unified_mca_relayer_submit error: {e}")
        return {"code": -1, "msg": str(e), "data": None}


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

    near_dir = (not _is_cross_chain(from_chain, to_chain)) and near_same_chain_mca_applies(
        from_chain, to_chain, token_in_info, token_out_info, mca_enriched
    )

    if not _is_cross_chain(from_chain, to_chain):
        if near_dir:
            flow_nd = _mca_flow(mca_enriched) if isinstance(mca_enriched, dict) else ""
            if flow_nd == "deposit":
                crm_chk = (mca_enriched or {}).get("customRecipientMsg") or (mca_enriched or {}).get("custom_recipient_msg")
                if not (isinstance(crm_chk, str) and crm_chk.strip()):
                    return {
                        "code": -1,
                        "msg": "NEAR same-chain Lending deposit requires customRecipientMsg (use mca.signer + mcaAccountId for server CRM).",
                        "data": None,
                    }
            elif flow_nd == "withdraw":
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
        is_cross_chain=_is_cross_chain(from_chain, to_chain),
        near_same_chain_mca=near_dir,
    )
    if mc_ctx is not None and isinstance(resp, dict) and resp.get("code") == 0:
        dat = resp.get("data")
        if isinstance(dat, dict):
            dat["mcaContext"] = mc_ctx

    if (
        isinstance(resp, dict)
        and resp.get("code") == 0
        and isinstance(resp.get("data"), dict)
        and _is_cross_chain(from_chain, to_chain)
        and not near_dir
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
        )

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
    mca_block: Optional[Dict] = None,
) -> Dict:
    """Run OmniBridge + NearIntents quotes in parallel, return best."""
    oneclick_extensions = _mca_deposit_extensions(mca_block)
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
            oneclick_extensions=oneclick_extensions,
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
    mca_relayer: Optional[Dict] = None,
    mca_oneclick: Optional[Dict] = None,
) -> Dict:
    """
    Unified swap (build tx) entry point.
    - Same chain: build tx + approve info
    - Cross chain: build via specified router (omnibridge / nearintents)
    - Optional `mca_relayer`: forward signed relayer payloads (no ordinary swap tx)
    """
    if mca_relayer is not None and isinstance(mca_relayer, dict) and mca_relayer:
        return _unified_mca_relayer_submit(mca_relayer)

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
        if isinstance(mca_oc, dict) and mca_oc and near_same_chain_mca_applies(
            from_chain, to_chain, token_in_info, token_out_info, mca_oc
        ):
            fl = _mca_flow(mca_oc)
            if fl == "deposit":
                return _near_same_chain_mca_deposit_swap(
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
            if fl == "withdraw":
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


def _stage_a_build_evm(
    chain_int: int,
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    deposit_address: str,
) -> Dict:
    """Build the EVM Stage-A swap tx (OKX exactIn delivered to depositAddress).

    Returns ``{"success": bool, "tx", "estimatedOut", "minAmountOut",
    "router": "okx", "error"?}``.

    NOTE: We deliberately use OKX exactIn (not exactOut). OKX exactOut is
    only available on Ethereum / Base / BSC / Arbitrum via Uni V3 pools and
    long-tail tokens that actually need the two-stage route almost never have
    Uni V3 liquidity. With exactIn + OKX contract-level slippage guard the
    amount delivered is guaranteed to be >= quote-time `amountOutTarget`;
    1Click bridges anything >= the EXACT_INPUT amount we promised it.
    """
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
        return {"success": False, "error": res.get("error", "Pre-swap tx build failed"), "router": "okx"}
    return {
        "success": True,
        "tx": res.get("tx") or {},
        "estimatedOut": str(res.get("estimatedOut") or ""),
        "minAmountOut": str(res.get("minAmountOut") or ""),
        "router": "okx",
        # OKX router contract address — needed so the caller can build approve.
        "spender": (res.get("tx") or {}).get("to", ""),
    }


def _stage_a_build_solana(
    token_in: Dict,
    intermediate: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    deposit_address: str,
) -> Dict:
    """Build the Solana Stage-A swap tx using Jupiter `/swap/v2/build` and
    inject `createAssociatedTokenAccountIdempotent` for the bridge ATA.

    The 1Click depositAddress is brand new for every order (verified
    2026-05-09 — no pre-created ATAs), so we always prepend createATA
    targeting `(deposit_address, intermediate_mint)` for SPL intermediates.
    For native SOL we use Jupiter's `nativeDestinationAccount` parameter
    instead and skip the ATA hop entirely.

    Returns the same shape as `_stage_a_build_evm` but with the Solana
    base64 VersionedTransaction in `tx`.
    """
    from jupiter_utils import jupiter_build
    from solana_tx_assembler import (
        assemble_jupiter_preswap_tx,
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
        return {"success": False, "error": f"Jupiter build failed: {build_res.get('error')}", "router": "jupiter"}

    data = build_res.get("data") or {}
    out_amount_str = str(data.get("outAmount") or "")
    other_threshold = str(data.get("otherAmountThreshold") or "")
    if not out_amount_str:
        return {"success": False, "error": "Jupiter build missing outAmount", "router": "jupiter"}

    try:
        assembled = assemble_jupiter_preswap_tx(
            sender=sender,
            deposit_address=deposit_address,
            intermediate_mint=intermediate_mint,
            build_resp=data,
        )
    except Exception as e:
        return {"success": False, "error": f"Solana stage-A assembly failed: {e}", "router": "jupiter"}

    return {
        "success": True,
        "tx": assembled,
        "estimatedOut": out_amount_str,
        # Jupiter exposes its slippage-protected min-out as `otherAmountThreshold`
        # in ExactIn mode; this is what the on-chain swap will refuse to go below.
        "minAmountOut": other_threshold or out_amount_str,
        "router": "jupiter",
        # Solana has no ERC-20 style approve; signal that to the caller.
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

    Supports both EVM and Solana fromChain. The Stage-A aggregator is OKX for
    EVM and Jupiter for Solana (Jupiter `/build` instructions are merged with
    a `createAssociatedTokenAccountIdempotent` for the bridge ATA so the deposit
    works against a brand-new 1Click address).

    Response shape matches the direct cross-chain swap (`tx` + `approve` + `deposit`)
    with an additional `preSwap` block carrying Stage-A metadata and route info.
    """
    if not pre_swap or not isinstance(pre_swap, dict):
        return {"code": -1, "msg": "preSwap is required for two-stage route (from /api/swap/quote response)"}
    if not bridge or not isinstance(bridge, dict):
        return {"code": -1, "msg": "bridge is required for two-stage route (from /api/swap/quote response)"}

    is_solana_src = _is_solana_chain(from_chain)
    chain_int = None if is_solana_src else _chain_id_int(from_chain)
    if not is_solana_src and chain_int is None:
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
            oneclick_extensions=oneclick_extensions,
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

        # 2) Stage A: chain-specific same-chain swap delivering to depositAddress.
        if is_solana_src:
            stage_a = _stage_a_build_solana(
                token_in=token_in,
                intermediate=intermediate,
                amount_in=str(amount_in),
                slippage=slippage,
                sender=sender,
                deposit_address=deposit_address,
            )
        else:
            stage_a = _stage_a_build_evm(
                chain_int=chain_int,
                token_in=token_in,
                intermediate=intermediate,
                amount_in=str(amount_in),
                slippage=slippage,
                sender=sender,
                deposit_address=deposit_address,
            )
        if not stage_a.get("success"):
            return {"code": -1, "msg": f"Pre-swap tx build failed: {stage_a.get('error')}"}

        stage_a_tx = stage_a.get("tx") or {}
        stage_a_estimated_out = stage_a.get("estimatedOut") or ""
        stage_a_min_out = stage_a.get("minAmountOut") or ""
        stage_a_router = stage_a.get("router") or ("jupiter" if is_solana_src else "okx")

        # Safety: aggregator's swap-time min-out must be >= the bridge's expected mid amount
        # (otherwise the bridge could be under-delivered and refund). If the aggregator now
        # quotes lower than quote time, fail fast so the frontend re-quotes.
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

        source_chain_type = CHAIN_TYPE_SOLANA if is_solana_src else CHAIN_TYPE_EVM
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

        # 3) Approve info: EVM only. Solana base64 VersionedTransaction includes its
        #    setup/wrap instructions inline (and the user signs in one go), and there is
        #    no ERC-20 style approval on Solana.
        if not is_solana_src and not is_native_token(token_in.get("address", "")):
            approve_amount = str(amount_in)
            approve_spender = stage_a.get("spender") or stage_a_tx.get("to", "")
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
            response_data["tx"] = build_near_deposit_tx(
                token_address=token_in.get("address", ""),
                deposit_address=deposit_address,
                amount_smallest=amount_in,
                sender=sender,
                deposit_memo=deposit_memo,
            )
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
