#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
EVM DEX Aggregation - Quote and Transaction Building

Supports Bitget and OKX DEX aggregators.
Architecture: Backend builds quotes + transaction data, frontend signs and sends via wallet.

API Endpoints:
  - /api/swap/quote        : Aggregated quote from Bitget/OKX (best price selection)
  - /api/swap/build-tx     : Build swap transaction calldata for wallet signing
  - /api/swap/approve-tx   : Build ERC20 approve transaction data
  - /api/swap/supported-routers : Get supported router info per chain
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional

from loguru import logger
from bitget_utils import proxy_bitget_request, proxy_okx_request

from near_smart_router_swap import (
    ROUTER_NEAR_REF_SMARTROUTER as NEAR_REF_SMART_ROUTER_NAME,
    near_same_chain_build_tx as _near_ref_build_tx,
    near_same_chain_quote as _near_ref_quote,
)


# ============================================================
# Chain Type Detection
# ============================================================

CHAIN_TYPE_EVM = "evm"
CHAIN_TYPE_SOLANA = "solana"
CHAIN_TYPE_APTOS = "aptos"
CHAIN_TYPE_NEAR = "near"
CHAIN_TYPE_SUI = "sui"
CHAIN_TYPE_TRON = "tron"
# Generic UTXO: covers BTC / ZEC / LTC / DOGE / BCH / DASH. Their cross-chain
# deposit shape is identical (single P2PKH/P2WPKH transfer to depositAddress
# with no contract call), so they share the same builder and same descriptor.
# If we ever need per-chain branching (e.g. ZEC shielded sends, RBF for BTC)
# we can split this out later — the `chain` field on the descriptor preserves
# the original chain identifier the user passed.
CHAIN_TYPE_UTXO = "utxo"

SOLANA_CHAIN_IDS = {"solana", "solana-mainnet", "501", 501}
APTOS_CHAIN_IDS = {"aptos", "aptos-mainnet", "637", 637}
# NEAR doesn't have a canonical numeric chainId in our system; keep the string
# aliases the frontend / 1Click might pass and route them all to the same
# NEAR-native tx builder. DO NOT add a fallthrough to `CHAIN_TYPE_EVM` for
# these — that's exactly what produced bogus EVM `transfer(address,uint256)`
# calldata when `fromChain=near` was tested in production.
NEAR_CHAIN_IDS = {"near", "near-mainnet"}
SUI_CHAIN_IDS = {"sui", "sui-mainnet", "784", 784}
TRON_CHAIN_IDS = {"tron", "tron-mainnet", "195", 195}
# UTXO-style chains. 1Click currently supports BTC and ZEC as ORIGIN_CHAIN
# (verified live 2026-05-11 — both return `depositMode=SIMPLE`, no memo).
# LTC / DOGE / BCH / DASH are listed by 1Click's `/v0/tokens` but have not
# been exercised against the production quote endpoint; they ride the same
# code path on our side, so deposit-tx building will Just Work as soon as
# 1Click flips them on.
UTXO_CHAIN_IDS = {
    "btc", "bitcoin",
    "zec", "zcash",
    "ltc", "litecoin",
    "doge", "dogecoin",
    "bch",
    "dash",
}

OKX_SOLANA_CHAIN_INDEX = "501"


def detect_chain_type(chain_id, chain_type_hint=None) -> str:
    if chain_type_hint:
        return chain_type_hint
    if chain_id in SOLANA_CHAIN_IDS:
        return CHAIN_TYPE_SOLANA
    if chain_id in APTOS_CHAIN_IDS:
        return CHAIN_TYPE_APTOS
    if chain_id in NEAR_CHAIN_IDS:
        return CHAIN_TYPE_NEAR
    if chain_id in SUI_CHAIN_IDS:
        return CHAIN_TYPE_SUI
    if chain_id in TRON_CHAIN_IDS:
        return CHAIN_TYPE_TRON
    # UTXO chains are matched on the string form only (no numeric chain IDs
    # are stable for Bitcoin-family chains in our system).
    chain_id_str = str(chain_id).lower() if chain_id is not None else ""
    if chain_id_str in UTXO_CHAIN_IDS:
        return CHAIN_TYPE_UTXO
    return CHAIN_TYPE_EVM


# ============================================================
# Constants
# ============================================================

# Bitget chain ID -> chain name mapping
BITGET_CHAIN_MAP = {
    1: "eth",
    56: "bnb",
    137: "matic",
    8453: "base",
    42161: "arbitrum",
    10: "optimism",
}

# OKX native token sentinel address
OKX_NATIVE_TOKEN_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# EIP-1559 supported chain IDs
EIP1559_CHAINS = [8453, 1, 42161, 10, 137]

# Bluechip tokens per chain (for intermediate token selection in pre-swap)
BLUECHIP_TOKENS = {
    1: {  # Ethereum
        "USDT": {"address": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "symbol": "USDT", "decimals": 6},
        "USDC": {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "symbol": "USDC", "decimals": 6},
        "WETH": {"address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "symbol": "WETH", "decimals": 18},
    },
    56: {  # BSC
        "USDT": {"address": "0x55d398326f99059fF775485246999027B3197955", "symbol": "USDT", "decimals": 18},
        "USDC": {"address": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d", "symbol": "USDC", "decimals": 18},
    },
    8453: {  # Base
        "USDC": {"address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "symbol": "USDC", "decimals": 6},
        "WETH": {"address": "0x4200000000000000000000000000000000000006", "symbol": "WETH", "decimals": 18},
    },
    42161: {  # Arbitrum
        "USDT": {"address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "symbol": "USDT", "decimals": 6},
        "USDC": {"address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "symbol": "USDC", "decimals": 6},
        "WETH": {"address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "symbol": "WETH", "decimals": 18},
    },
    137: {  # Polygon
        "USDT": {"address": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", "symbol": "USDT", "decimals": 6},
        "USDC": {"address": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359", "symbol": "USDC", "decimals": 6},
    },
    10: {  # Optimism
        "USDT": {"address": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58", "symbol": "USDT", "decimals": 6},
        "USDC": {"address": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", "symbol": "USDC", "decimals": 6},
        "WETH": {"address": "0x4200000000000000000000000000000000000006", "symbol": "WETH", "decimals": 18},
    },
    143: {  # Monad
        "USDT": {"address": "", "symbol": "USDT", "decimals": 6},
        "USDC": {"address": "", "symbol": "USDC", "decimals": 6},
        "WETH": {"address": "0x4200000000000000000000000000000000000006", "symbol": "WETH", "decimals": 18},
    },
}

# Standard ERC20 approve function selector: approve(address,uint256)
ERC20_APPROVE_SELECTOR = "0x095ea7b3"
# Max uint256 for unlimited approval
MAX_UINT256 = "0x" + "f" * 64

# Public RPC fallbacks for pre-send eth_call simulation (build-time sanity check).
EVM_RPC_FALLBACK: Dict[int, list] = {
    1: ["https://1rpc.io/eth", "https://ethereum.publicnode.com"],
    56: ["https://1rpc.io/bnb", "https://bsc-dataseed.binance.org"],
    137: ["https://1rpc.io/matic", "https://polygon-rpc.com"],
    8453: ["https://1rpc.io/base", "https://mainnet.base.org"],
    42161: ["https://1rpc.io/arb", "https://arb1.arbitrum.io/rpc"],
    10: ["https://1rpc.io/op", "https://mainnet.optimism.io"],
}


# ============================================================
# Helper Functions
# ============================================================

def is_native_token(address: str) -> bool:
    """Check if address represents native token (ETH/BNB etc.)"""
    if not address or address == "":
        return True
    normalized = address.lower().replace("0x", "")
    return (
        normalized == "0"
        or normalized == "0" * 40
        or address.lower() == OKX_NATIVE_TOKEN_ADDRESS.lower()
    )


def normalize_evm_address(address: str) -> str:
    """Normalize EVM address to lowercase with 0x prefix"""
    if not address:
        return ""
    addr = address.lower()
    return addr if addr.startswith("0x") else f"0x{addr}"


def normalize_okx_token_address(chain_id, address: str) -> str:
    """OKX DEX token address: EVM hex with 0x; Solana base58 unchanged."""
    if is_native_token(address or ""):
        return OKX_NATIVE_TOKEN_ADDRESS
    cid = str(chain_id)
    if chain_id in SOLANA_CHAIN_IDS or cid == OKX_SOLANA_CHAIN_INDEX:
        return (address or "").strip()
    return normalize_evm_address(address)


def get_bitget_chain_name(chain_id: int) -> str:
    """Convert chain ID to Bitget chain name"""
    return BITGET_CHAIN_MAP.get(chain_id, "eth")


def _evm_test_okx_only() -> bool:
    """Return True when EVM_TEST_OKX_ONLY temp flag is enabled (db_info/config)."""
    try:
        from config import Cfg
        return bool(getattr(Cfg, "EVM_TEST_OKX_ONLY", False))
    except Exception:
        return False


def shrink_token(amount: str, decimals: int) -> str:
    """Convert smallest-unit amount to human-readable decimal string"""
    try:
        d = Decimal(str(amount)) / Decimal(10 ** decimals)
        # Use quantize to get proper decimal representation without trailing zeros
        result = format(d, 'f')
        # Remove trailing zeros but keep at least one decimal for readability
        if '.' in result:
            result = result.rstrip('0').rstrip('.')
        return result if result else "0"
    except (InvalidOperation, ValueError):
        return "0"


def expand_token(amount: str, decimals: int) -> str:
    """Convert human-readable amount to smallest-unit integer string"""
    try:
        d = Decimal(str(amount)) * Decimal(10 ** decimals)
        return str(int(d))
    except (InvalidOperation, ValueError):
        return "0"


def convert_slippage_to_decimal(slippage: float) -> float:
    """
    Convert slippage input to decimal form.
    - >= 1: bps (e.g., 50 -> 0.005)
    - [0.01, 1): percent (e.g., 0.5 -> 0.005)
    - (0, 0.01): already decimal (e.g., 0.005)
    """
    if slippage >= 1:
        return slippage / 10000
    elif slippage >= 0.01:
        return slippage / 100
    else:
        return slippage


def convert_preswap_slippage_to_decimal(slippage) -> float:
    """
    preSwap.slippage from /quote is a decimal fraction (not bps / not OKX percent).
    Examples: 0.001 = 0.1%, 0.01 = 1%, 0.05 = 5%.
    """
    v = float(slippage)
    if v >= 1:
        return v / 10000
    return v


def slippage_decimal_to_okx_percent(slippage_decimal: float) -> str:
    """OKX v6 swap API expects slippagePercent as a percent literal (0.5 = 0.5%, 10 = 10%)."""
    pct = float(Decimal(str(slippage_decimal)) * Decimal("100"))
    s = f"{pct:.10f}".rstrip("0").rstrip(".")
    return s if s else "0"


def get_bluechip_tokens(chain_id: int) -> Dict[str, Dict]:
    """Get bluechip tokens config for a specific chain"""
    return BLUECHIP_TOKENS.get(chain_id, {})


def find_best_bluechip_token(chain_id: int) -> Optional[Dict]:
    """Find best intermediate bluechip token (priority: USDT > USDC > WETH)"""
    tokens = get_bluechip_tokens(chain_id)
    for symbol in ["USDT", "USDC", "WETH"]:
        if symbol in tokens and tokens[symbol].get("address"):
            return tokens[symbol]
    return None


def is_bluechip_token(chain_id: int, token_address: str) -> bool:
    """Check if a token is a bluechip token on the given chain"""
    if is_native_token(token_address):
        return True
    tokens = get_bluechip_tokens(chain_id)
    normalized = normalize_evm_address(token_address)
    for config in tokens.values():
        if config.get("address") and normalize_evm_address(config["address"]) == normalized:
            return True
    return False


def _is_bitget_response_success(response: Dict) -> bool:
    """Check if Bitget API response indicates success"""
    return (
        response.get("code") == "00000"
        or response.get("error_code") == 0
        or response.get("status") == 0
    )


def _build_erc20_approve_calldata(spender: str, amount: str = None) -> str:
    """
    Build ERC20 approve(address,uint256) calldata.
    spender: the address to approve
    amount: the amount to approve (default: MaxUint256 for unlimited)
    """
    if amount is None:
        amount = MAX_UINT256

    # Pad spender to 32 bytes (remove 0x, pad left to 64 hex chars)
    spender_hex = spender.lower().replace("0x", "").zfill(64)

    # Pad amount to 32 bytes
    if amount.startswith("0x"):
        amount_hex = amount[2:].zfill(64)
    else:
        amount_hex = hex(int(amount))[2:].zfill(64)

    return f"{ERC20_APPROVE_SELECTOR}{spender_hex}{amount_hex}"


# ============================================================
# Bitget DEX API
# ============================================================

def bitget_quote(
    chain_id: int,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    user_address: str,
) -> Dict:
    """
    Call Bitget quote API.

    Args:
        amount_in: smallest unit (e.g., wei for ETH, or 1000000 for 1 USDT)
        slippage: decimal (e.g., 0.005 for 0.5%)
    Returns:
        {"success": True/False, "router": "bitget", "data": ..., "error": ...}
    """
    try:
        chain_name = get_bitget_chain_name(chain_id)
        token_in_decimals = token_in.get("decimals", 18)
        readable_amount = shrink_token(amount_in, token_in_decimals)

        normalized_in = "" if is_native_token(token_in.get("address", "")) else normalize_evm_address(token_in["address"])
        normalized_out = "" if is_native_token(token_out.get("address", "")) else normalize_evm_address(token_out["address"])
        normalized_user = normalize_evm_address(user_address)

        body = {
            "fromSymbol": token_in.get("symbol", ""),
            "fromAmount": readable_amount,
            "fromChain": chain_name,
            "fromAddress": normalized_user,
            "toSymbol": token_out.get("symbol", ""),
            "toChain": chain_name,
            "toAddress": normalized_user,
            "estimateGas": True,
            "skipCache": True,
        }
        if normalized_in:
            body["fromContract"] = normalized_in
        if normalized_out:
            body["toContract"] = normalized_out
        if slippage is not None:
            body["slippage"] = slippage * 100  # Bitget uses percentage (e.g., 0.5 for 0.5%)

        result = proxy_bitget_request(
            api_path="/bgw-pro/swapx/pro/quote",
            method="POST",
            body=body,
        )
        return {"success": True, "router": "bitget", "data": result}
    except Exception as e:
        logger.error(f"bitget_quote error: {e}")
        return {"success": False, "router": "bitget", "error": str(e)}


def bitget_swap(
    chain_id: int,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    from_address: str,
    to_address: str,
    market: str,
) -> Dict:
    """
    Call Bitget swap API to get calldata.

    Args:
        market: market/channel from quote response (required)
    Returns:
        {"success": True/False, "router": "bitget", "data": ..., "error": ...}
    """
    try:
        chain_name = get_bitget_chain_name(chain_id)
        token_in_decimals = token_in.get("decimals", 18)
        readable_amount = shrink_token(amount_in, token_in_decimals)

        normalized_in = "" if is_native_token(token_in.get("address", "")) else normalize_evm_address(token_in["address"])
        normalized_out = "" if is_native_token(token_out.get("address", "")) else normalize_evm_address(token_out["address"])
        normalized_from = normalize_evm_address(from_address)
        normalized_to = normalize_evm_address(to_address)

        body = {
            "fromSymbol": token_in.get("symbol", ""),
            "fromAmount": readable_amount,
            "fromChain": chain_name,
            "fromAddress": normalized_from,
            "toSymbol": token_out.get("symbol", ""),
            "toChain": chain_name,
            "toAddress": normalized_to,
            "slippage": slippage * 100,
            "market": market,
            "feeRate": 0,
        }
        if normalized_in:
            body["fromContract"] = normalized_in
        if normalized_out:
            body["toContract"] = normalized_out

        result = proxy_bitget_request(
            api_path="/bgw-pro/swapx/pro/swap",
            method="POST",
            body=body,
        )
        return {"success": True, "router": "bitget", "data": result}
    except Exception as e:
        logger.error(f"bitget_swap error: {e}")
        return {"success": False, "router": "bitget", "error": str(e)}


# ============================================================
# OKX DEX API
# ============================================================

def okx_quote(
    chain_id: int,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    user_address: str,
    swap_mode: str = "exactIn",
) -> Dict:
    """
    Call OKX quote API.

    Args:
        amount_in: smallest unit. Interpreted as:
            - exactIn  (default): amount of token_in to sell.
            - exactOut: amount of token_out to receive exactly.
        slippage: decimal (e.g., 0.005 for 0.5%)
        swap_mode: "exactIn" | "exactOut"
    """
    try:
        normalized_in = normalize_okx_token_address(chain_id, token_in.get("address", ""))
        normalized_out = normalize_okx_token_address(chain_id, token_out.get("address", ""))

        query = {
            "chainIndex": str(chain_id),
            "fromTokenAddress": normalized_in,
            "toTokenAddress": normalized_out,
            "amount": str(amount_in),
            "swapMode": swap_mode,
            "slippage": str(slippage),
        }
        if user_address:
            query["userWalletAddress"] = user_address

        result = proxy_okx_request(
            api_path="/api/v6/dex/aggregator/quote",
            method="GET",
            query=query,
        )
        return {"success": True, "router": "okx", "data": result}
    except Exception as e:
        logger.error(f"okx_quote error: {e}")
        return {"success": False, "router": "okx", "error": str(e)}


def okx_swap(
    chain_id: int,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    from_address: str,
    to_address: str,
    swap_mode: str = "exactIn",
) -> Dict:
    """Call OKX swap API to get calldata.

    swap_mode:
        - exactIn  (default): amount = tokenIn to sell, tokenOut receive estimated (subject to slippage).
        - exactOut: amount = tokenOut to buy exactly, tokenIn is the max the user agrees to pay.
    """
    try:
        normalized_in = normalize_okx_token_address(chain_id, token_in.get("address", ""))
        normalized_out = normalize_okx_token_address(chain_id, token_out.get("address", ""))

        query = {
            "chainIndex": str(chain_id),
            "fromTokenAddress": normalized_in,
            "toTokenAddress": normalized_out,
            "amount": str(amount_in),
            "swapMode": swap_mode,
            "slippagePercent": slippage_decimal_to_okx_percent(slippage),
            "userWalletAddress": from_address,
        }
        if to_address and to_address.lower() != from_address.lower():
            query["swapReceiverAddress"] = to_address

        result = proxy_okx_request(
            api_path="/api/v6/dex/aggregator/swap",
            method="GET",
            query=query,
        )
        return {"success": True, "router": "okx", "data": result}
    except Exception as e:
        logger.error(f"okx_swap error: {e}")
        return {"success": False, "router": "okx", "error": str(e)}


def okx_approve_transaction(
    chain_id: int,
    token_address: str,
    approve_amount: str,
) -> Dict:
    """Get OKX approve transaction data (includes dexContractAddress)."""
    try:
        normalized_token = normalize_evm_address(token_address)
        query = {
            "chainIndex": str(chain_id),
            "tokenContractAddress": normalized_token,
            "approveAmount": str(approve_amount),
        }

        result = proxy_okx_request(
            api_path="/api/v6/dex/aggregator/approve-transaction",
            method="GET",
            query=query,
        )
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"okx_approve_transaction error: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Parse Quote Responses
# ============================================================

def _parse_bitget_quote(data: Dict, token_out: Dict, slippage_decimal: float) -> Optional[Dict]:
    """Parse Bitget quote response into unified format."""
    if not _is_bitget_response_success(data) or not data.get("data"):
        return None

    bd = data["data"]
    raw_out = bd.get("outAmount") or bd.get("toAmount") or "0"
    token_out_decimals = token_out.get("decimals", 18)

    try:
        amount_out = Decimal(raw_out) * Decimal(10 ** token_out_decimals)
    except (InvalidOperation, ValueError):
        return None

    if amount_out <= 0:
        return None

    # Bitget quote may not provide an explicit minOut. If missing, compute it using slippage,
    # to keep semantics consistent with OKX (and to avoid minAmountOut == amountOut, which
    # makes deviation checks overly strict).
    raw_min_out = bd.get("minOutAmount") or bd.get("toMinAmount")
    if raw_min_out is not None and str(raw_min_out).strip() != "":
        try:
            min_amount_out = Decimal(str(raw_min_out)) * Decimal(10 ** token_out_decimals)
        except (InvalidOperation, ValueError):
            min_amount_out = amount_out * (Decimal("1") - Decimal(str(slippage_decimal)))
    else:
        min_amount_out = amount_out * (Decimal("1") - Decimal(str(slippage_decimal)))

    return {
        "router": "bitget",
        "amountOut": str(int(amount_out)),
        "amountOutReadable": raw_out,
        "minAmountOut": str(int(min_amount_out)),
        "market": bd.get("market", ""),
        "gasEstimate": bd.get("gas", ""),
        "estimateRevert": bd.get("estimateRevert", False),
        "_amountOutDecimal": amount_out,
    }


def _parse_okx_quote(data: Dict, token_out: Dict, slippage_decimal: float) -> Optional[Dict]:
    """Parse OKX quote response into unified format."""
    code = data.get("code")
    if str(code) != "0" or not data.get("data") or len(data["data"]) == 0:
        return None

    od = data["data"][0]
    raw_out = od.get("toTokenAmount") or od.get("toAmount") or "0"

    try:
        amount_out = Decimal(raw_out)
    except (InvalidOperation, ValueError):
        return None

    if amount_out <= 0:
        return None

    # Calculate minAmountOut
    min_amount_out = int(amount_out * (Decimal("1") - Decimal(str(slippage_decimal))))
    token_out_decimals = token_out.get("decimals", 18)

    parsed: Dict = {
        "router": "okx",
        "amountOut": str(int(amount_out)),
        "amountOutReadable": shrink_token(str(int(amount_out)), token_out_decimals),
        "minAmountOut": str(min_amount_out),
        "gasEstimate": od.get("estimateGasFee") or od.get("estimatedGas") or "",
        "_amountOutDecimal": amount_out,
    }
    alts = _solana_alt_pubkeys_from_provider("okx", data)
    if alts:
        parsed["addressLookupTableAddresses"] = alts
    return parsed


# ============================================================
# Aggregated Quote
# ============================================================

def aggregate_quote(
    chain_id: int,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str = None,
) -> Dict:
    """
    Call Bitget and OKX quote APIs in parallel, select best quote by amountOut.

    Args:
        chain_id: EVM chain ID
        token_in: {"address": "0x...", "symbol": "USDT", "decimals": 6}
        token_out: {"address": "0x...", "symbol": "USDC", "decimals": 6}
        amount_in: smallest unit string (e.g., "1000000" for 1 USDT)
        slippage: flexible input (bps, percent, or decimal)
        sender: user wallet address
        recipient: optional recipient address (defaults to sender)
    Returns:
        Unified quote result with best route info.
    """
    if not recipient:
        recipient = sender

    slippage_decimal = convert_slippage_to_decimal(slippage)

    # Determine which routers to query based on chain support
    routers_to_query = []
    # ===== TEMP TEST START: EVM OKX-only — skip Bitget parallel quote =====
    if not _evm_test_okx_only():
        if chain_id in BITGET_CHAIN_MAP:
            routers_to_query.append(("bitget", bitget_quote))
    # Original (restore when reverting EVM_TEST_OKX_ONLY):
    # if chain_id in BITGET_CHAIN_MAP:
    #     routers_to_query.append(("bitget", bitget_quote))
    # ===== TEMP TEST END =====
    # OKX supports many chains, always try
    routers_to_query.append(("okx", okx_quote))

    if not routers_to_query:
        return {
            "success": False,
            "error": f"No DEX router supports chain ID {chain_id}",
        }

    quotes = []
    errors = []

    # Parallel query using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=len(routers_to_query)) as executor:
        futures = {}
        for router_name, quote_fn in routers_to_query:
            future = executor.submit(
                quote_fn,
                chain_id=chain_id,
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                slippage=slippage_decimal,
                user_address=sender,
            )
            futures[future] = router_name

        for future in as_completed(futures):
            router_name = futures[future]
            try:
                result = future.result()
                if result.get("success"):
                    quotes.append(result)
                else:
                    errors.append({"router": router_name, "error": result.get("error", "Unknown error")})
            except Exception as e:
                errors.append({"router": router_name, "error": str(e)})

    if not quotes:
        return {
            "success": False,
            "error": "All DEX routers failed to return quotes",
            "details": errors,
        }

    # Parse and compare quotes, select best by amountOut
    parsed_quotes = []

    for q in quotes:
        router = q["router"]
        data = q["data"]
        parsed = None

        if router == "bitget":
            parsed = _parse_bitget_quote(data, token_out, slippage_decimal)
        elif router == "okx":
            parsed = _parse_okx_quote(data, token_out, slippage_decimal)

        if parsed:
            parsed_quotes.append(parsed)
        else:
            # Quote returned but could not be parsed (API error or empty data)
            error_msg = data.get("msg", "") if isinstance(data, dict) else str(data)
            errors.append({"router": router, "error": f"Invalid quote response: {error_msg}"})

    if not parsed_quotes:
        return {
            "success": False,
            "error": "No valid quote received from any DEX router",
            "details": errors,
        }

    # Select best quote by highest amountOut
    best = max(parsed_quotes, key=lambda q: q["_amountOutDecimal"])
    # Remove internal field
    best.pop("_amountOutDecimal", None)

    # Build all_quotes for comparison
    all_quotes_summary = []
    for pq in parsed_quotes:
        summary = {k: v for k, v in pq.items() if not k.startswith("_")}
        all_quotes_summary.append(summary)

    return {
        "success": True,
        "quote": {
            **best,
            "chainId": chain_id,
            "tokenIn": token_in,
            "tokenOut": token_out,
            "amountIn": amount_in,
            "slippage": slippage_decimal,
            "sender": sender,
            "recipient": recipient,
            "isBluechipIn": is_bluechip_token(chain_id, token_in.get("address", "")),
            "isBluechipOut": is_bluechip_token(chain_id, token_out.get("address", "")),
        },
        "allQuotes": all_quotes_summary,
        "errors": errors if errors else None,
    }


# ============================================================
# Build Swap Transaction
# ============================================================

def build_swap_tx(
    chain_id: int,
    router: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str,
    market: str = None,
) -> Dict:
    """
    Build swap transaction parameters.
    Calls the appropriate DEX swap API and returns transaction params
    ready for frontend wallet signing.

    Args:
        router: "bitget" or "okx"
        market: required for Bitget (from quote response)
    Returns:
        {"success": True, "tx": {"to", "data", "value", "gasLimit", "chainId"}, "router": ...}
    """
    slippage_decimal = convert_slippage_to_decimal(slippage)

    if router == "bitget":
        # ===== TEMP TEST START: EVM OKX-only — reject explicit Bitget build =====
        if _evm_test_okx_only():
            return {"success": False, "error": "Bitget disabled (EVM_TEST_OKX_ONLY test flag)"}
        # ===== TEMP TEST END =====
        return _build_bitget_swap_tx(chain_id, token_in, token_out, amount_in, slippage_decimal, sender, recipient, market)
    elif router == "okx":
        return _build_okx_swap_tx(chain_id, token_in, token_out, amount_in, slippage_decimal, sender, recipient)
    else:
        return {"success": False, "error": f"Unknown router: {router}"}


def _build_bitget_swap_tx(chain_id, token_in, token_out, amount_in, slippage, sender, recipient, market):
    """Build Bitget swap transaction."""
    if not market:
        return {"success": False, "error": "market is required for Bitget swap (from quote response)"}

    result = bitget_swap(
        chain_id=chain_id,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        slippage=slippage,
        from_address=sender,
        to_address=recipient,
        market=market,
    )

    if not result.get("success"):
        return {"success": False, "error": result.get("error", "Bitget swap API call failed")}

    data = result["data"]
    if not _is_bitget_response_success(data) or not data.get("data"):
        return {
            "success": False,
            "error": data.get("msg", "Bitget swap API returned error"),
            "rawResponse": data,
        }

    swap_data = data["data"]

    # Check for estimateRevert
    if swap_data.get("estimateRevert") is True:
        return {
            "success": False,
            "error": "Transaction would revert (slippage or price impact too high)",
        }

    calldata = swap_data.get("calldata") or swap_data.get("data") or ""
    to = swap_data.get("contract") or swap_data.get("to") or ""
    value = swap_data.get("value") or "0"
    gas = swap_data.get("gas") or ""
    if not gas and swap_data.get("computeUnits") is not None:
        gas = str(swap_data["computeUnits"])

    # Ensure calldata starts with 0x
    if calldata and not calldata.startswith("0x"):
        calldata = "0x" + calldata

    # For native token input, value = amountIn
    if is_native_token(token_in.get("address", "")):
        value = amount_in

    # Validation
    if not to or not calldata:
        return {
            "success": False,
            "error": "Bitget swap: missing transaction data or contract address",
            "rawResponse": data,
        }

    # Try to extract estimated output/min output if present (varies by Bitget backend).
    # These values are used for server-side deviation checks against /quote results.
    estimated_out = (
        swap_data.get("toTokenAmount")
        or swap_data.get("toAmount")
        or swap_data.get("outAmount")
        or swap_data.get("amountOut")
        or ""
    )
    min_amount_out = (
        swap_data.get("minReceiveAmount")
        or swap_data.get("minOutAmount")
        or swap_data.get("toMinAmount")
        or swap_data.get("minAmountOut")
        or ""
    )

    # Bitget may return human-readable decimal strings (e.g. "0.00043" WETH) in toAmount.
    # Normalize to smallest-unit integers using tokenOut decimals so our deviation checks work.
    def _normalize_decimal_to_int_str(v, decimals: int) -> str:
        try:
            if v is None:
                return ""
            s = str(v).strip()
            if not s:
                return ""
            if "." not in s:
                # already smallest-unit integer string (or int)
                int(s)  # validate
                return s
            from decimal import Decimal, InvalidOperation
            d = Decimal(s)
            if d <= 0:
                return "0"
            scaled = d * (Decimal(10) ** Decimal(decimals))
            return str(int(scaled))
        except Exception:
            return ""

    token_out_decimals = int(token_out.get("decimals", 18) or 18)
    estimated_out = _normalize_decimal_to_int_str(estimated_out, token_out_decimals)
    min_amount_out = _normalize_decimal_to_int_str(min_amount_out, token_out_decimals)

    return {
        "success": True,
        "tx": {
            "to": to,
            "data": calldata,
            "value": value,
            "gasLimit": gas,
            "chainId": chain_id,
        },
        "router": "bitget",
        # The 'to' address is the spender for ERC20 approval
        "approveSpender": to,
        "estimatedOut": str(estimated_out) if estimated_out is not None else "",
        "minAmountOut": str(min_amount_out) if min_amount_out is not None else "",
    }


def _build_okx_swap_tx(chain_id, token_in, token_out, amount_in, slippage, sender, recipient):
    """Build OKX swap transaction."""
    result = okx_swap(
        chain_id=chain_id,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        slippage=slippage,
        from_address=sender,
        to_address=recipient,
    )

    if not result.get("success"):
        return {"success": False, "error": result.get("error", "OKX swap API call failed")}

    data = result["data"]
    code = data.get("code")

    if str(code) != "0" or not data.get("data"):
        return {
            "success": False,
            "error": data.get("msg", "OKX swap API returned error"),
            "rawResponse": data,
        }

    # Parse OKX response (supports multiple response formats)
    response_data = data["data"]
    tx_data = None
    route_data = None

    if isinstance(response_data, list) and len(response_data) > 0:
        route_data = response_data[0]
        tx_data = route_data.get("tx") or route_data
    elif isinstance(response_data, dict):
        route_data = response_data
        tx_data = route_data.get("tx") or route_data.get("transaction") or route_data

    if not tx_data:
        return {
            "success": False,
            "error": "OKX swap: no transaction data found in response",
            "rawResponse": data,
        }

    # Check estimateRevert
    if tx_data.get("estimateRevert") is True:
        return {
            "success": False,
            "error": "Transaction would revert (slippage or price impact too high)",
        }

    calldata = tx_data.get("data") or ""
    to = tx_data.get("to") or ""
    value = tx_data.get("value") or "0"
    gas = tx_data.get("gas") or tx_data.get("gasLimit") or ""

    # Ensure calldata starts with 0x
    if calldata and not calldata.startswith("0x"):
        calldata = "0x" + calldata

    # For native token input, value = amountIn
    if is_native_token(token_in.get("address", "")):
        value = amount_in

    # Validation
    if not to or not calldata:
        return {
            "success": False,
            "error": "OKX swap: missing transaction data or contract address",
            "rawResponse": data,
        }

    # Extract estimated/min receive from OKX swap response if available.
    # OKX doc: data[0].routerResult.toTokenAmount + tx.minReceiveAmount
    estimated_out = ""
    min_amount_out = ""
    if isinstance(route_data, dict):
        router_result = route_data.get("routerResult") or {}
        if isinstance(router_result, dict):
            estimated_out = router_result.get("toTokenAmount") or router_result.get("toAmount") or ""
            min_amount_out = router_result.get("minReceiveAmount") or ""
        if not estimated_out:
            estimated_out = route_data.get("toTokenAmount") or route_data.get("toAmount") or ""
        if not min_amount_out:
            min_amount_out = tx_data.get("minReceiveAmount") or ""

    return {
        "success": True,
        "tx": {
            "to": to,
            "data": calldata,
            "value": value,
            "gasLimit": gas,
            "chainId": chain_id,
        },
        "router": "okx",
        "estimatedOut": str(estimated_out) if estimated_out is not None else "",
        "minAmountOut": str(min_amount_out) if min_amount_out is not None else "",
    }


# ============================================================
# OKX exactOut quote / swap helpers (used by the two-stage
# pre-swap + NearIntents cross-chain route)
# ============================================================

def okx_quote_exact_out(
    chain_id: int,
    token_in: Dict,
    token_out: Dict,
    amount_out: str,
    slippage: float,
    user_address: str = "",
) -> Dict:
    """Quote in exactOut mode: `amount_out` is the exact amount of token_out we want to receive.

    Returns a parsed dict with `maxAmountIn` (smallest units of token_in to pay, with slippage).
    """
    slippage_decimal = convert_slippage_to_decimal(slippage)
    raw = okx_quote(
        chain_id=chain_id,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_out,
        slippage=slippage_decimal,
        user_address=user_address,
        swap_mode="exactOut",
    )
    if not raw.get("success"):
        return {"success": False, "error": raw.get("error", "OKX exactOut quote failed")}

    data = raw.get("data") or {}
    if str(data.get("code")) != "0" or not data.get("data"):
        return {
            "success": False,
            "error": data.get("msg", "OKX exactOut quote returned error"),
            "raw": data,
        }

    od = data["data"][0] if isinstance(data["data"], list) else data["data"]
    # In exactOut mode, OKX's `fromTokenAmount` = amount of token_in required at mid price.
    from_amount = od.get("fromTokenAmount") or od.get("fromAmount") or "0"
    to_amount = od.get("toTokenAmount") or od.get("toAmount") or amount_out
    try:
        from_amount_dec = Decimal(str(from_amount))
    except (InvalidOperation, ValueError):
        from_amount_dec = Decimal("0")
    if from_amount_dec <= 0:
        return {"success": False, "error": "OKX exactOut quote: fromTokenAmount invalid", "raw": data}

    # Caller still owes slippage headroom for the approve/max-in amount.
    max_amount_in = int(from_amount_dec * (Decimal("1") + Decimal(str(slippage_decimal))))

    return {
        "success": True,
        "router": "okx",
        "swapMode": "exactOut",
        "amountIn": str(int(from_amount_dec)),
        "maxAmountIn": str(max_amount_in),
        "amountOut": str(to_amount),
        "raw": od,
    }


def build_okx_exact_out_swap_tx(
    chain_id: int,
    token_in: Dict,
    token_out: Dict,
    amount_out: str,
    slippage: float,
    sender: str,
    receiver: str,
) -> Dict:
    """Build an OKX swap tx in exactOut mode that delivers exactly `amount_out` to `receiver`.

    Used by the pre-swap stage of the two-stage cross-chain route: the receiver is the
    NearIntents 1Click depositAddress, so the bridge picks up exactly the promised
    intermediate amount without requiring a second user signature.
    """
    slippage_decimal = convert_slippage_to_decimal(slippage)
    raw = okx_swap(
        chain_id=chain_id,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_out,
        slippage=slippage_decimal,
        from_address=sender,
        to_address=receiver,
        swap_mode="exactOut",
    )
    if not raw.get("success"):
        return {"success": False, "error": raw.get("error", "OKX exactOut swap failed")}

    data = raw["data"]
    if str(data.get("code")) != "0" or not data.get("data"):
        return {
            "success": False,
            "error": data.get("msg", "OKX exactOut swap API returned error"),
            "rawResponse": data,
        }

    response_data = data["data"]
    tx_data = None
    route_data = None
    if isinstance(response_data, list) and len(response_data) > 0:
        route_data = response_data[0]
        tx_data = route_data.get("tx") or route_data
    elif isinstance(response_data, dict):
        route_data = response_data
        tx_data = route_data.get("tx") or route_data.get("transaction") or route_data

    if not tx_data:
        return {
            "success": False,
            "error": "OKX exactOut swap: no transaction data found in response",
            "rawResponse": data,
        }
    if tx_data.get("estimateRevert") is True:
        return {"success": False, "error": "Pre-swap would revert (price impact or insufficient liquidity)"}

    calldata = tx_data.get("data") or ""
    to = tx_data.get("to") or ""
    value = tx_data.get("value") or "0"
    gas = tx_data.get("gas") or tx_data.get("gasLimit") or ""
    if calldata and not calldata.startswith("0x"):
        calldata = "0x" + calldata

    # For native token input, OKX returns `value`; keep as-is (native exactOut is rare for pre-swap
    # but we preserve provider behavior).
    if not to or not calldata:
        return {
            "success": False,
            "error": "OKX exactOut swap: missing transaction data or contract address",
            "rawResponse": data,
        }

    # Extract in/out amounts from the route.
    from_amount = ""
    max_in = ""
    to_amount = ""
    if isinstance(route_data, dict):
        router_result = route_data.get("routerResult") or {}
        if isinstance(router_result, dict):
            from_amount = router_result.get("fromTokenAmount") or router_result.get("fromAmount") or ""
            to_amount = router_result.get("toTokenAmount") or router_result.get("toAmount") or ""
        if not from_amount:
            from_amount = route_data.get("fromTokenAmount") or route_data.get("fromAmount") or ""
        if not to_amount:
            to_amount = route_data.get("toTokenAmount") or route_data.get("toAmount") or ""
        max_in = tx_data.get("maxSpend") or tx_data.get("maxSendAmount") or ""

    return {
        "success": True,
        "tx": {
            "to": to,
            "data": calldata,
            "value": value,
            "gasLimit": gas,
            "chainId": chain_id,
        },
        "router": "okx",
        "swapMode": "exactOut",
        "amountIn": str(from_amount) if from_amount else "",
        "maxAmountIn": str(max_in) if max_in else "",
        "amountOut": str(to_amount) if to_amount else str(amount_out),
        "receiver": receiver,
    }


# ============================================================
# EVM transaction simulation
# ============================================================

ERC20_ALLOWANCE_SELECTOR = "0xdd62ed3e"


def get_erc20_allowance(
    chain_id: int,
    token_address: str,
    owner: str,
    spender: str,
    block: str = "latest",
) -> Optional[int]:
    """Read ERC20 allowance via eth_call. Returns None if RPC unavailable."""
    import requests

    rpc_urls = EVM_RPC_FALLBACK.get(int(chain_id)) or []
    owner_hex = normalize_evm_address(owner).replace("0x", "").zfill(64)
    spender_hex = normalize_evm_address(spender).replace("0x", "").zfill(64)
    data = ERC20_ALLOWANCE_SELECTOR + owner_hex + spender_hex
    call_obj = {"to": normalize_evm_address(token_address), "data": data}
    for url in rpc_urls:
        try:
            resp = requests.post(
                url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_call",
                    "params": [call_obj, block],
                },
                timeout=15,
            )
            body = resp.json()
            result = body.get("result")
            if result:
                return int(result, 16)
        except Exception:
            continue
    return None


def simulate_evm_tx(chain_id: int, from_address: str, tx: Dict, block: str = "latest") -> Dict:
    """
    eth_call simulation for a signed-ready EVM tx dict.
    Used at build time to catch stale routes / insufficient slippage before the user signs.
    """
    import requests

    to_addr = tx.get("to") or ""
    data = tx.get("data") or ""
    if not to_addr or not data:
        return {"success": False, "error": "simulate_evm_tx: missing to or data"}

    rpc_urls = EVM_RPC_FALLBACK.get(int(chain_id)) or []
    if not rpc_urls:
        return {"success": True, "skipped": True, "msg": "no public RPC configured for simulation"}

    value = tx.get("value") or "0"
    if isinstance(value, int):
        value = hex(value)
    elif isinstance(value, str) and value and not value.startswith("0x"):
        try:
            value = hex(int(value))
        except ValueError:
            value = "0x0"

    gas = tx.get("gasLimit") or tx.get("gas") or "800000"
    if isinstance(gas, int):
        gas = hex(gas)
    elif isinstance(gas, str) and gas and not gas.startswith("0x"):
        try:
            gas = hex(int(gas))
        except ValueError:
            gas = "0xc3500"

    call_obj = {
        "from": normalize_evm_address(from_address),
        "to": normalize_evm_address(to_addr),
        "data": data,
        "value": value,
        "gas": gas,
    }

    last_err = ""
    for url in rpc_urls:
        try:
            resp = requests.post(
                url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_call",
                    "params": [call_obj, block],
                },
                timeout=20,
            )
            body = resp.json()
            if body.get("result"):
                return {"success": True, "rpc": url}
            err = body.get("error") or {}
            last_err = err.get("message") or str(err) or "execution reverted"
        except Exception as e:
            last_err = str(e)
            continue

    return {"success": False, "error": last_err or "simulation failed"}


def simulate_preswap_evm_swap(
    chain_id: int,
    sender: str,
    token_in_address: str,
    amount_in: str,
    spender: str,
    tx: Dict,
) -> Dict:
    """
    Simulate Stage-A swap. Skips simulation when allowance is missing (user must
    approve first — eth_call would falsely revert with TransferFromFailed).
    """
    try:
        need = int(str(amount_in))
    except ValueError:
        need = 0

    allowance = None
    if spender and token_in_address and need > 0:
        allowance = get_erc20_allowance(chain_id, token_in_address, sender, spender)

    if allowance is not None and allowance < need:
        return {
            "success": True,
            "skipped": True,
            "reason": "approve_required",
            "allowance": str(allowance),
            "amountIn": str(need),
            "spender": spender,
        }

    sim = simulate_evm_tx(chain_id, sender, tx)
    if allowance is not None:
        sim["allowance"] = str(allowance)
    return sim


# ============================================================
# Build Approve Transaction
# ============================================================

def build_approve_tx(
    chain_id: int,
    router: str,
    token_address: str,
    approve_amount: str,
    spender: str = None,
) -> Dict:
    """
    Build ERC20 approve transaction parameters.

    For OKX: calls OKX API to get approve tx data + dexContractAddress.
    For Bitget: builds standard ERC20 approve calldata (requires spender from build-tx).

    Args:
        chain_id: EVM chain ID
        router: "okx" or "bitget"
        token_address: ERC20 token contract address
        approve_amount: amount to approve (smallest unit, or "max" for unlimited)
        spender: spender address (required for Bitget, optional for OKX)
    """
    if is_native_token(token_address):
        return {"success": False, "error": "Native tokens do not need approval"}

    if router == "okx":
        return _build_okx_approve_tx(chain_id, token_address, approve_amount)
    elif router == "bitget":
        # ===== TEMP TEST START: EVM OKX-only — reject Bitget approve =====
        if _evm_test_okx_only():
            return {"success": False, "error": "Bitget disabled (EVM_TEST_OKX_ONLY test flag)"}
        # ===== TEMP TEST END =====
        return _build_bitget_approve_tx(chain_id, token_address, approve_amount, spender)
    else:
        return {"success": False, "error": f"Unknown router: {router}"}


def _build_okx_approve_tx(chain_id, token_address, approve_amount):
    """Build OKX approve transaction using OKX API."""
    result = okx_approve_transaction(chain_id, token_address, approve_amount)

    if not result.get("success"):
        return {"success": False, "error": result.get("error", "OKX approve API call failed")}

    data = result["data"]
    code = data.get("code")

    if str(code) != "0":
        return {
            "success": False,
            "error": data.get("msg", "OKX approve transaction API returned error"),
        }

    response_data = data.get("data")
    approve_data = None

    if isinstance(response_data, list) and len(response_data) > 0:
        approve_data = response_data[0]
    elif isinstance(response_data, dict):
        approve_data = response_data

    if not approve_data:
        return {
            "success": False,
            "error": "OKX: no approve transaction data in response",
        }

    return {
        "success": True,
        "tx": {
            "to": normalize_evm_address(token_address),
            "data": approve_data.get("data", ""),
            "value": "0",
            "gasLimit": approve_data.get("gasLimit", ""),
            "gasPrice": approve_data.get("gasPrice", ""),
            "chainId": chain_id,
        },
        "dexContractAddress": approve_data.get("dexContractAddress", ""),
        "router": "okx",
    }


def _build_bitget_approve_tx(chain_id, token_address, approve_amount, spender):
    """Build Bitget approve transaction using standard ERC20 approve calldata."""
    if not spender:
        return {
            "success": False,
            "error": "spender address is required for Bitget approve (use 'approveSpender' from build-tx response)",
        }

    # Build standard ERC20 approve calldata
    if approve_amount == "max" or approve_amount == "unlimited":
        calldata = _build_erc20_approve_calldata(spender)
    else:
        calldata = _build_erc20_approve_calldata(spender, approve_amount)

    return {
        "success": True,
        "tx": {
            "to": normalize_evm_address(token_address),
            "data": calldata,
            "value": "0",
            "gasLimit": "60000",  # Standard ERC20 approve gas limit
            "chainId": chain_id,
        },
        "dexContractAddress": spender,
        "router": "bitget",
    }


# ============================================================
# Supported Routers Info
# ============================================================

def get_supported_routers(chain_id: int = None) -> Dict:
    """
    Get supported DEX router information.
    If chain_id is provided, returns routers for that specific chain.
    Otherwise returns all supported chains and routers.
    """
    if chain_id is not None:
        routers = []
        # ===== TEMP TEST START: EVM OKX-only — hide Bitget from router list =====
        if not _evm_test_okx_only():
            if chain_id in BITGET_CHAIN_MAP:
                routers.append({
                    "name": "bitget",
                    "chainName": get_bitget_chain_name(chain_id),
                    "supported": True,
                })
        # Original (restore when reverting EVM_TEST_OKX_ONLY):
        # if chain_id in BITGET_CHAIN_MAP:
        #     routers.append({
        #         "name": "bitget",
        #         "chainName": get_bitget_chain_name(chain_id),
        #         "supported": True,
        #     })
        # ===== TEMP TEST END =====
        routers.append({
            "name": "okx",
            "chainId": str(chain_id),
            "supported": True,
        })

        bluechip = get_bluechip_tokens(chain_id)
        bluechip_list = [
            {"symbol": k, "address": v["address"], "decimals": v["decimals"]}
            for k, v in bluechip.items() if v.get("address")
        ]

        return {
            "chainId": chain_id,
            "routers": routers,
            "bluechipTokens": bluechip_list,
            "isEip1559": chain_id in EIP1559_CHAINS,
        }
    else:
        # Return all supported chains
        all_chains = {}

        # Bitget chains
        # ===== TEMP TEST START: EVM OKX-only — hide Bitget from all-chains list =====
        if not _evm_test_okx_only():
            for cid, cname in BITGET_CHAIN_MAP.items():
                if cid not in all_chains:
                    all_chains[cid] = {"chainId": cid, "routers": [], "bluechipTokens": []}
                all_chains[cid]["routers"].append({"name": "bitget", "chainName": cname})
        # Original (restore when reverting EVM_TEST_OKX_ONLY):
        # for cid, cname in BITGET_CHAIN_MAP.items():
        #     if cid not in all_chains:
        #         all_chains[cid] = {"chainId": cid, "routers": [], "bluechipTokens": []}
        #     all_chains[cid]["routers"].append({"name": "bitget", "chainName": cname})
        # ===== TEMP TEST END =====

        # OKX supports many chains, add some common ones
        okx_chains = [1, 56, 137, 8453, 42161, 10, 250, 43114, 324, 59144, 5000, 534352, 146, 130, 143]
        for cid in okx_chains:
            if cid not in all_chains:
                all_chains[cid] = {"chainId": cid, "routers": [], "bluechipTokens": []}
            all_chains[cid]["routers"].append({"name": "okx", "chainId": str(cid)})

        # Add bluechip info
        for cid in all_chains:
            bluechip = get_bluechip_tokens(cid)
            all_chains[cid]["bluechipTokens"] = [
                {"symbol": k, "address": v["address"], "decimals": v["decimals"]}
                for k, v in bluechip.items() if v.get("address")
            ]
            all_chains[cid]["isEip1559"] = cid in EIP1559_CHAINS

        return {"chains": all_chains}


# ============================================================
# Solana Aggregation (Jupiter + OKX)
# ============================================================

SOLANA_BLUECHIP_TOKENS = {
    "SOL": {"address": "So11111111111111111111111111111111111111112", "symbol": "SOL", "decimals": 9},
    "USDC": {"address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "symbol": "USDC", "decimals": 6},
    "USDT": {"address": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "symbol": "USDT", "decimals": 6},
}


def _solana_alt_pubkeys_from_provider(router: str, data: Dict) -> List[str]:
    """ALT account pubkeys for quote transparency (Titan / Jupiter / OKX)."""
    from solana_tx_assembler import (
        extract_alt_pubkeys_from_versioned_tx_b64,
        jupiter_alt_pubkey_strings,
        okx_solana_tx_to_base64,
    )

    if not isinstance(data, dict):
        return []
    if router == "titan":
        return [
            str(a).strip()
            for a in (data.get("addressLookupTables") or [])
            if str(a).strip()
        ]
    if router == "jupiter":
        alts = jupiter_alt_pubkey_strings(data)
        swap_tx = str(data.get("swapTransaction") or "").strip()
        if not alts and swap_tx:
            alts = extract_alt_pubkeys_from_versioned_tx_b64(swap_tx)
        return alts
    if router == "okx":
        payload = data.get("data")
        if not payload:
            return []
        od = payload[0] if isinstance(payload, list) else payload
        tx_obj = (od.get("tx") or od) if isinstance(od, dict) else None
        tx_b64 = okx_solana_tx_to_base64(tx_obj if isinstance(tx_obj, dict) else None)
        if tx_b64:
            return extract_alt_pubkeys_from_versioned_tx_b64(tx_b64)
    return []


def _parse_titan_order(data: Dict, token_out: Dict, slippage_decimal: float) -> Optional[Dict]:
    """Parse Titan swap quote into unified format."""
    if not data:
        return None

    out_amount = data.get("outAmount")
    if not out_amount:
        return None

    try:
        amount_out = Decimal(str(out_amount))
    except (InvalidOperation, ValueError):
        return None

    if amount_out <= 0:
        return None

    min_amount_out = int(amount_out * (Decimal("1") - Decimal(str(slippage_decimal))))
    token_out_decimals = token_out.get("decimals", 9)

    parsed: Dict = {
        "router": "titan",
        "amountOut": str(int(amount_out)),
        "amountOutReadable": shrink_token(str(int(amount_out)), token_out_decimals),
        "minAmountOut": str(min_amount_out),
        "gasEstimate": "",
        "_amountOutDecimal": amount_out,
        "_titanRaw": data,
    }
    alts = _solana_alt_pubkeys_from_provider("titan", data)
    if alts:
        parsed["addressLookupTableAddresses"] = alts
    return parsed


# Jupiter /swap/v2/order may include non-fatal `error` strings (e.g. taker
# balance checks) while still returning a usable `outAmount` for quoting.
_JUPITER_NON_FATAL_QUOTE_ERRORS = frozenset({
    "insufficient funds",
    "insufficient balance",
})


def _jupiter_quote_error_blocks_parse(data: Dict) -> bool:
    """Return True when Jupiter's `error` field should reject quote parsing."""
    if not data:
        return True
    err = data.get("error")
    if err is None or err == "":
        return False
    if isinstance(err, dict):
        # Structured errors without outAmount are blocking.
        return not (data.get("outAmount") or data.get("outputAmount"))
    err_str = str(err).strip()
    if not err_str:
        return False
    if (data.get("outAmount") or data.get("outputAmount")) and err_str.lower() in _JUPITER_NON_FATAL_QUOTE_ERRORS:
        return False
    # Unknown error text: block only when there is no output amount to use.
    return not (data.get("outAmount") or data.get("outputAmount"))


def _parse_jupiter_order(data: Dict, token_out: Dict, slippage_decimal: float) -> Optional[Dict]:
    """Parse Jupiter /swap/v2/quote or /swap/v2/order response into unified format."""
    if _jupiter_quote_error_blocks_parse(data):
        return None

    out_amount = data.get("outAmount") or data.get("outputAmount")
    if not out_amount:
        return None

    try:
        amount_out = Decimal(str(out_amount))
    except (InvalidOperation, ValueError):
        return None

    if amount_out <= 0:
        return None

    min_amount_out = int(amount_out * (Decimal("1") - Decimal(str(slippage_decimal))))
    token_out_decimals = token_out.get("decimals", 9)

    parsed: Dict = {
        "router": "jupiter",
        "amountOut": str(int(amount_out)),
        "amountOutReadable": shrink_token(str(int(amount_out)), token_out_decimals),
        "minAmountOut": str(min_amount_out),
        "gasEstimate": "",
        "swapTransaction": data.get("swapTransaction", ""),
        "_amountOutDecimal": amount_out,
    }
    alts = _solana_alt_pubkeys_from_provider("jupiter", data)
    if alts:
        parsed["addressLookupTableAddresses"] = alts
    return parsed


def _parse_okx_solana_quote(data: Dict, token_out: Dict, slippage_decimal: float) -> Optional[Dict]:
    """Parse OKX quote response for Solana."""
    return _parse_okx_quote(data, token_out, slippage_decimal)


def aggregate_solana_quote(
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str = None,
) -> Dict:
    """
    Aggregate quotes from Jupiter, OKX, and Titan for Solana swaps.

    Args:
        token_in: {"address": "So11...112", "symbol": "SOL", "decimals": 9}
        token_out: {"address": "EPjF...t1v", "symbol": "USDC", "decimals": 6}
        amount_in: smallest unit (lamports)
        slippage: flexible input
        sender: wallet public key
    """
    from jupiter_utils import jupiter_quote
    from titan_utils import titan_order

    if not recipient:
        recipient = sender

    slippage_decimal = convert_slippage_to_decimal(slippage)
    slippage_bps = max(1, int(slippage_decimal * 10000))

    token_in_addr = token_in.get("address", "")
    token_out_addr = token_out.get("address", "")

    routers = [
        ("jupiter", lambda: jupiter_quote(
            input_mint=token_in_addr,
            output_mint=token_out_addr,
            amount=amount_in,
            slippage_bps=slippage_bps,
        )),
        ("titan", lambda: titan_order(
            input_mint=token_in_addr,
            output_mint=token_out_addr,
            amount=amount_in,
            slippage_bps=slippage_bps,
            taker=sender,
        )),
        ("okx", lambda: okx_quote(
            chain_id=501,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage=slippage_decimal,
            user_address=sender,
        )),
    ]

    quotes = []
    errors = []

    with ThreadPoolExecutor(max_workers=len(routers)) as executor:
        futures = {}
        for name, fn in routers:
            futures[executor.submit(fn)] = name
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                if result.get("success"):
                    quotes.append(result)
                else:
                    errors.append({"router": name, "error": result.get("error", "Unknown")})
            except Exception as e:
                errors.append({"router": name, "error": str(e)})

    if not quotes:
        return {"success": False, "error": "All Solana routers failed", "details": errors}

    parsed = []
    for q in quotes:
        router = q["router"]
        data = q["data"]
        p = None
        if router == "jupiter":
            p = _parse_jupiter_order(data, token_out, slippage_decimal)
        elif router == "titan":
            p = _parse_titan_order(data, token_out, slippage_decimal)
        elif router == "okx":
            p = _parse_okx_solana_quote(data, token_out, slippage_decimal)
        if p:
            parsed.append(p)
        else:
            errors.append({"router": router, "error": "Invalid quote response"})

    if not parsed:
        return {"success": False, "error": "No valid Solana quotes", "details": errors}

    best = max(parsed, key=lambda q: q["_amountOutDecimal"])
    best.pop("_amountOutDecimal", None)

    all_summary = [{k: v for k, v in pq.items() if not k.startswith("_")} for pq in parsed]

    return {
        "success": True,
        "chainType": CHAIN_TYPE_SOLANA,
        "quote": {
            **best,
            "chainId": "solana-mainnet",
            "tokenIn": token_in,
            "tokenOut": token_out,
            "amountIn": amount_in,
            "slippage": slippage_decimal,
            "sender": sender,
            "recipient": recipient,
        },
        "allQuotes": all_summary,
        "errors": errors if errors else None,
    }


def build_solana_swap_tx(
    router: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str = None,
) -> Dict:
    """
    Build Solana swap transaction.

    For Jupiter: returns pre-built base64 transaction for wallet signing.
    For Titan: assembles VersionedTransaction from route instructions + ALTs.
    For OKX: returns OKX Solana swap transaction.
    """
    from jupiter_utils import jupiter_order, jupiter_build
    from titan_utils import titan_order
    from solana_tx_assembler import (
        assemble_jupiter_swap_tx,
        assemble_titan_swap_tx,
        SolanaDepositTxBuildError,
    )

    if not recipient:
        recipient = sender
    slippage_decimal = convert_slippage_to_decimal(slippage)
    slippage_bps = max(1, int(slippage_decimal * 10000))

    if router == "titan":
        result = titan_order(
            input_mint=token_in.get("address", ""),
            output_mint=token_out.get("address", ""),
            amount=amount_in,
            slippage_bps=slippage_bps,
            taker=sender,
        )
        if not result.get("success"):
            return {"success": False, "error": result.get("error", "Titan order failed")}
        titan_data = result.get("data") or {}

        try:
            assembled = assemble_titan_swap_tx(sender=sender, titan_data=titan_data)
        except SolanaDepositTxBuildError as e:
            return {"success": False, "error": str(e)}

        return {
            "success": True,
            "chainType": CHAIN_TYPE_SOLANA,
            "tx": assembled,
            "router": "titan",
            "estimatedOut": str(titan_data.get("outAmount") or ""),
            "minAmountOut": str(titan_data.get("outAmount") or ""),
        }

    if router == "jupiter":
        result = jupiter_order(
            input_mint=token_in.get("address", ""),
            output_mint=token_out.get("address", ""),
            amount=amount_in,
            slippage_bps=slippage_bps,
            taker=sender,
        )
        if result.get("success"):
            data = result["data"]
            swap_tx = data.get("swapTransaction", "")
            if swap_tx:
                from solana_tx_assembler import enrich_solana_tx_envelope

                tx_envelope = enrich_solana_tx_envelope(
                    {"transaction": swap_tx, "format": "base64"},
                    alt_pubkeys=_solana_alt_pubkeys_from_provider("jupiter", data),
                )
                return {
                    "success": True,
                    "chainType": CHAIN_TYPE_SOLANA,
                    "tx": tx_envelope,
                    "router": "jupiter",
                }

        build_res = jupiter_build(
            input_mint=token_in.get("address", ""),
            output_mint=token_out.get("address", ""),
            amount=amount_in,
            slippage_bps=slippage_bps,
            taker=sender,
        )
        if not build_res.get("success"):
            err = result.get("error") if not result.get("success") else "Jupiter: no swapTransaction in response"
            return {"success": False, "error": build_res.get("error") or err or "Jupiter swap build failed"}

        try:
            assembled = assemble_jupiter_swap_tx(sender=sender, build_resp=build_res.get("data") or {})
        except SolanaDepositTxBuildError as e:
            return {"success": False, "error": str(e)}

        return {
            "success": True,
            "chainType": CHAIN_TYPE_SOLANA,
            "tx": assembled,
            "router": "jupiter",
        }

    elif router == "okx":
        result = okx_swap(
            chain_id=501,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            slippage=slippage_decimal,
            from_address=sender,
            to_address=recipient,
        )
        if not result.get("success"):
            return {"success": False, "error": result.get("error", "OKX Solana swap failed")}

        data = result["data"]
        if str(data.get("code")) != "0" or not data.get("data"):
            return {"success": False, "error": data.get("msg", "OKX Solana swap error")}

        tx_data = data["data"][0] if isinstance(data["data"], list) else data["data"]
        tx = tx_data.get("tx") or tx_data
        from solana_tx_assembler import okx_solana_tx_to_base64

        tx_b64 = okx_solana_tx_to_base64(tx if isinstance(tx, dict) else None)
        if not tx_b64:
            return {"success": False, "error": "OKX Solana swap missing or invalid tx.data"}

        from solana_tx_assembler import enrich_solana_tx_envelope

        tx_envelope = enrich_solana_tx_envelope(
            {"transaction": tx_b64, "format": "base64"},
            alt_pubkeys=_solana_alt_pubkeys_from_provider("okx", data),
        )
        return {
            "success": True,
            "chainType": CHAIN_TYPE_SOLANA,
            "tx": tx_envelope,
            "router": "okx",
        }

    return {"success": False, "error": f"Unknown Solana router: {router}"}


# ============================================================
# Aptos Aggregation (Panora)
# ============================================================

APTOS_BLUECHIP_TOKENS = {
    "APT": {"address": "0xa", "symbol": "APT", "decimals": 8},
    "USDC": {"address": "0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b", "symbol": "USDC", "decimals": 6},
    "USDT": {"address": "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b", "symbol": "USDT", "decimals": 6},
}


def _panora_aptos_quote(
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage_decimal: float,
    sender: str,
    recipient: str,
) -> Dict:
    from panora_utils import panora_swap

    slippage_pct = slippage_decimal * 100
    token_in_decimals = token_in.get("decimals", 8)
    readable_amount = shrink_token(amount_in, token_in_decimals)

    result = panora_swap(
        from_token=token_in.get("address", ""),
        to_token=token_out.get("address", ""),
        from_amount=readable_amount,
        to_wallet=recipient or sender,
        slippage=slippage_pct,
    )
    if not result.get("success"):
        return {"success": False, "router": "panora", "error": result.get("error", "Panora API failed")}

    data = result["data"]
    quotes_list = data if isinstance(data, list) else data.get("quotes", [data])
    if not quotes_list:
        return {"success": False, "router": "panora", "error": "Panora returned no quotes"}

    best_q = None
    best_out = Decimal("-1")
    for q in quotes_list:
        to_amount = q.get("toTokenAmount")
        if not to_amount:
            continue
        try:
            amount_out_readable = Decimal(str(to_amount))
        except (InvalidOperation, ValueError):
            continue
        token_out_decimals = token_out.get("decimals", 8)
        amount_out_raw = int(amount_out_readable * Decimal(10 ** token_out_decimals))
        if Decimal(str(amount_out_raw)) > best_out:
            best_out = Decimal(str(amount_out_raw))
            min_out = int(amount_out_raw * (Decimal("1") - Decimal(str(slippage_decimal))))
            best_q = {
                "router": "panora",
                "amountOut": str(amount_out_raw),
                "amountOutReadable": str(to_amount),
                "minAmountOut": str(min_out),
                "priceImpact": q.get("priceImpact", ""),
                "txData": q.get("txData", {}),
            }

    if not best_q:
        return {"success": False, "router": "panora", "error": "No valid Aptos quotes from Panora"}
    return {"success": True, **best_q}


def _hyperion_aptos_quote(
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage_decimal: float,
) -> Dict:
    from hyperion_utils import hyperion_quote

    res = hyperion_quote(
        token_in=token_in.get("address", ""),
        token_out=token_out.get("address", ""),
        amount_in=str(amount_in),
        slippage_decimal=slippage_decimal,
        safe_mode=False,
    )
    if not res.get("success"):
        return {"success": False, "router": "hyperion", "error": res.get("error", "Hyperion quote failed")}
    return {
        "success": True,
        "router": "hyperion",
        "amountOut": str(res.get("amountOut") or ""),
        "minAmountOut": str(res.get("minAmountOut") or ""),
        "poolRoute": res.get("poolRoute"),
    }


def aggregate_aptos_quote(
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str = None,
) -> Dict:
    """
    Get Aptos swap quote from Panora + Hyperion CLMM (best amountOut wins).

    Panora accepts human-readable amounts; Hyperion uses smallest units.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not recipient:
        recipient = sender

    slippage_decimal = convert_slippage_to_decimal(slippage)
    errors = []
    parsed = []

    def _run_panora():
        return _panora_aptos_quote(
            token_in, token_out, amount_in, slippage_decimal, sender, recipient,
        )

    def _run_hyperion():
        return _hyperion_aptos_quote(token_in, token_out, amount_in, slippage_decimal)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(_run_panora): "panora", pool.submit(_run_hyperion): "hyperion"}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                errors.append({name: str(e)})
                continue
            if not res.get("success"):
                errors.append({name: res.get("error", "failed")})
                continue
            try:
                out_dec = Decimal(str(res.get("amountOut") or "0"))
            except (InvalidOperation, ValueError):
                errors.append({name: "invalid amountOut"})
                continue
            if out_dec <= 0:
                errors.append({name: "empty amountOut"})
                continue
            parsed.append({**res, "_amountOutDecimal": out_dec})

    if not parsed:
        detail = errors if errors else "All Aptos routers failed"
        return {"success": False, "error": "All Aptos routers failed", "details": detail}

    best = max(parsed, key=lambda q: q["_amountOutDecimal"])
    best.pop("_amountOutDecimal", None)
    all_summary = [{k: v for k, v in pq.items() if not k.startswith("_")} for pq in parsed]

    return {
        "success": True,
        "chainType": CHAIN_TYPE_APTOS,
        "quote": {
            **best,
            "chainId": "aptos-mainnet",
            "tokenIn": token_in,
            "tokenOut": token_out,
            "amountIn": amount_in,
            "slippage": slippage_decimal,
            "sender": sender,
            "recipient": recipient or sender,
        },
        "allQuotes": all_summary,
        "errors": errors if errors else None,
    }


def build_aptos_swap_tx(
    router: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str = None,
) -> Dict:
    """
    Build Aptos swap transaction via Panora or Hyperion CLMM.

    Returns Move entry function data for Aptos wallet adapter.
    """
    if not recipient:
        recipient = sender

    slippage_decimal = convert_slippage_to_decimal(slippage)
    router_name = (router or "").strip().lower()

    if router_name == "hyperion":
        from hyperion_utils import hyperion_build_swap_tx

        res = hyperion_build_swap_tx(
            token_in=token_in.get("address", ""),
            token_out=token_out.get("address", ""),
            amount_in=str(amount_in),
            slippage_decimal=slippage_decimal,
            recipient=recipient,
            safe_mode=False,
        )
        if not res.get("success"):
            return {"success": False, "error": res.get("error", "Hyperion swap build failed")}
        return {
            "success": True,
            "chainType": CHAIN_TYPE_APTOS,
            "tx": res.get("tx") or {},
            "router": "hyperion",
            "estimatedOut": str(res.get("estimatedOut") or ""),
            "minAmountOut": str(res.get("minAmountOut") or ""),
        }

    if router_name and router_name not in ("panora", ""):
        return {"success": False, "error": f"Unknown Aptos router: {router}"}

    from panora_utils import panora_swap

    slippage_pct = slippage_decimal * 100
    token_in_decimals = token_in.get("decimals", 8)
    readable_amount = shrink_token(amount_in, token_in_decimals)

    result = panora_swap(
        from_token=token_in.get("address", ""),
        to_token=token_out.get("address", ""),
        from_amount=readable_amount,
        to_wallet=recipient,
        slippage=slippage_pct,
    )

    if not result.get("success"):
        return {"success": False, "error": result.get("error", "Panora swap failed")}

    data = result["data"]
    quotes_list = data if isinstance(data, list) else data.get("quotes", [data])
    if not quotes_list:
        return {"success": False, "error": "Panora returned no transaction data"}

    best_quote = quotes_list[0]
    tx_data = best_quote.get("txData", {})

    if not tx_data:
        return {"success": False, "error": "Panora: no txData in response"}

    to_amount = best_quote.get("toTokenAmount")
    token_out_decimals = token_out.get("decimals", 8)
    amount_out_raw = ""
    min_out = ""
    if to_amount is not None:
        try:
            amount_out_readable = Decimal(str(to_amount))
            amount_out_raw = str(int(amount_out_readable * Decimal(10 ** token_out_decimals)))
            min_out = str(int(Decimal(amount_out_raw) * (Decimal("1") - Decimal(str(slippage_decimal)))))
        except (InvalidOperation, ValueError):
            pass

    return {
        "success": True,
        "chainType": CHAIN_TYPE_APTOS,
        "tx": tx_data,
        "router": "panora",
        "estimatedOut": amount_out_raw,
        "minAmountOut": min_out or amount_out_raw,
    }


# ============================================================
# Unified Multi-Chain Entry Points
# ============================================================

def multi_chain_quote(
    chain_id,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str = None,
    chain_type: str = None,
) -> Dict:
    """
    Unified quote entry point for all chains.
    Detects chain type and routes to the appropriate aggregator.
    """
    ct = detect_chain_type(chain_id, chain_type)

    if ct == CHAIN_TYPE_SOLANA:
        return aggregate_solana_quote(token_in, token_out, amount_in, slippage, sender, recipient)
    elif ct == CHAIN_TYPE_APTOS:
        return aggregate_aptos_quote(token_in, token_out, amount_in, slippage, sender, recipient)
    elif ct == CHAIN_TYPE_NEAR:
        slip = convert_slippage_to_decimal(float(slippage))
        q = _near_ref_quote(
            token_in,
            token_out,
            str(amount_in),
            slip,
            sender,
            recipient,
        )
        if isinstance(q.get("quote"), dict) and q["success"]:
            q["quote"].setdefault("routerName", NEAR_REF_SMART_ROUTER_NAME)
        return q
    elif ct == CHAIN_TYPE_SUI:
        # SUI same-chain swap (Cetus / Turbos / Aftermath etc.) is not yet
        # integrated. Cross-chain via 1Click works.
        return {
            "success": False,
            "chainType": CHAIN_TYPE_SUI,
            "error": "SUI same-chain swap not supported yet; use cross-chain (fromChain != toChain) via 1Click.",
        }
    elif ct == CHAIN_TYPE_TRON:
        # TRON same-chain swap (SunSwap / JustMoney) is not yet integrated.
        return {
            "success": False,
            "chainType": CHAIN_TYPE_TRON,
            "error": "TRON same-chain swap not supported yet; use cross-chain (fromChain != toChain) via 1Click.",
        }
    elif ct == CHAIN_TYPE_UTXO:
        # UTXO chains have no DEX concept. Same-chain "swap" is effectively
        # just sending the native coin to yourself, which the frontend should
        # never request. Reject explicitly so misrouted requests fail loud.
        return {
            "success": False,
            "chainType": CHAIN_TYPE_UTXO,
            "error": (
                "UTXO chains (BTC/ZEC/LTC/DOGE/BCH/DASH) have no on-chain DEX; "
                "same-chain swap is not supported. Use cross-chain via 1Click."
            ),
        }
    else:
        result = aggregate_quote(chain_id, token_in, token_out, amount_in, slippage, sender, recipient)
        if result.get("success") and "chainType" not in result:
            result["chainType"] = CHAIN_TYPE_EVM
        return result


def multi_chain_build_tx(
    chain_id,
    router: str,
    token_in: Dict,
    token_out: Dict,
    amount_in: str,
    slippage: float,
    sender: str,
    recipient: str = None,
    market: str = None,
    chain_type: str = None,
) -> Dict:
    """
    Unified build-tx entry point for all chains.
    """
    ct = detect_chain_type(chain_id, chain_type)

    if ct == CHAIN_TYPE_SOLANA:
        return build_solana_swap_tx(router, token_in, token_out, amount_in, slippage, sender, recipient)
    elif ct == CHAIN_TYPE_APTOS:
        return build_aptos_swap_tx(
            router, token_in, token_out, amount_in, slippage, sender, recipient,
        )
    elif ct == CHAIN_TYPE_NEAR:
        slip = convert_slippage_to_decimal(float(slippage))
        return _near_ref_build_tx(
            router,
            token_in,
            token_out,
            str(amount_in),
            slip,
            sender,
            recipient,
        )
    elif ct == CHAIN_TYPE_SUI:
        return {
            "success": False,
            "chainType": CHAIN_TYPE_SUI,
            "error": "SUI same-chain swap not supported yet; use cross-chain (fromChain != toChain) via 1Click.",
        }
    elif ct == CHAIN_TYPE_TRON:
        return {
            "success": False,
            "chainType": CHAIN_TYPE_TRON,
            "error": "TRON same-chain swap not supported yet; use cross-chain (fromChain != toChain) via 1Click.",
        }
    elif ct == CHAIN_TYPE_UTXO:
        return {
            "success": False,
            "chainType": CHAIN_TYPE_UTXO,
            "error": (
                "UTXO chains (BTC/ZEC/LTC/DOGE/BCH/DASH) have no on-chain DEX; "
                "same-chain swap is not supported. Use cross-chain via 1Click."
            ),
        }
    else:
        return build_swap_tx(chain_id, router, token_in, token_out, amount_in, slippage, sender, recipient, market)


def multi_chain_approve_tx(
    chain_id,
    router: str,
    token_address: str,
    approve_amount: str,
    spender: str = None,
    chain_type: str = None,
) -> Dict:
    """
    Unified approve-tx entry point.
    Aptos and Solana do not need approvals.
    """
    ct = detect_chain_type(chain_id, chain_type)

    if ct == CHAIN_TYPE_SOLANA:
        return {"success": True, "msg": "Solana tokens do not require approval"}
    elif ct == CHAIN_TYPE_APTOS:
        return {"success": True, "msg": "Aptos tokens do not require approval"}
    elif ct == CHAIN_TYPE_NEAR:
        # NEAR's NEP-141 standard bakes the approval into `ft_transfer_call`
        # (the `msg` argument carries authorization for the receiver), so
        # there's no separate ERC20-style approval tx to build.
        return {"success": True, "msg": "NEAR NEP-141 tokens do not require a separate approval"}
    elif ct == CHAIN_TYPE_SUI:
        # SUI's coin object model passes ownership directly in the same tx
        # that constructs the transfer — no separate approval step.
        return {"success": True, "msg": "SUI tokens do not require approval"}
    elif ct == CHAIN_TYPE_TRON:
        # TRC20 has the same `approve(address,uint256)` shape as ERC20, but
        # 1Click's bridge takes funds via a direct transfer to depositAddress,
        # so the user does not need to pre-approve the bridge. The deposit
        # tx is a single transferFromOwner call signed by the user.
        return {"success": True, "msg": "TRON deposit-to-address transfers do not require approval"}
    elif ct == CHAIN_TYPE_UTXO:
        return {"success": True, "msg": "UTXO chains do not have a token-approval concept"}
    else:
        return build_approve_tx(chain_id, router, token_address, approve_amount, spender)


def multi_chain_supported_routers(chain_id=None, chain_type=None) -> Dict:
    """Get supported routers for all chain types."""
    if chain_type == CHAIN_TYPE_SOLANA or chain_id in SOLANA_CHAIN_IDS:
        return {
            "chainType": CHAIN_TYPE_SOLANA,
            "chainId": "solana-mainnet",
            "routers": [
                {"name": "jupiter", "supported": True},
                {"name": "titan", "supported": True},
                {"name": "okx", "chainId": "501", "supported": True},
            ],
            "bluechipTokens": [
                {"symbol": k, "address": v["address"], "decimals": v["decimals"]}
                for k, v in SOLANA_BLUECHIP_TOKENS.items()
            ],
            "needsApproval": False,
        }
    elif chain_type == CHAIN_TYPE_APTOS or chain_id in APTOS_CHAIN_IDS:
        return {
            "chainType": CHAIN_TYPE_APTOS,
            "chainId": "aptos-mainnet",
            "routers": [
                {"name": "hyperion", "supported": True},
                {"name": "panora", "supported": True},
            ],
            "bluechipTokens": [
                {"symbol": k, "address": v["address"], "decimals": v["decimals"]}
                for k, v in APTOS_BLUECHIP_TOKENS.items()
            ],
            "needsApproval": False,
        }
    elif chain_type == CHAIN_TYPE_NEAR or chain_id in NEAR_CHAIN_IDS:
        return {
            "chainType": CHAIN_TYPE_NEAR,
            "chainId": "near",
            "routers": [{"name": NEAR_REF_SMART_ROUTER_NAME, "supported": True}],
            "crossChainRouters": [
                {"name": "nearintents", "supported": True},
            ],
            "bluechipTokens": [],
            "needsApproval": False,
            "sameChainSupported": True,
        }
    elif chain_type == CHAIN_TYPE_SUI or chain_id in SUI_CHAIN_IDS:
        return {
            "chainType": CHAIN_TYPE_SUI,
            "chainId": "sui",
            "routers": [],
            "crossChainRouters": [
                {"name": "nearintents", "supported": True},
            ],
            "bluechipTokens": [],
            "needsApproval": False,
            "sameChainSupported": False,
        }
    elif chain_type == CHAIN_TYPE_TRON or chain_id in TRON_CHAIN_IDS:
        return {
            "chainType": CHAIN_TYPE_TRON,
            "chainId": "tron",
            "routers": [],
            "crossChainRouters": [
                {"name": "nearintents", "supported": True},
            ],
            "bluechipTokens": [],
            "needsApproval": False,
            "sameChainSupported": False,
        }
    elif chain_type == CHAIN_TYPE_UTXO or (
        chain_id is not None and str(chain_id).lower() in UTXO_CHAIN_IDS
    ):
        return {
            "chainType": CHAIN_TYPE_UTXO,
            "chainId": str(chain_id) if chain_id is not None else "",
            "routers": [],
            "crossChainRouters": [
                {"name": "nearintents", "supported": True},
            ],
            "bluechipTokens": [],
            "needsApproval": False,
            "sameChainSupported": False,
        }
    else:
        return get_supported_routers(chain_id)
