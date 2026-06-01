"""
Build source-chain deposit transactions for cross-chain swaps.

After OmniBridge / NearIntents returns a `depositAddress`, the user must
transfer tokens on the source chain to that address. This module produces
tx payloads in the SAME shape as same-chain swap tx per source chain type,
so frontend can use one code path per chain.

Shapes:
  EVM   -> {to, data, value, gasLimit, chainId}
  Aptos -> {function, type_arguments, arguments}  (Move entry function)
  Solana-> {transaction, format, txValidUntil, lastValidBlockHeight, recentBlockhash}
           `format` is always "base64". `transaction` is the base64 of a
           serialized v0 `VersionedTransaction` (no signatures yet). Frontend
           just needs `VersionedTransaction.deserialize` + `wallet.signTransaction`
           + send. The legacy `sol_transfer` / `spl_transfer` descriptor
           fallbacks (where the frontend re-built the tx itself) are still
           defined in this module but are NOT emitted by the default code
           path while we are in the testing phase — any failure (RPC
           unreachable, `solders` missing, invalid pubkey, exotic mint, ...)
           now raises `SolanaDepositTxBuildError`, which propagates up to
           `/api/swap/swap` as `{"code": -1, "msg": "..."}`. This makes
           regressions loud rather than silently degrading the UX.

Solana additional response fields:
  - txValidUntil:  unix epoch (ms) after which the embedded blockhash is
                   considered expired. The frontend should sign and
                   submit before this time, otherwise re-call /api/swap/swap.
  - lastValidBlockHeight: native Solana concept; same purpose as txValidUntil
                          but expressed as a slot height.
"""

import base64
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger

from swap_utils import is_native_token, normalize_evm_address

try:
    from config import Cfg as _Cfg
except Exception:  # pragma: no cover - config import errors should not break this module
    _Cfg = None  # type: ignore

try:  # solders is the recommended Solana types lib (Rust-backed, prebuilt wheels).
    from solders.pubkey import Pubkey  # type: ignore
    from solders.hash import Hash  # type: ignore
    from solders.instruction import Instruction, AccountMeta  # type: ignore
    from solders.message import MessageV0  # type: ignore
    from solders.transaction import VersionedTransaction  # type: ignore
    from solders.signature import Signature  # type: ignore
    _SOLDERS_AVAILABLE = True
except Exception:
    _SOLDERS_AVAILABLE = False


APTOS_NATIVE_ALIASES = {"0xa", "0x1::aptos_coin::aptoscoin", "apt", ""}
SOLANA_NATIVE_MINTS = {"so11111111111111111111111111111111111111112", ""}
# NEAR-side "native NEAR" markers. 1Click's `/v0/tokens` does NOT list native
# NEAR with `contractAddress=null` (unlike Solana / Aptos); the canonical
# NEAR-side asset on the bridge is wNEAR (`wrap.near`). We accept the empty
# string and a couple of common symbolic aliases so the frontend can pass
# whatever the user picks and the backend always builds the right tx.
NEAR_NATIVE_ALIASES = {"wrap.near", "near", "wnear", ""}
# SUI native coin type path. SUI's wallet adapters (Sui Wallet / Suiet / Slush)
# all expect this exact Move type tag when splitting from the gas coin. We
# accept the alias "sui" and empty string as the user may pass either; the
# generated tx descriptor always emits the canonical type path back.
SUI_NATIVE_TYPE = "0x2::sui::SUI"
SUI_NATIVE_ALIASES = {"0x2::sui::sui", "sui", ""}
# TRON native TRX has no contract address. Empty string and the "trx"/"tron"
# aliases all mean "native TRX" for deposit-tx building.
TRON_NATIVE_ALIASES = {"trx", "tron", ""}
# Bitcoin-family UTXO chains where 1Click currently uses `depositMode=SIMPLE`
# (no memo, single P2WPKH/P2PKH transfer). Verified against the production
# 1Click /v0/quote endpoint on 2026-05-11 for BTC and ZEC. The other chains
# in this set are listed by 1Click's `/v0/tokens` and ride the same code path
# on our side; they'll Just Work once 1Click enables them as ORIGIN_CHAIN.
UTXO_CHAIN_IDS = {
    "btc", "bitcoin",
    "zec", "zcash",
    "ltc", "litecoin",
    "doge", "dogecoin",
    "bch", "dash",
}

# Gas budgets for NEAR FunctionCall actions. `ft_transfer_call` may trigger a
# long cross-contract callback chain on 1Click's side (their bridge deposit
# contract has to forward & emit logs), so we send 100 Tgas to be safe — the
# unused portion is refunded automatically. `near_deposit` is a single state
# write so 30 Tgas is plenty.
_NEAR_GAS_FT_TRANSFER_CALL = "100000000000000"  # 100 Tgas
_NEAR_GAS_NEAR_DEPOSIT = "30000000000000"       # 30 Tgas
_NEAR_ATTACHED_YOCTO = "1"                       # NEP-141 spec requires 1 yoctoNEAR on transfer calls
_NEAR_GAS_STORAGE_DEPOSIT = "10000000000000"    # 10 Tgas — match Lending `commonTx.ts` / Meteor preview
_NEAR_TOKEN_STORAGE_READ_NEAR_HUMAN = Decimal("0.00125")
# NEAR implicit accounts only “exist” after the first transfer of NEAR to that id.
# Intents 64-hex deposit receivers often need this bootstrap before NEP-141 can
# run storage_deposit(receiver) in the follow-up tx (single receiverId = FT contract).
_NEAR_IMPLICIT_BOOTSTRAP_NEAR_HUMAN = Decimal("0.005")


def _near_transfer_bootstrap_implicit_account_action() -> Dict[str, Any]:
    yocto = str(int(_NEAR_IMPLICIT_BOOTSTRAP_NEAR_HUMAN * (Decimal(10) ** 24)))
    return {"type": "Transfer", "params": {"deposit": yocto}}


def _near_implicit_bootstrap_transaction_if_needed(
    network_id: Optional[str],
    sender: str,
    deposit_address: str,
    *,
    needs_fungible_ledger_registration: bool,
) -> Optional[Dict[str, Any]]:
    """
    Second NEAR tx needs ``storage_deposit(account_id=deposit)`` on the NEP-141 /
    wrap ledger. That call requires ``deposit`` to already exist as a NEAR
    protocol account. Intents ``depositAddress`` values are often 64-hex implicits
    that do not exist until funded — prepend **this** tx (native NEAR Transfer)
    so the wallet signs bootstrap first, then the token leg from ``build_near_deposit_tx``.
    """
    if not needs_fungible_ledger_registration:
        return None
    if not _looks_like_implicit_near_account_id(deposit_address):
        return None
    existed = _near_protocol_account_exists(network_id, deposit_address)
    if existed is True:
        return None
    return {
        "signerId": sender,
        "receiverId": deposit_address,
        "actions": [_near_transfer_bootstrap_implicit_account_action()],
    }


def _cfg_near_rpc_urls(network_id: Optional[str]) -> List[str]:
    if _Cfg is None:
        return ["https://rpc.mainnet.near.org"]
    oid = str(
        network_id or getattr(_Cfg, "NETWORK_ID", None) or "MAINNET"
    ).upper()
    raw = (getattr(_Cfg, "NETWORK", {}) or {}).get(oid, {}).get("NEAR_RPC_URL") or []
    if isinstance(raw, str) and raw.strip():
        return [raw.strip()]
    urls = [str(u) for u in raw if u]
    return urls if urls else ["https://rpc.mainnet.near.org"]


def _near_yocto_from_near_human(amount_near: str) -> str:
    return str(int(Decimal(str(amount_near)) * Decimal(10**24)))


def _near_view_call(
    network_id: Optional[str],
    *,
    contract_id: str,
    method_name: str,
    args: Dict[str, Any],
    timeout_sec: float = 8.0,
) -> Any:
    """
    Lightweight NEAR RPC view helper (no indexer-helper NEAR deps).
    Raises on exhausted RPC failures so callers can assume deposit is unknown.
    """
    ctr = str(contract_id or "").strip()
    if not ctr:
        raise ValueError("empty NEAR contract id for view")

    payload_bin = json.dumps(args or {}, separators=(",", ":")).encode("utf-8")
    body = {
        "jsonrpc": "2.0",
        "id": "cross-chain-tx-builder",
        "method": "query",
        "params": {
            "request_type": "call_function",
            "finality": "final",
            "account_id": ctr,
            "method_name": method_name,
            "args_base64": base64.b64encode(payload_bin).decode("ascii"),
        },
    }
    last_err: Optional[str] = None
    for url in _cfg_near_rpc_urls(network_id):
        try:
            r = requests.post(str(url).strip(), json=body, timeout=timeout_sec)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                raise RuntimeError(str(data["error"]))
            res = data.get("result") or {}
            blobs = res.get("result")
            if isinstance(blobs, list) and not blobs:
                return None
            if blobs is None:
                return None
            s = "".join(chr(int(b)) for b in blobs)
            return json.loads(s)
        except Exception as e:
            last_err = str(e)
            logger.warning(f"NEAR RPC view {ctr}.{method_name} via {url}: {e}")
            continue
    raise RuntimeError(f"NEAR view failed for {ctr}.{method_name}: {last_err}")


def _near_protocol_account_exists(
    network_id: Optional[str], account_id: str, *, timeout_sec: float = 8.0
) -> Optional[bool]:
    """
    True iff ``account_id`` exists on the NEAR protocol (view_account succeeds).

    NEP-141 ``storage_deposit(account_id=X)`` requires ``X`` to already be a NEAR
    account. Near Intents often returns implicit-style 64-hex deposit targets that
    are not spun up until funds arrive — prepending ``storage_deposit`` for those
    fails with ``account … doesn't exist`` (not a signature issue).
    """
    aid = str(account_id or "").strip()
    if not aid:
        return None

    body = {
        "jsonrpc": "2.0",
        "id": "near-protocol-account-check",
        "method": "query",
        "params": {"request_type": "view_account", "account_id": aid, "finality": "final"},
    }
    last_err = None
    for url in _cfg_near_rpc_urls(network_id):
        try:
            r = requests.post(str(url).strip(), json=body, timeout=timeout_sec)
            r.raise_for_status()
            data = r.json() or {}
            if isinstance(data.get("result"), dict) and data["result"]:
                return True
            err = data.get("error") or {}
            msg = ""
            err_name = ""
            cause = err.get("cause") if isinstance(err.get("cause"), dict) else None
            if isinstance(cause, dict):
                msg = str(cause.get("info", {}).get("error_message") or "")
                err_name = str(cause.get("name") or "")
                nm_norm = err_name.upper().replace("-", "").replace("_", "")
                if nm_norm in ("UNKNOWNACCOUNT", "ACCOUNTDOESNOTEXIST"):
                    return False
            combined = msg + str(err.get("message", "")) + err_name
            if isinstance(combined, str) and combined:
                lc = combined.lower()
                if "does not exist" in lc or "doesn't exist" in lc:
                    return False
            logger.warning(f"near view_account ambiguous for {aid!r}: {data}")
            return None
        except Exception as e:
            last_err = str(e)
            logger.warning(f"near view_account via {url} err: {e}")
            continue
    logger.warning(f"near view_account failed for {aid!r}: {last_err}")
    return None


def _looks_like_implicit_near_account_id(account_id: str) -> bool:
    """Implicit NEAR ids are lowercase hex encoding of a pubkey, 64 chars."""
    a = str(account_id or "").strip().lower()
    if len(a) != 64:
        return False
    return all(("0" <= c <= "9") or ("a" <= c <= "f") for c in a)


def _near_deposit_account_needs_registration(
    network_id: Optional[str],
    token_contract: str,
    deposit_account_id: str,
) -> bool:
    """True iff we should prepend ``storage_deposit`` for ``deposit_account_id`` on ``token_contract``."""
    tid = str(token_contract or "").strip()
    dep = str(deposit_account_id or "").strip()
    if not tid or not dep:
        return True
    try:
        bal = _near_view_call(
            network_id,
            contract_id=tid,
            method_name="storage_balance_of",
            args={"account_id": dep},
        )
        # NEAR FT: registered accounts return a dict; absent registration is null-ish.
        return not bool(bal)
    except Exception as e:
        logger.warning(f"storage_balance_of check skipped (assume deposit needed): {e}")
        return True


def _near_wallet_storage_deposit_action(deposit_account_id: str) -> Dict:
    """Wallet-selector shaped action; matches Lending ``tansfer_txs_query`` / ``near.transfer_near``."""
    dep_yocto = _near_yocto_from_near_human(str(_NEAR_TOKEN_STORAGE_READ_NEAR_HUMAN))
    return {
        "type": "FunctionCall",
        "params": {
            "methodName": "storage_deposit",
            "args": {
                "account_id": str(deposit_account_id),
                "registration_only": True,
            },
            "gas": _NEAR_GAS_STORAGE_DEPOSIT,
            "deposit": dep_yocto,
        },
    }


class SolanaDepositTxBuildError(RuntimeError):
    """Raised when we cannot assemble a signable Solana deposit tx.

    During the testing phase we surface these failures to the caller instead
    of silently returning a legacy descriptor — that way frontend / monitoring
    sees the regression immediately. The exception message is propagated to
    the API caller via the catch-all in `_cross_chain_swap`.
    """

# Solana program IDs.
_SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_ATA_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
_MEMO_PROGRAM_ID = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"

# Solana blockhash is valid for ~150 slots (~60-90s). We expose a conservative
# 60-second window so the frontend has a hard deadline to sign + submit.
_SOL_TX_LIFETIME_MS = 60_000

# Default RPC fallback if Cfg.SOLANA_RPC_URL is missing. Public mainnet-beta is
# rate-limited but works for occasional smoke tests.
_DEFAULT_SOLANA_RPC = "https://api.mainnet-beta.solana.com"


def _to_chain_int(chain_id) -> Optional[int]:
    try:
        return int(chain_id)
    except (TypeError, ValueError):
        return None


def build_evm_deposit_tx(
    chain_id,
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
) -> Dict:
    """
    Build an EVM transaction to deposit tokens into the cross-chain deposit address.

    - Native token: {to: depositAddress, data: "0x", value: amount, ...}
    - ERC20:        {to: tokenAddress,   data: transfer(depositAddr, amount), value: "0x0", ...}
    """
    chain_id_int = _to_chain_int(chain_id)
    chain_id_out = chain_id_int if chain_id_int is not None else chain_id

    try:
        amount_int = int(amount_smallest)
    except (TypeError, ValueError):
        amount_int = 0

    if is_native_token(token_address):
        return {
            "to": normalize_evm_address(deposit_address) if deposit_address.startswith("0x") else deposit_address,
            "data": "0x",
            "value": hex(amount_int),
            "gasLimit": "0x5208",
            "chainId": chain_id_out,
        }

    recipient_hex = normalize_evm_address(deposit_address).replace("0x", "").lower().zfill(64)
    amount_hex = hex(amount_int).replace("0x", "").zfill(64)
    selector = "a9059cbb"
    calldata = "0x" + selector + recipient_hex + amount_hex

    # Polygon USDC transfers can exceed 70k gas (cold account / high network load).
    # 100k avoids OOG reverts while staying cheap vs a full DEX swap.
    return {
        "to": normalize_evm_address(token_address),
        "data": calldata,
        "value": "0x0",
        "gasLimit": "0x186a0",
        "chainId": chain_id_out,
    }


def build_aptos_deposit_tx(
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
) -> Dict:
    """
    Build an Aptos Move entry function payload for cross-chain deposit.

    Aptos has two mutually-exclusive asset standards. Long-form transaction
    shapes below are snake_case to match the rest of our API; the wallet
    adapter SDK on the frontend converts them to camelCase when calling
    `signAndSubmitTransaction`.

    1) Native APT (empty / `0xa` / `0x1::aptos_coin::AptosCoin`):
           function: 0x1::aptos_account::transfer
           type_arguments: []
           arguments: [depositAddress, amount]

    2) Fungible Asset (FA) token (plain object address, e.g. USDT/USDC FA):
           function: 0x1::primary_fungible_store::transfer
           type_arguments: ["0x1::fungible_asset::Metadata"]
           arguments: [tokenAddress, depositAddress, amount]

    3) Legacy Coin-only tokens (type path `<addr>::<module>::<Type>`):
           function: 0x1::aptos_account::transfer_coins
           type_arguments: [tokenAddress]
           arguments: [depositAddress, amount]

    All new Aptos tokens and tokens that NEAR-Intents 1Click lists for
    cross-chain deposits are FA, so branch (2) is the common path. Branch
    (3) is kept as a fallback in case a Coin-only token ever hits us.

    Convenience top-level fields (`tokenAddress`, `depositAddress`,
    `amount`, `standard`) are included so the frontend can dispatch
    without re-parsing `arguments`.
    """
    addr_raw = (token_address or "").strip()
    addr_lower = addr_raw.lower()
    amount_str = str(amount_smallest)

    if addr_lower in APTOS_NATIVE_ALIASES:
        return {
            "function": "0x1::aptos_account::transfer",
            "type_arguments": [],
            "arguments": [deposit_address, amount_str],
            "standard": "native",
            "tokenAddress": "",
            "depositAddress": deposit_address,
            "amount": amount_str,
        }

    # Legacy Coin type path (contains `::`) — token is identified by generic type argument.
    if "::" in addr_raw:
        return {
            "function": "0x1::aptos_account::transfer_coins",
            "type_arguments": [addr_raw],
            "arguments": [deposit_address, amount_str],
            "standard": "coin",
            "tokenAddress": addr_raw,
            "depositAddress": deposit_address,
            "amount": amount_str,
        }

    # Default: Fungible Asset standard — token is the Metadata object address.
    return {
        "function": "0x1::primary_fungible_store::transfer",
        "type_arguments": ["0x1::fungible_asset::Metadata"],
        "arguments": [addr_raw, deposit_address, amount_str],
        "standard": "fa",
        "tokenAddress": addr_raw,
        "depositAddress": deposit_address,
        "amount": amount_str,
    }


def build_near_deposit_tx(
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
    sender: str,
    deposit_memo: str = "",
    *,
    network_id: Optional[str] = None,
    skip_implicit_bootstrap: bool = False,
) -> Dict:
    """
    Build a NEAR transaction payload that deposits `amount_smallest` of the
    NEP-141 token `token_address` to a 1Click `depositAddress`.

    1Click on NEAR: token leg uses ``ft_transfer`` (same payload shape as the
    previous ``ft_transfer_call`` branch, ``methodName`` only differs). The
    NEAR native token is NOT a NEP-141; the canonical bridge form is wNEAR
    (`wrap.near`). We collapse the empty string and the symbolic aliases in
    `NEAR_NATIVE_ALIASES` onto the wrap.near flow, so the frontend can pass
    whatever the user picks.

    When ``network_id`` is supplied (recommended: ``Cfg.NETWORK_ID``), we RPC
    query ``storage_balance_of`` on the relevant token ledger (``wrap.near`` or
    NEP-141). If ``depositAddress`` lacks FT registration we prepend
    ``storage_deposit`` (0.00125 NEAR, 10 Tgas, ``registration_only``),
    matching Lending ``tansfer_txs_query`` / ``near.transfer_near``.

    Intents ``depositAddress`` values are often **64-char hex implicits** that do
    not exist on the NEAR protocol until they receive NEAR. Unless
    ``skip_implicit_bootstrap=True`` (1Click swap build from ``unified_swap``),
    we attach ``depositSetupTransaction``: a native NEAR **Transfer** (0.005 NEAR)
    to bootstrap the implicit before ``storage_deposit`` + token leg.
    Two transaction shapes:

    1) NEAR-native source (`token_address` in `NEAR_NATIVE_ALIASES`):
       Actions on ``wrap.near``: optional ``storage_deposit``,
       ``near_deposit`` (`amount_smallest` yoctoNEAR -> wraps), then
       ``ft_transfer`` (fresh wNEAR -> depositAddress).

    2) NEP-141 source (any other `token_address`):
       Optional ``storage_deposit``, then ``ft_transfer``.
       User must already hold the NEP-141 token in their NEAR wallet.

    Returned shape follows the NEAR `wallet-selector` / near-api-js
    convention so the frontend can pass it straight into
    `wallet.signAndSendTransaction({receiverId, actions})`:
      {
        "chainId":        "near",
        "signerId":       "<sender>",
        "receiverId":     "<NEP-141 contract>",
        "standard":       "native" | "nep141",
        "tokenAddress":   "<NEP-141 contract>",
        "depositAddress": "<1Click depositAddress>",
        "amount":         "<smallest unit>",
        "actions":        [ { "type": "FunctionCall" | "Transfer", "params": {...} }, ... ],
        "depositSetupTransaction" (optional): tx to submit **before** the main payload
          when Intents implicit ``depositAddress`` must be bootstrapped on-chain.
      }
    """
    addr_raw = (token_address or "").strip()
    addr_lower = addr_raw.lower()
    memo = deposit_memo or ""
    amount_str = str(amount_smallest)

    # Same JSON shape as before; only ``methodName`` uses ``ft_transfer``.
    ft_deposit_final_action = {
        "type": "FunctionCall",
        "params": {
            "methodName": "ft_transfer",
            "args": {
                "receiver_id": deposit_address,
                "amount": amount_str,
                "msg": memo,
            },
            "gas": _NEAR_GAS_FT_TRANSFER_CALL,
            "deposit": _NEAR_ATTACHED_YOCTO,
        },
    }

    if addr_lower in NEAR_NATIVE_ALIASES:
        near_deposit_action = {
            "type": "FunctionCall",
            "params": {
                "methodName": "near_deposit",
                "args": {},
                "gas": _NEAR_GAS_NEAR_DEPOSIT,
                "deposit": amount_str,
            },
        }
        acts: List[Dict[str, Any]] = []
        needs_wr = _near_deposit_account_needs_registration(
            network_id, "wrap.near", deposit_address
        )
        setup_tx = None
        if not skip_implicit_bootstrap:
            setup_tx = _near_implicit_bootstrap_transaction_if_needed(
                network_id,
                sender,
                deposit_address,
                needs_fungible_ledger_registration=needs_wr,
            )
        if needs_wr:
            acts.append(_near_wallet_storage_deposit_action(deposit_address))
        acts.append(near_deposit_action)
        acts.append(ft_deposit_final_action)
        out_native: Dict[str, Any] = {
            "chainId": "near",
            "signerId": sender,
            "receiverId": "wrap.near",
            "standard": "native",
            "tokenAddress": "wrap.near",
            "depositAddress": deposit_address,
            "amount": amount_str,
            "actions": acts,
        }
        if setup_tx:
            out_native["depositSetupTransaction"] = setup_tx
        return out_native

    nep141_actions: List[Dict[str, Any]] = []
    needs_reg = _near_deposit_account_needs_registration(network_id, addr_raw, deposit_address)
    setup_tx_nep = None
    if not skip_implicit_bootstrap:
        setup_tx_nep = _near_implicit_bootstrap_transaction_if_needed(
            network_id,
            sender,
            deposit_address,
            needs_fungible_ledger_registration=needs_reg,
        )
    if needs_reg:
        nep141_actions.append(_near_wallet_storage_deposit_action(deposit_address))
    nep141_actions.append(ft_deposit_final_action)
    out_nep: Dict[str, Any] = {
        "chainId": "near",
        "signerId": sender,
        "receiverId": addr_raw,
        "standard": "nep141",
        "tokenAddress": addr_raw,
        "depositAddress": deposit_address,
        "amount": amount_str,
        "actions": nep141_actions,
    }
    if setup_tx_nep:
        out_nep["depositSetupTransaction"] = setup_tx_nep
    return out_nep


# ============================================================================
# Solana cross-chain deposit
# ============================================================================
#
# We try to assemble a *signable* VersionedTransaction on the backend so the
# frontend just deserializes -> signs -> sends, matching the same-chain Jupiter
# response shape. If anything goes wrong (no `solders`, no RPC, mint exotic,
# etc.), we fall back to the legacy descriptor format and let the frontend
# build the tx itself.


def _encode_solana_payload(payload: Dict) -> str:
    """Pack the cross-chain Solana deposit descriptor into base64(JSON).

    Used by the legacy fallback path; the frontend decodes the JSON and
    builds the SystemProgram.transfer / SPL transferChecked itself.
    """
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.b64encode(encoded).decode("ascii")


def _solana_descriptor_native(deposit_address: str, amount: str, decimals: int, memo: str) -> Dict:
    payload = {
        "depositAddress": deposit_address,
        "amount": str(amount),
        "decimals": int(decimals) if decimals else 9,
        "depositMemo": memo or "",
    }
    return {"transaction": _encode_solana_payload(payload), "format": "sol_transfer"}


def _solana_descriptor_spl(token_address: str, deposit_address: str, amount: str, decimals: int, memo: str) -> Dict:
    payload = {
        "depositAddress": deposit_address,
        "mint": token_address,
        "amount": str(amount),
        "decimals": int(decimals) if decimals else 6,
        "depositMemo": memo or "",
    }
    return {"transaction": _encode_solana_payload(payload), "format": "spl_transfer"}


def _solana_rpc_url() -> str:
    url = ""
    if _Cfg is not None:
        url = (getattr(_Cfg, "SOLANA_RPC_URL", "") or "").strip()
    return url or _DEFAULT_SOLANA_RPC


def _fetch_latest_blockhash() -> Optional[Tuple[str, int]]:
    """Return (blockhash_str, lastValidBlockHeight) or None on failure."""
    rpc_url = _solana_rpc_url()
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": [{"commitment": "confirmed"}],
    }
    for attempt in (1, 2):
        try:
            resp = requests.post(rpc_url, json=body, timeout=5)
            if resp.status_code != 200:
                logger.warning(
                    f"Solana getLatestBlockhash non-200 (attempt {attempt}): "
                    f"status={resp.status_code} body={resp.text[:200]}"
                )
                continue
            data = resp.json() or {}
            result = data.get("result") or {}
            value = result.get("value") or {}
            blockhash = value.get("blockhash")
            last_valid = value.get("lastValidBlockHeight")
            if blockhash and last_valid is not None:
                return str(blockhash), int(last_valid)
            logger.warning(f"Solana getLatestBlockhash unexpected payload: {data}")
        except Exception as e:
            logger.warning(f"Solana getLatestBlockhash error (attempt {attempt}): {e}")
    return None


def _u64_le(n: int) -> bytes:
    return int(n).to_bytes(8, "little")


def _system_transfer_ix(sender_pk: "Pubkey", dest_pk: "Pubkey", lamports: int) -> "Instruction":
    # SystemProgram::Transfer instruction: tag=2 (u32 LE), then u64 LE lamports.
    data = (2).to_bytes(4, "little") + _u64_le(lamports)
    return Instruction(
        program_id=Pubkey.from_string(_SYSTEM_PROGRAM_ID),
        accounts=[
            AccountMeta(pubkey=sender_pk, is_signer=True, is_writable=True),
            AccountMeta(pubkey=dest_pk, is_signer=False, is_writable=True),
        ],
        data=data,
    )


def _memo_ix(memo: str) -> "Instruction":
    return Instruction(
        program_id=Pubkey.from_string(_MEMO_PROGRAM_ID),
        accounts=[],
        data=memo.encode("utf-8"),
    )


def _ata(owner: "Pubkey", mint: "Pubkey") -> "Pubkey":
    """Derive the classic-SPL associated token account for (owner, mint)."""
    addr, _bump = Pubkey.find_program_address(
        [bytes(owner), bytes(Pubkey.from_string(_TOKEN_PROGRAM_ID)), bytes(mint)],
        Pubkey.from_string(_ATA_PROGRAM_ID),
    )
    return addr


def _create_ata_idempotent_ix(payer: "Pubkey", owner: "Pubkey", mint: "Pubkey") -> "Instruction":
    """ATA program CreateIdempotent (tag=1)."""
    associated = _ata(owner, mint)
    return Instruction(
        program_id=Pubkey.from_string(_ATA_PROGRAM_ID),
        accounts=[
            AccountMeta(pubkey=payer, is_signer=True, is_writable=True),
            AccountMeta(pubkey=associated, is_signer=False, is_writable=True),
            AccountMeta(pubkey=owner, is_signer=False, is_writable=False),
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),
            AccountMeta(pubkey=Pubkey.from_string(_SYSTEM_PROGRAM_ID), is_signer=False, is_writable=False),
            AccountMeta(pubkey=Pubkey.from_string(_TOKEN_PROGRAM_ID), is_signer=False, is_writable=False),
        ],
        data=bytes([1]),
    )


def _spl_transfer_checked_ix(
    source: "Pubkey",
    mint: "Pubkey",
    destination: "Pubkey",
    owner: "Pubkey",
    amount: int,
    decimals: int,
) -> "Instruction":
    """SPL Token::TransferChecked (tag=12)."""
    data = bytes([12]) + _u64_le(amount) + bytes([int(decimals) & 0xFF])
    return Instruction(
        program_id=Pubkey.from_string(_TOKEN_PROGRAM_ID),
        accounts=[
            AccountMeta(pubkey=source, is_signer=False, is_writable=True),
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),
            AccountMeta(pubkey=destination, is_signer=False, is_writable=True),
            AccountMeta(pubkey=owner, is_signer=True, is_writable=False),
        ],
        data=data,
    )


def _serialize_unsigned_versioned_tx(payer: "Pubkey", instructions: list, blockhash_str: str) -> bytes:
    """Compile a v0 message and emit an UNSIGNED VersionedTransaction.

    The signatures slot is filled with placeholder zero bytes — the frontend
    wallet replaces them when it signs. This is the standard pattern for
    server-prepared Solana txs (Jupiter `swapTransaction` works the same way).
    """
    msg = MessageV0.try_compile(
        payer=payer,
        instructions=instructions,
        address_lookup_table_accounts=[],
        recent_blockhash=Hash.from_string(blockhash_str),
    )
    num_required = msg.header.num_required_signatures
    placeholder_sigs = [Signature.default() for _ in range(num_required)]
    tx = VersionedTransaction.populate(msg, placeholder_sigs)
    return bytes(tx)


def _build_signable_solana_tx(
    sender: str,
    deposit_address: str,
    amount_smallest: str,
    *,
    is_native: bool,
    token_address: str = "",
    decimals: int = 0,
    memo: str = "",
) -> Dict:
    """Assemble a signable VersionedTransaction for the given deposit.

    Raises `SolanaDepositTxBuildError` on any failure; returns the response
    dict (with format="base64" + lifetime fields) on success.
    """
    if not _SOLDERS_AVAILABLE:
        raise SolanaDepositTxBuildError(
            "Solana tx build: `solders` package not installed on the backend"
        )
    if not sender:
        raise SolanaDepositTxBuildError(
            "Solana tx build: sender is required for cross-chain Solana deposit"
        )

    try:
        amount_int = int(str(amount_smallest))
    except (TypeError, ValueError) as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: invalid amount {amount_smallest!r}: {e}"
        ) from e
    if amount_int <= 0:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: amount must be positive (got {amount_int})"
        )

    bh = _fetch_latest_blockhash()
    if not bh:
        raise SolanaDepositTxBuildError(
            "Solana tx build: failed to fetch recent blockhash from SOLANA_RPC_URL "
            "(check the RPC endpoint is reachable and not rate-limited)"
        )
    blockhash, last_valid_block_height = bh

    try:
        sender_pk = Pubkey.from_string(sender)
        deposit_pk = Pubkey.from_string(deposit_address)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: invalid pubkey sender={sender} deposit={deposit_address}: {e}"
        ) from e

    instructions: list = []
    try:
        if is_native:
            instructions.append(_system_transfer_ix(sender_pk, deposit_pk, amount_int))
        else:
            mint_pk = Pubkey.from_string(token_address)
            sender_ata = _ata(sender_pk, mint_pk)
            deposit_ata = _ata(deposit_pk, mint_pk)
            instructions.append(_create_ata_idempotent_ix(sender_pk, deposit_pk, mint_pk))
            instructions.append(_spl_transfer_checked_ix(
                source=sender_ata,
                mint=mint_pk,
                destination=deposit_ata,
                owner=sender_pk,
                amount=amount_int,
                decimals=int(decimals) if decimals else 6,
            ))

        if memo:
            instructions.append(_memo_ix(memo))

        raw = _serialize_unsigned_versioned_tx(sender_pk, instructions, blockhash)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: instruction/serialize step failed sender={sender} "
            f"deposit={deposit_address} mint={token_address or 'native'}: {e}"
        ) from e

    valid_until_ms = int(time.time() * 1000) + _SOL_TX_LIFETIME_MS
    return {
        "transaction": base64.b64encode(raw).decode("ascii"),
        "format": "base64",
        "txValidUntil": valid_until_ms,
        "lastValidBlockHeight": last_valid_block_height,
        "recentBlockhash": blockhash,
    }


def build_solana_deposit_tx(
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
    decimals: int,
    deposit_memo: str = "",
    sender: str = "",
) -> Dict:
    """
    Build a Solana cross-chain deposit transaction for the user to sign.

    Returns:
        {
          "transaction": "<base64 of unsigned VersionedTransaction>",
          "format": "base64",
          "txValidUntil": <unix ms>,
          "lastValidBlockHeight": <slot>,
          "recentBlockhash": "<base58 hash>"
        }

    Frontend simply does:
        const bytes = base64Decode(tx.transaction)
        const vtx = VersionedTransaction.deserialize(bytes)
        await wallet.signAndSendTransaction(vtx)

    Failure modes: this function raises `SolanaDepositTxBuildError` when any
    of {sender missing, `solders` not installed, RPC blockhash fetch failed,
    invalid pubkey, exotic mint, instruction-assembly error} occurs. The
    caller in `_cross_chain_swap` propagates the message verbatim to the
    API caller as `{"code": -1, "msg": "..."}`. While we are in the testing
    phase we deliberately do NOT fall back to the legacy descriptor format
    so that any regression on the new path is loud and easy to spot.

    The legacy `_solana_descriptor_native` / `_solana_descriptor_spl` helpers
    are still defined in this module and can be re-enabled later if we want
    a graceful fallback in production.
    """
    addr_lower = (token_address or "").lower().strip()
    is_native = addr_lower in SOLANA_NATIVE_MINTS

    return _build_signable_solana_tx(
        sender=sender,
        deposit_address=deposit_address,
        amount_smallest=amount_smallest,
        is_native=is_native,
        token_address=token_address if not is_native else "",
        decimals=decimals,
        memo=deposit_memo or "",
    )


# ============================================================
# SUI cross-chain deposit
# ============================================================
#
# SUI uses an object-centric model: every coin balance is a `Coin<T>` object
# owned by an address. The wallet adapter SDKs (`@mysten/sui.js`,
# `@mysten/wallet-standard`) accept a Programmable Transaction Block (PTB)
# description and handle gas coin selection / signing internally. Rather than
# build a binary `TransactionBlock` on the backend (which would require pinning
# to a specific SDK version and re-implementing object selection ourselves),
# we emit a declarative descriptor that the frontend converts into a PTB at
# signing time. This mirrors the Aptos / NEAR strategy in this module.
#
# The two shapes the frontend handles are:
#   - native SUI:  `splitCoins(gas, [amount])` -> `transferObjects([coin], to)`
#   - non-native Coin<T>:  the frontend looks up its own `Coin<T>` objects
#       (or merges from balance), splits the requested amount, and transfers
#       to `depositAddress`. The descriptor only needs the type tag.

def build_sui_deposit_tx(
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
    decimals: int = 9,
    sender: str = "",
    deposit_memo: str = "",
) -> Dict:
    """Build a declarative SUI deposit descriptor.

    Returns a payload of the form::

        {
          "kind":           "sui_transfer",
          "chainId":        "sui",
          "standard":       "native" | "coin",
          "coinType":       "0x2::sui::SUI" | "<package>::<module>::<TYPE>",
          "tokenAddress":   "<coin type>",
          "depositAddress": "0x...",
          "amount":         "<smallest unit>",
          "decimals":       9,
          "sender":         "0x...",
          "depositMemo":    "" | "<1Click memo>"
        }

    SUI does not currently use `depositMemo` for any 1Click route (verified
    live 2026-05-11 — production quotes return `depositMode=SIMPLE` and no
    memo field), so the field will be empty in practice. We still pass it
    through so that the response shape stays uniform with the other
    chain-specific builders and we'll be ready if 1Click ever flips a chain
    to memo mode.
    """
    addr_raw = (token_address or "").strip()
    addr_lower = addr_raw.lower()
    amount_str = str(amount_smallest)

    is_native = (not addr_raw) or (addr_lower in SUI_NATIVE_ALIASES)
    coin_type = SUI_NATIVE_TYPE if is_native else addr_raw

    return {
        "kind": "sui_transfer",
        "chainId": "sui",
        "standard": "native" if is_native else "coin",
        "coinType": coin_type,
        "tokenAddress": coin_type,
        "depositAddress": deposit_address,
        "amount": amount_str,
        "decimals": int(decimals) if decimals else 9,
        "sender": sender,
        "depositMemo": deposit_memo or "",
    }


# ============================================================
# TRON cross-chain deposit
# ============================================================
#
# TRON has two transfer shapes:
#   - native TRX: `tronWeb.transactionBuilder.sendTrx(to, amount, sender)`
#   - TRC20:      `contract.transfer(to, amount).send({from: sender})`
# Both produce a signed transaction that the frontend submits via TronWeb.
# Building the unsigned tx on the backend would require speaking TronWeb's
# protobuf format and pulling a recent block header from a full node, which
# we currently don't have a stable RPC for. Emitting a declarative descriptor
# instead keeps the same wallet-adapter pattern as Aptos / SUI / NEAR.

def build_tron_deposit_tx(
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
    decimals: int = 6,
    sender: str = "",
    deposit_memo: str = "",
) -> Dict:
    """Build a declarative TRON deposit descriptor.

    Returns a payload of the form::

        {
          "kind":           "tron_transfer",
          "chainId":        "tron",
          "standard":       "trx" | "trc20",
          "tokenAddress":   "" | "T...",
          "depositAddress": "T...",
          "amount":         "<smallest unit, sun for TRX>",
          "decimals":       6,
          "sender":         "T...",
          "depositMemo":    ""
        }

    Native TRX uses 6 decimals (1 TRX = 1_000_000 sun). TRC20 tokens carry
    their own decimals in the descriptor for frontend display formatting;
    on-chain the integer amount is what's used.
    """
    addr_raw = (token_address or "").strip()
    addr_lower = addr_raw.lower()
    amount_str = str(amount_smallest)

    is_native = (not addr_raw) or (addr_lower in TRON_NATIVE_ALIASES)

    return {
        "kind": "tron_transfer",
        "chainId": "tron",
        "standard": "trx" if is_native else "trc20",
        "tokenAddress": "" if is_native else addr_raw,
        "depositAddress": deposit_address,
        "amount": amount_str,
        "decimals": int(decimals) if decimals else 6,
        "sender": sender,
        "depositMemo": deposit_memo or "",
    }


# ============================================================
# UTXO cross-chain deposit (BTC / ZEC / LTC / DOGE / BCH / DASH)
# ============================================================
#
# 1Click's UTXO-chain deposits all use `depositMode = SIMPLE`: the user just
# sends `amount` of the native coin to `depositAddress`. There is no contract
# call, no token approval, and (verified live 2026-05-11) no `depositMemo`.
# We still propagate `depositMemo` through to the descriptor so we won't have
# to plumb it again if 1Click ever flips a chain to `MEMO` mode (e.g. an
# OP_RETURN tag on BTC). When `memo` is empty the frontend should just emit a
# plain send; otherwise it should attach the memo per chain convention.
#
# Because the descriptor only carries the destination address and amount,
# the frontend's wallet integration (UniSat, Xverse, Phantom-BTC, the user's
# own ZEC desktop wallet, ...) is responsible for UTXO selection, fee
# estimation, and signing. Backend-side PSBT assembly is intentionally NOT
# attempted here — it would require a full UTXO indexer per chain, and is
# orthogonal to the cross-chain orchestration this module is responsible for.

def build_utxo_deposit_tx(
    chain_id: str,
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
    decimals: int = 8,
    symbol: str = "",
    deposit_memo: str = "",
) -> Dict:
    """Build a declarative UTXO-chain deposit descriptor.

    Returns a payload of the form::

        {
          "kind":           "utxo_transfer",
          "chain":          "btc" | "zec" | "ltc" | ...,
          "chainId":        "<original chain id>",
          "tokenAddress":   "",
          "depositAddress": "bc1q..." | "t1..." | ...,
          "amount":         "<smallest unit, sat / zat / litoshi / ...>",
          "decimals":       8,
          "symbol":         "BTC" | "ZEC" | ...,
          "depositMemo":    "" | "<1Click memo>"
        }

    `chain` is normalized to the short form (`btc` for "btc"/"bitcoin",
    `zec` for "zec"/"zcash", etc.) so the frontend can drive a single
    switch statement over wallet adapters.
    """
    chain_str = (str(chain_id) if chain_id is not None else "").lower()
    if chain_str in ("bitcoin", "btc"):
        chain_short = "btc"
    elif chain_str in ("zcash", "zec"):
        chain_short = "zec"
    elif chain_str in ("litecoin", "ltc"):
        chain_short = "ltc"
    elif chain_str in ("dogecoin", "doge"):
        chain_short = "doge"
    elif chain_str in ("bch", "dash"):
        chain_short = chain_str
    else:
        chain_short = chain_str

    return {
        "kind": "utxo_transfer",
        "chain": chain_short,
        "chainId": str(chain_id) if chain_id is not None else "",
        "tokenAddress": "",
        "depositAddress": deposit_address,
        "amount": str(amount_smallest),
        "decimals": int(decimals) if decimals else 8,
        "symbol": symbol or chain_short.upper(),
        "depositMemo": deposit_memo or "",
    }
