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
from typing import Dict, Optional, Tuple

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

    return {
        "to": normalize_evm_address(token_address),
        "data": calldata,
        "value": "0x0",
        "gasLimit": "0x11170",
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
