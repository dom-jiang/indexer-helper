"""
Solana transaction assembler used by the cross-chain pre-swap path.

When the user is on Solana and wants to swap an arbitrary SPL token to a
token on another chain, our two-stage pre-swap route does:

    Stage A  Jupiter swap  : tokenIn -> intermediate (USDC/USDT/SOL)
                              with the OUTPUT delivered to the 1Click
                              `depositAddress` (an SPL ATA derived from it
                              for SPL intermediates, or the wallet itself
                              for native SOL).

    Stage B  1Click bridge : intermediate (Solana) -> tokenOut (toChain).
                              1Click watches the depositAddress, processes
                              the deposit, and delivers tokenOut to
                              `recipient` on the destination chain.

Verification (2026-05-09) showed:
  - 1Click returns a brand-new depositAddress for every /quote;
  - the wallet does not exist on-chain yet, so its USDC/USDT ATAs do NOT
    exist either.
  - native SOL deposits work without an ATA — a SystemProgram::Transfer
    to the wallet is enough.

Therefore for SPL intermediates we MUST prepend an
`createAssociatedTokenAccountIdempotent` instruction so the destination ATA
exists by the time Jupiter's `swapInstruction` writes into it. For native
SOL output we use Jupiter's `nativeDestinationAccount` parameter and skip
the ATA step entirely.

This module exposes a single entry point, `assemble_jupiter_preswap_tx`,
which takes the parsed Jupiter `/swap/v2/build` response plus the bridge
deposit address and emits an unsigned base64 VersionedTransaction in the
SAME shape as `build_solana_deposit_tx` (Solana signable tx response).
"""

from __future__ import annotations

import base64
import time
from typing import Dict, Iterable, List, Optional

from loguru import logger

try:
    from solders.pubkey import Pubkey  # type: ignore
    from solders.hash import Hash  # type: ignore
    from solders.instruction import Instruction, AccountMeta  # type: ignore
    from solders.message import MessageV0  # type: ignore
    from solders.transaction import VersionedTransaction  # type: ignore
    from solders.signature import Signature  # type: ignore
    from solders.address_lookup_table_account import AddressLookupTableAccount  # type: ignore
    _SOLDERS_AVAILABLE = True
except Exception:  # pragma: no cover
    _SOLDERS_AVAILABLE = False

# Re-use the helpers/IDs that the deposit-tx builder already exposes so we
# stay consistent (same ATA derivation, same program IDs).
from cross_chain_tx_builder import (
    _ATA_PROGRAM_ID,
    _TOKEN_PROGRAM_ID,
    _SYSTEM_PROGRAM_ID,
    _SOL_TX_LIFETIME_MS,
    _ata,
    _create_ata_idempotent_ix,
    _fetch_latest_blockhash,
    _solana_rpc_url,
    SolanaDepositTxBuildError,
)


# Native SOL mint constant; matches Jupiter / SPL convention. Output mint
# equal to this means native SOL was unwrapped into `nativeDestinationAccount`.
NATIVE_SOL_MINT = "So11111111111111111111111111111111111111112"


def jupiter_alt_pubkey_strings(build_resp: Dict) -> List[str]:
    """Base58 ALT account pubkeys from Jupiter `/swap/v2/build`."""
    if not isinstance(build_resp, dict):
        return []
    raw = build_resp.get("addressesByLookupTableAddress")
    if isinstance(raw, dict):
        return [str(k).strip() for k in raw.keys() if str(k).strip()]
    listed = build_resp.get("addressLookupTableAddresses")
    if isinstance(listed, list):
        return [str(a).strip() for a in listed if str(a).strip()]
    return []


def extract_alt_pubkeys_from_versioned_tx_b64(tx_b64: str) -> List[str]:
    """Read v0 message ALT account keys from a base64 serialized VersionedTransaction."""
    if not _SOLDERS_AVAILABLE or not tx_b64:
        return []
    try:
        raw = base64.b64decode(str(tx_b64))
        vtx = VersionedTransaction.from_bytes(raw)
    except Exception:
        return []
    lookups = getattr(vtx.message, "address_table_lookups", None)
    if not lookups:
        return []
    return [str(lut.account_key) for lut in lookups]


def extract_recent_blockhash_from_versioned_tx_b64(tx_b64: str) -> str:
    """Return recent blockhash embedded in a serialized v0 VersionedTransaction."""
    if not _SOLDERS_AVAILABLE or not tx_b64:
        return ""
    try:
        raw = base64.b64decode(str(tx_b64))
        vtx = VersionedTransaction.from_bytes(raw)
        bh = getattr(vtx.message, "recent_blockhash", None)
        return str(bh) if bh else ""
    except Exception:
        return ""


def enrich_solana_tx_envelope(
    envelope: Dict,
    *,
    alt_pubkeys: Optional[List[str]] = None,
    instructions: Optional[List[Dict]] = None,
) -> Dict:
    """Attach fields the web client needs to recompile v0 txs with priority fees."""
    out = dict(envelope)
    alts = [str(a).strip() for a in (alt_pubkeys or []) if str(a).strip()]
    if not alts and out.get("transaction"):
        alts = extract_alt_pubkeys_from_versioned_tx_b64(str(out.get("transaction")))
    # Always emit the field so clients never branch on key presence.
    out["addressLookupTableAddresses"] = alts
    if instructions:
        out["instructions"] = instructions
    tx_b64 = str(out.get("transaction") or "")
    if tx_b64 and _SOLDERS_AVAILABLE:
        try:
            out["transactionSize"] = len(base64.b64decode(tx_b64))
        except Exception:
            pass
        if not out.get("recentBlockhash"):
            embedded_bh = extract_recent_blockhash_from_versioned_tx_b64(tx_b64)
            if embedded_bh:
                out["recentBlockhash"] = embedded_bh
    if out.get("transaction") and not out.get("txValidUntil"):
        out["txValidUntil"] = int(time.time() * 1000) + _SOL_TX_LIFETIME_MS
    return out


def okx_solana_tx_to_base64(tx_data: Optional[Dict]) -> str:
    """
    OKX Solana /swap returns a serialized VersionedTransaction in ``tx.data``
    as **base58**. Unified API consumers (web3.js) expect standard **base64**.
    """
    if not isinstance(tx_data, dict):
        return ""
    raw = str(tx_data.get("data") or "").strip()
    if not raw:
        return ""

    def _validate_and_b64(blob: bytes) -> Optional[str]:
        if not blob:
            return None
        if _SOLDERS_AVAILABLE:
            try:
                VersionedTransaction.from_bytes(blob)
            except Exception:
                return None
        return base64.b64encode(blob).decode("ascii")

    if raw.startswith("0x"):
        out = _validate_and_b64(bytes.fromhex(raw[2:]))
        if out:
            return out

    try:
        import base58

        out = _validate_and_b64(base58.b58decode(raw))
        if out:
            return out
    except Exception:
        pass

    try:
        out = _validate_and_b64(base64.b64decode(raw))
        if out:
            return out
    except Exception:
        pass

    logger.warning("okx_solana_tx_to_base64: could not normalize OKX Solana tx.data")
    return raw


def _decode_account(acc: Dict) -> "AccountMeta":
    """Convert Jupiter API account spec into solders AccountMeta."""
    return AccountMeta(
        pubkey=Pubkey.from_string(acc["pubkey"]),
        is_signer=bool(acc.get("isSigner", False)),
        is_writable=bool(acc.get("isWritable", False)),
    )


def _decode_instruction(ix: Dict) -> "Instruction":
    """Convert one Jupiter API instruction into a solders Instruction."""
    return Instruction(
        program_id=Pubkey.from_string(ix["programId"]),
        accounts=[_decode_account(a) for a in ix.get("accounts", [])],
        data=base64.b64decode(ix.get("data", "") or ""),
    )


def _decode_alts(raw: Optional[Dict[str, List[str]]]) -> List["AddressLookupTableAccount"]:
    """Convert Jupiter's `addressesByLookupTableAddress` into solders ALT
    account objects. Jupiter ships the addresses inline so no extra RPC
    call is required to resolve them.
    """
    if not raw:
        return []
    alts: List[AddressLookupTableAccount] = []
    for table_addr, addrs in raw.items():
        try:
            alts.append(
                AddressLookupTableAccount(
                    key=Pubkey.from_string(table_addr),
                    addresses=[Pubkey.from_string(a) for a in addrs],
                )
            )
        except Exception as e:  # malformed entry — skip but warn
            logger.warning(
                f"solana_tx_assembler: skipping malformed ALT {table_addr}: {e}"
            )
    return alts


def _decode_blockhash(meta: Dict) -> str:
    """Jupiter returns blockhash as a number[] of 32 bytes. Encode back to base58."""
    raw = meta.get("blockhash") if isinstance(meta, dict) else None
    if isinstance(raw, list) and len(raw) == 32:
        return str(Hash(bytes(raw)))
    if isinstance(raw, str) and raw:
        return raw
    raise SolanaDepositTxBuildError(
        "Jupiter /build: missing or invalid blockhashWithMetadata.blockhash"
    )


def _flatten_jupiter_instructions(build_resp: Dict) -> List[Dict]:
    """Concat Jupiter /build instructions in the canonical order Jupiter docs
    show: computeBudget, setup, swap, cleanup, other. `tipInstruction` is
    intentionally skipped — we do not submit through Jupiter /submit, the
    user signs and broadcasts via their wallet so the SOL tip is unnecessary.
    """
    out: List[Dict] = []
    for key in ("computeBudgetInstructions", "setupInstructions"):
        out.extend(build_resp.get(key) or [])
    swap_ix = build_resp.get("swapInstruction")
    if not swap_ix:
        raise SolanaDepositTxBuildError("Jupiter /build: missing swapInstruction")
    out.append(swap_ix)
    cleanup = build_resp.get("cleanupInstruction")
    if cleanup:
        out.append(cleanup)
    out.extend(build_resp.get("otherInstructions") or [])
    return out


def assemble_jupiter_preswap_tx(
    *,
    sender: str,
    deposit_address: str,
    intermediate_mint: str,
    build_resp: Dict,
) -> Dict:
    """Take a Jupiter `/swap/v2/build` response and return a
    base64 VersionedTransaction ready for the user's wallet to sign.

    For SPL intermediates we prepend `createAssociatedTokenAccountIdempotent`
    targeting `(deposit_address, intermediate_mint)` so the bridge deposit
    ATA always exists when the swap instruction runs (1Click never pre-creates
    it — every order spawns a fresh depositAddress).

    For the native SOL case (`intermediate_mint == NATIVE_SOL_MINT`) the
    Jupiter request must already use `nativeDestinationAccount=deposit_address`,
    so no ATA work is needed and we skip the prepend.

    Args:
        sender: User wallet pubkey (paper-fee payer / signer).
        deposit_address: 1Click bridge deposit wallet pubkey.
        intermediate_mint: Mint of the Stage-A output token. For native SOL
            pass `NATIVE_SOL_MINT`.
        build_resp: Parsed JSON returned by `jupiter_build`.

    Returns:
        ``{"transaction": <base64>, "format": "base64", "txValidUntil": ...,
            "lastValidBlockHeight": ..., "recentBlockhash": ...}``
    """
    if not _SOLDERS_AVAILABLE:
        raise SolanaDepositTxBuildError(
            "Solana tx build: `solders` package not installed on the backend"
        )
    if not sender:
        raise SolanaDepositTxBuildError(
            "Solana tx build: sender is required for pre-swap assembly"
        )
    if not deposit_address:
        raise SolanaDepositTxBuildError(
            "Solana tx build: deposit_address is required for pre-swap assembly"
        )

    try:
        sender_pk = Pubkey.from_string(sender)
        deposit_pk = Pubkey.from_string(deposit_address)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: invalid pubkey sender={sender} deposit={deposit_address}: {e}"
        ) from e

    blockhash_meta = build_resp.get("blockhashWithMetadata") or {}
    blockhash_str = _decode_blockhash(blockhash_meta)
    last_valid = int(blockhash_meta.get("lastValidBlockHeight") or 0)

    instructions: List["Instruction"] = []

    # Prepend createATAIdempotent unless the intermediate is native SOL —
    # in that case Jupiter unwraps to `nativeDestinationAccount` (the
    # deposit wallet itself) and no SPL ATA is involved.
    if (intermediate_mint or "").lower() != NATIVE_SOL_MINT.lower():
        try:
            mint_pk = Pubkey.from_string(intermediate_mint)
        except Exception as e:
            raise SolanaDepositTxBuildError(
                f"Solana tx build: invalid intermediate mint {intermediate_mint}: {e}"
            ) from e
        instructions.append(_create_ata_idempotent_ix(sender_pk, deposit_pk, mint_pk))

    try:
        for ix_json in _flatten_jupiter_instructions(build_resp):
            instructions.append(_decode_instruction(ix_json))
    except SolanaDepositTxBuildError:
        raise
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: failed to decode Jupiter instructions: {e}"
        ) from e

    alts = _decode_alts(build_resp.get("addressesByLookupTableAddress"))

    try:
        msg = MessageV0.try_compile(
            payer=sender_pk,
            instructions=instructions,
            address_lookup_table_accounts=alts,
            recent_blockhash=Hash.from_string(blockhash_str),
        )
        num_required = msg.header.num_required_signatures
        placeholder_sigs = [Signature.default() for _ in range(num_required)]
        tx = VersionedTransaction.populate(msg, placeholder_sigs)
        tx_bytes = bytes(tx)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: failed to compile/serialize VersionedTransaction: {e}"
        ) from e

    alt_keys = jupiter_alt_pubkey_strings(build_resp)
    return enrich_solana_tx_envelope(
        {
            "transaction": base64.b64encode(tx_bytes).decode("ascii"),
            "format": "base64",
            "txValidUntil": int(time.time() * 1000) + _SOL_TX_LIFETIME_MS,
            "lastValidBlockHeight": last_valid,
            "recentBlockhash": blockhash_str,
        },
        alt_pubkeys=alt_keys,
    )


def assemble_jupiter_swap_tx(
    *,
    sender: str,
    build_resp: Dict,
) -> Dict:
    """Same-chain Jupiter swap from `/swap/v2/build` (no bridge deposit ATA)."""
    if not _SOLDERS_AVAILABLE:
        raise SolanaDepositTxBuildError(
            "Solana tx build: `solders` package not installed on the backend"
        )
    if not sender:
        raise SolanaDepositTxBuildError(
            "Solana tx build: sender is required for Jupiter swap assembly"
        )

    try:
        sender_pk = Pubkey.from_string(sender)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: invalid sender pubkey {sender}: {e}"
        ) from e

    blockhash_meta = build_resp.get("blockhashWithMetadata") or {}
    blockhash_str = _decode_blockhash(blockhash_meta)
    last_valid = int(blockhash_meta.get("lastValidBlockHeight") or 0)

    try:
        instructions = [_decode_instruction(ix_json) for ix_json in _flatten_jupiter_instructions(build_resp)]
    except SolanaDepositTxBuildError:
        raise
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: failed to decode Jupiter instructions: {e}"
        ) from e

    # Jupiter /build ships full ALT contents in addressesByLookupTableAddress
    # (same as preswap). Do not rely on addressLookupTableAddresses alone.
    alt_dict = build_resp.get("addressesByLookupTableAddress")
    if alt_dict:
        alts = _decode_alts(alt_dict)
    else:
        alt_pubkeys = jupiter_alt_pubkey_strings(build_resp)
        alts = _fetch_address_lookup_table_accounts(alt_pubkeys)

    try:
        msg = MessageV0.try_compile(
            payer=sender_pk,
            instructions=instructions,
            address_lookup_table_accounts=alts,
            recent_blockhash=Hash.from_string(blockhash_str),
        )
        num_required = msg.header.num_required_signatures
        placeholder_sigs = [Signature.default() for _ in range(num_required)]
        tx = VersionedTransaction.populate(msg, placeholder_sigs)
        tx_bytes = bytes(tx)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: failed to compile/serialize VersionedTransaction: {e}"
        ) from e

    alt_keys = jupiter_alt_pubkey_strings(build_resp)
    return enrich_solana_tx_envelope(
        {
            "transaction": base64.b64encode(tx_bytes).decode("ascii"),
            "format": "base64",
            "txValidUntil": int(time.time() * 1000) + _SOL_TX_LIFETIME_MS,
            "lastValidBlockHeight": last_valid,
            "recentBlockhash": blockhash_str,
        },
        alt_pubkeys=alt_keys,
    )


def derive_destination_token_account(
    deposit_address: str,
    intermediate_mint: str,
) -> Optional[str]:
    """Compute the deterministic SPL Associated Token Account that Jupiter
    should write the swap output to. Returns None for the native SOL case
    (caller should use Jupiter's `nativeDestinationAccount` instead).
    """
    if not _SOLDERS_AVAILABLE:
        raise SolanaDepositTxBuildError(
            "Solana tx build: `solders` package not installed on the backend"
        )
    if (intermediate_mint or "").lower() == NATIVE_SOL_MINT.lower():
        return None
    try:
        owner = Pubkey.from_string(deposit_address)
        mint = Pubkey.from_string(intermediate_mint)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: invalid (deposit/mint) pair: {e}"
        ) from e
    return str(_ata(owner, mint))


def _fetch_address_lookup_table_accounts(
    alt_pubkeys: Iterable[str],
) -> List["AddressLookupTableAccount"]:
    """Resolve Titan/Jupiter ALT pubkeys via Solana RPC `getAddressLookupTable`."""
    import requests

    rpc_url = _solana_rpc_url()
    alts: List[AddressLookupTableAccount] = []
    for key_str in alt_pubkeys:
        if not key_str:
            continue
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAddressLookupTable",
            "params": [key_str, {"encoding": "base64"}],
        }
        try:
            resp = requests.post(rpc_url, json=body, timeout=8)
            resp.raise_for_status()
            value = (resp.json() or {}).get("result", {}).get("value")
            if not value:
                logger.warning(f"solana_tx_assembler: ALT not found on-chain: {key_str}")
                continue
            addresses = [
                Pubkey.from_string(addr)
                for addr in (value.get("state") or {}).get("addresses") or []
            ]
            alts.append(AddressLookupTableAccount(key=Pubkey.from_string(key_str), addresses=addresses))
        except Exception as e:
            logger.warning(f"solana_tx_assembler: failed to fetch ALT {key_str}: {e}")
    return alts


def _compile_versioned_tx(
    *,
    sender_pk: "Pubkey",
    instructions: List["Instruction"],
    alts: List["AddressLookupTableAccount"],
    blockhash_str: str,
    last_valid: int,
) -> Dict:
    try:
        msg = MessageV0.try_compile(
            payer=sender_pk,
            instructions=instructions,
            address_lookup_table_accounts=alts,
            recent_blockhash=Hash.from_string(blockhash_str),
        )
        num_required = msg.header.num_required_signatures
        placeholder_sigs = [Signature.default() for _ in range(num_required)]
        tx = VersionedTransaction.populate(msg, placeholder_sigs)
        tx_bytes = bytes(tx)
    except Exception as e:
        raise SolanaDepositTxBuildError(
            f"Solana tx build: failed to compile/serialize VersionedTransaction: {e}"
        ) from e

    envelope = {
        "transaction": base64.b64encode(tx_bytes).decode("ascii"),
        "format": "base64",
        "txValidUntil": int(time.time() * 1000) + _SOL_TX_LIFETIME_MS,
        "lastValidBlockHeight": last_valid,
        "recentBlockhash": blockhash_str,
    }
    alt_keys = [str(alt.key) for alt in alts] if alts else []
    return enrich_solana_tx_envelope(envelope, alt_pubkeys=alt_keys)


def assemble_titan_swap_tx(
    *,
    sender: str,
    titan_data: Dict,
    prepend_instructions: Optional[List["Instruction"]] = None,
) -> Dict:
    """Build a signable base64 VersionedTransaction from a Titan quote payload.

    If Titan returned a pre-built `swapTransaction`, pass it through directly.
    Otherwise compile route `instructions` + RPC-resolved ALTs with a fresh
    blockhash (same pattern as the frontend `transfer_solana` helper).
    """
    if not _SOLDERS_AVAILABLE:
        raise SolanaDepositTxBuildError(
            "Solana tx build: `solders` package not installed on the backend"
        )
    if not sender:
        raise SolanaDepositTxBuildError("Solana tx build: sender is required")

    prebuilt = str(titan_data.get("swapTransaction") or "").strip()
    if prebuilt:
        alt_pubkeys = [
            str(a).strip()
            for a in (titan_data.get("addressLookupTables") or [])
            if str(a).strip()
        ]
        if not alt_pubkeys:
            alt_pubkeys = extract_alt_pubkeys_from_versioned_tx_b64(prebuilt)
        embedded_bh = extract_recent_blockhash_from_versioned_tx_b64(prebuilt)
        envelope: Dict = {
            "transaction": prebuilt,
            "format": "base64",
            "txValidUntil": int(time.time() * 1000) + _SOL_TX_LIFETIME_MS,
        }
        if embedded_bh:
            envelope["recentBlockhash"] = embedded_bh
        else:
            bh = _fetch_latest_blockhash()
            if bh:
                envelope["recentBlockhash"] = bh[0]
                envelope["lastValidBlockHeight"] = bh[1]
        return enrich_solana_tx_envelope(
            envelope,
            alt_pubkeys=alt_pubkeys,
            instructions=titan_data.get("instructions"),
        )

    bh = _fetch_latest_blockhash()
    if not bh:
        raise SolanaDepositTxBuildError(
            "Solana tx build: failed to fetch recent blockhash for Titan assembly"
        )
    blockhash_str, last_valid = bh

    try:
        sender_pk = Pubkey.from_string(sender)
    except Exception as e:
        raise SolanaDepositTxBuildError(f"Solana tx build: invalid sender {sender}: {e}") from e

    instructions: List[Instruction] = list(prepend_instructions or [])
    for ix_json in titan_data.get("instructions") or []:
        instructions.append(_decode_instruction(ix_json))

    alt_pubkeys = [
        str(a).strip() for a in (titan_data.get("addressLookupTables") or []) if str(a).strip()
    ]
    alts = _fetch_address_lookup_table_accounts(alt_pubkeys)
    compiled = _compile_versioned_tx(
        sender_pk=sender_pk,
        instructions=instructions,
        alts=alts,
        blockhash_str=blockhash_str,
        last_valid=last_valid,
    )
    return enrich_solana_tx_envelope(
        compiled,
        alt_pubkeys=alt_pubkeys,
        instructions=titan_data.get("instructions"),
    )


def assemble_titan_preswap_tx(
    *,
    sender: str,
    deposit_address: str,
    intermediate_mint: str,
    titan_data: Dict,
) -> Dict:
    """Titan Stage-A pre-swap tx with optional createATA for bridge deposit."""
    if not _SOLDERS_AVAILABLE:
        raise SolanaDepositTxBuildError(
            "Solana tx build: `solders` package not installed on the backend"
        )

    prepend: List[Instruction] = []
    if (intermediate_mint or "").lower() != NATIVE_SOL_MINT.lower():
        sender_pk = Pubkey.from_string(sender)
        deposit_pk = Pubkey.from_string(deposit_address)
        mint_pk = Pubkey.from_string(intermediate_mint)
        prepend.append(_create_ata_idempotent_ix(sender_pk, deposit_pk, mint_pk))

    return assemble_titan_swap_tx(
        sender=sender,
        titan_data=titan_data,
        prepend_instructions=prepend,
    )
