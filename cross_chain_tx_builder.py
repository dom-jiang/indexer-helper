"""
Build source-chain deposit transactions for cross-chain swaps.

After OmniBridge / NearIntents returns a `depositAddress`, the user must
transfer tokens on the source chain to that address. This module produces
tx payloads in the SAME shape as same-chain swap tx per source chain type,
so frontend can use one code path per chain.

Shapes:
  EVM   -> {to, data, value, gasLimit, chainId}
  Aptos -> {function, type_arguments, arguments}  (Move entry function)
  Solana-> {transaction, format}
           * format="base64"        (same-chain Jupiter/OKX): transaction is a
                                     pre-built VersionedTransaction bytes, base64.
           * format="sol_transfer"  (cross-chain native SOL): transaction is
                                     base64(JSON({depositAddress, amount, decimals, depositMemo})).
           * format="spl_transfer"  (cross-chain SPL token):  transaction is
                                     base64(JSON({depositAddress, mint, amount, decimals, depositMemo})).
           Uniform outer shape lets the frontend run one base64-decode first,
           then dispatch on `format` for the follow-up handling.
"""

import base64
import json
from typing import Dict, Optional
from swap_utils import is_native_token, normalize_evm_address


APTOS_NATIVE_ALIASES = {"0xa", "0x1::aptos_coin::aptoscoin", "apt", ""}
SOLANA_NATIVE_MINTS = {"so11111111111111111111111111111111111111112", ""}


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


def _encode_solana_payload(payload: Dict) -> str:
    """Pack the cross-chain Solana deposit descriptor into base64(JSON).

    Kept outside the builder so tests / frontends can share the encoding.
    """
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.b64encode(encoded).decode("ascii")


def build_solana_deposit_tx(
    token_address: str,
    deposit_address: str,
    amount_smallest: str,
    decimals: int,
    deposit_memo: str = "",
) -> Dict:
    """
    Build a Solana cross-chain deposit descriptor.

    Because Solana transactions require a recent blockhash (2-min lifespan)
    fetched from an RPC, we do NOT assemble a signable base64 VersionedTransaction
    on the backend. Instead we emit a descriptor in the SAME outer shape as the
    same-chain path (`{transaction, format}`) and stash the descriptor fields
    inside `transaction` as base64(JSON). This keeps the response schema uniform
    for frontend type-definitions / parsers; the frontend decodes once, then
    dispatches on `format`:

      - "base64"        -> VersionedTransaction.deserialize(bytes)
      - "sol_transfer"  -> JSON.parse(text) -> build System.transfer + memo
      - "spl_transfer"  -> JSON.parse(text) -> build SPL transferChecked + ATA + memo
    """
    addr_lower = (token_address or "").lower().strip()

    if addr_lower in SOLANA_NATIVE_MINTS:
        payload = {
            "depositAddress": deposit_address,
            "amount": str(amount_smallest),
            "decimals": int(decimals) if decimals else 9,
            "depositMemo": deposit_memo or "",
        }
        return {
            "transaction": _encode_solana_payload(payload),
            "format": "sol_transfer",
        }

    payload = {
        "depositAddress": deposit_address,
        "mint": token_address,
        "amount": str(amount_smallest),
        "decimals": int(decimals) if decimals else 6,
        "depositMemo": deposit_memo or "",
    }
    return {
        "transaction": _encode_solana_payload(payload),
        "format": "spl_transfer",
    }
