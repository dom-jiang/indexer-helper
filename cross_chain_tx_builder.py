"""
Build source-chain deposit transactions for cross-chain swaps.

After OmniBridge / NearIntents returns a `depositAddress`, the user must
transfer tokens on the source chain to that address. This module produces
tx payloads in the SAME shape as same-chain swap tx per source chain type,
so frontend can use one code path per chain.

Shapes:
  EVM   -> {to, data, value, gasLimit, chainId}
  Aptos -> {function, type_arguments, arguments}  (Move entry function)
  Solana-> {transaction, format, depositAddress, mint?, amount, decimals}
           format is "sol_transfer" or "spl_transfer" for cross-chain,
           or "base64" for same-chain (Jupiter pre-built tx).
"""

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

    - Native APT:  0x1::aptos_account::transfer(recipient, amount)
    - Other coin:  0x1::aptos_account::transfer_coins<CoinType>(recipient, amount)
    """
    addr_lower = (token_address or "").lower().strip()
    if addr_lower in APTOS_NATIVE_ALIASES:
        return {
            "function": "0x1::aptos_account::transfer",
            "type_arguments": [],
            "arguments": [deposit_address, str(amount_smallest)],
        }

    return {
        "function": "0x1::aptos_account::transfer_coins",
        "type_arguments": [token_address],
        "arguments": [deposit_address, str(amount_smallest)],
    }


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
    fetched from an RPC, we return a descriptor with format "sol_transfer"
    or "spl_transfer" instead of a prebuilt base64 tx. Frontend uses
    @solana/web3.js + @solana/spl-token to construct and sign.

    Outer shape matches same-chain Solana tx (both have "transaction" and
    "format" keys), so frontend dispatches on `format`:
      - "base64"        -> sign pre-built tx (same-chain Jupiter/OKX)
      - "sol_transfer"  -> System.transfer
      - "spl_transfer"  -> SPL Token.transfer
    """
    addr_lower = (token_address or "").lower().strip()

    if addr_lower in SOLANA_NATIVE_MINTS:
        return {
            "transaction": "",
            "format": "sol_transfer",
            "depositAddress": deposit_address,
            "amount": str(amount_smallest),
            "decimals": int(decimals) if decimals else 9,
            "depositMemo": deposit_memo or "",
        }

    return {
        "transaction": "",
        "format": "spl_transfer",
        "depositAddress": deposit_address,
        "mint": token_address,
        "amount": str(amount_smallest),
        "decimals": int(decimals) if decimals else 6,
        "depositMemo": deposit_memo or "",
    }
