"""
MCA withdraw from Burrow + NEP-141 transfer to Near Intents 1Click deposit address.

Mirrors frontend `withdrawFromMca` (`src/services/lending/actions/withdraw.ts`) when:
  - simpleWithdrawData is absent (empty simple_withdraw prefix),
  - Burrow oracle path disabled (always `logic.execute`),
  - no collateral decrease (single `Withdraw`),
  - `tansfer_txs_query`-style transfer toward `depositAddress`.

Used for NEAR Lending -> arbitrary 1Click destination: one signature + multichain relayer.

"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from mca_burrow_auto import near_view_call, _burrow_logic_contract

# Same semantic as TOKEN_STORAGE_DEPOSIT_READ on the frontend (`0.00125` NEAR)
_TOKEN_STORAGE_DEPOSIT_READ_NEAR = Decimal("0.00125")


def _tgas_yocto(gas_tgas: int) -> str:
    return str(int(Decimal(gas_tgas) * Decimal(10**12)))


def _ndeposit_yocto(near_human: str) -> str:
    return str(int(Decimal(str(near_human)) * Decimal(10**24)))


def _serialize_args(params: Dict[str, Any]) -> str:
    """Match TS `serializationObj` (JSON stringify, no whitespace)."""
    return json.dumps(params or {}, separators=(",", ":"), ensure_ascii=False)


def _near_view_optional(
    network_id: str, contract_id: str, method: str, args: Dict[str, Any]
) -> Any:
    try:
        return near_view_call(network_id, contract_id, method, args)
    except Exception as e:
        logger.warning(f"mca_withdraw_cross_intents view {contract_id}.{method}: {e}")
        return None


def get_nonce_deadline(network_id: str, mca_account_id: str) -> Tuple[str, str]:
    """Align with frontend `get_nonce_deadline` (10 min expiry, ms string)."""
    mca = str(mca_account_id or "").strip()
    if not mca:
        raise ValueError("mcaAccountId is required")
    nonce = _near_view_optional(network_id, mca, "get_nonce", {})
    if nonce is None:
        raise RuntimeError(f"NEAR get_nonce failed for {mca}")
    deadline = str(int(time.time() * 1000) + 10 * 60 * 1000)
    return str(nonce), deadline


def build_mca_register_token_tx_requests(
    network_id: str, token_id: str, mca_account_id: str
) -> List[Dict[str, Any]]:
    """Port of `mca_register_token_tx_query` — storage_deposit when MCA lacks storage."""
    tid = str(token_id or "").strip()
    mca = str(mca_account_id or "").strip()
    if not tid or not mca:
        return []
    bal = _near_view_optional(network_id, tid, "storage_balance_of", {"account_id": mca})
    if bal:
        return []
    fc = [
        {
            "method_name": "storage_deposit",
            "args": _serialize_args(
                {"registration_only": False, "account_id": mca},
            ),
            "gas": _tgas_yocto(10),
            "deposit": _ndeposit_yocto(str(_TOKEN_STORAGE_DEPOSIT_READ_NEAR)),
        }
    ]
    return [
        {
            "FunctionCall": {
                "receiver_id": tid,
                "function_calls": fc,
            }
        }
    ]


def build_transfer_txs_to_intents_deposit(
    *,
    network_id: str,
    token_id: str,
    deposit_address: str,
    amount_token_smallest: str,
    frontend_target_chain: str,
) -> List[Dict[str, Any]]:
    """
    Port of `tansfer_txs_query`:
    - Only checks storage_balance_of on deposit recipient when frontend_target_chain === 'near'
    - Otherwise skips check and always behaves as TS `isRegistered=false` branch.
    """
    tid = str(token_id or "").strip()
    dep = str(deposit_address or "").strip()
    amt = str(amount_token_smallest or "").strip()
    if not tid or not dep or not amt:
        raise ValueError("tokenId, depositAddress, amount are required")

    function_calls: List[Dict[str, Any]] = []
    is_registered = False
    tchain = str(frontend_target_chain or "").strip().lower()
    if tchain == "near":
        bal = _near_view_optional(network_id, tid, "storage_balance_of", {"account_id": dep})
        is_registered = bool(bal)

    if not is_registered:
        function_calls.append(
            {
                "method_name": "storage_deposit",
                "args": _serialize_args(
                    {"account_id": dep, "registration_only": True},
                ),
                "gas": _tgas_yocto(10),
                "deposit": _ndeposit_yocto(str(_TOKEN_STORAGE_DEPOSIT_READ_NEAR)),
            }
        )

    function_calls.append(
        {
            "method_name": "ft_transfer",
            "args": _serialize_args(
                {"receiver_id": dep, "amount": amt, "memo": None},
            ),
            "gas": _tgas_yocto(10),
            "deposit": "1",
        }
    )
    return [
        {
            "FunctionCall": {
                "receiver_id": tid,
                "function_calls": function_calls,
                "interval_block": 2,
            }
        }
    ]


def build_withdraw_execute_tx_request(
    logic_contract_id: str, token_id: str, amount_burrow_inner: str
) -> Dict[str, Any]:
    """Single `Withdraw` via Burrow logic `execute` (non-oracle branch)."""
    logic = str(logic_contract_id or "").strip()
    withdraw_action = {"Withdraw": {"token_id": str(token_id), "max_amount": str(amount_burrow_inner)}}
    withdraw_fn = {
        "method_name": "execute",
        "args": _serialize_args({"actions": [withdraw_action]}),
        "gas": _tgas_yocto(120),
        "deposit": "1",
    }
    return {
        "FunctionCall": {
            "receiver_id": logic,
            "function_calls": [withdraw_fn],
        }
    }


def assemble_mca_withdraw_to_intents_business(
    *,
    network_id: str,
    mca_account_id: str,
    token_id_nep141: str,
    amount_token_smallest: str,
    amount_burrow_inner: str,
    intents_deposit_address: str,
    frontend_target_chain: str,
    sign_chain_is_near: bool,
    simple_withdraw_tx: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Returns the `businessMap` equivalent (nonce, deadline, tx_requests).

    When `sign_chain_is_near`, prepends NO simple_withdraw txs (frontend signs exec directly —
    separate path).

    Args:
      `amount_burrow_inner`: Burrow `Withdraw.max_amount` (metadata / inner decimals as on TS side).
      `amount_token_smallest`: NEP-141 smallest units attached to ft_transfer (`amountToken`).
    """
    mca = str(mca_account_id or "").strip()
    tid = str(token_id_nep141 or "").strip()
    if not mca or not tid:
        raise ValueError("mcaAccountId and tokenIn (NEP-141) are required")

    logic = _burrow_logic_contract(network_id)
    if not logic:
        raise RuntimeError("Burrow logic contract not configured")

    nonce, deadline = get_nonce_deadline(network_id, mca)

    tx_requests: List[Dict[str, Any]] = []

    if not sign_chain_is_near:
        prepend = simple_withdraw_tx if simple_withdraw_tx else []
        tx_requests.extend(prepend)

    tx_requests.extend(build_mca_register_token_tx_requests(network_id, tid, mca))

    amt_br = str(amount_burrow_inner or "").strip() or str(amount_token_smallest)
    tx_requests.append(build_withdraw_execute_tx_request(logic, tid, amt_br))

    tx_requests.extend(
        build_transfer_txs_to_intents_deposit(
            network_id=network_id,
            token_id=tid,
            deposit_address=intents_deposit_address,
            amount_token_smallest=str(amount_token_smallest),
            frontend_target_chain=frontend_target_chain,
        )
    )

    return {"nonce": nonce, "deadline": deadline, "tx_requests": tx_requests}


def message_to_sign_for_business(business: Dict[str, Any]) -> str:
    """Same string the wallet signs (`JSON.stringify` / compact JSON)."""
    return json.dumps(business or {}, separators=(",", ":"), ensure_ascii=False)


def attach_deposit_yocto_for_relayer(has_mca_storage_register_tx: bool) -> str:
    """Match TS NDeposit(storage_read * mult)."""
    mult = Decimal("2") if has_mca_storage_register_tx else Decimal("1")
    return _ndeposit_yocto(str(_TOKEN_STORAGE_DEPOSIT_READ_NEAR * mult))


def build_near_exec_wallet_preview_for_business(
    *,
    mca_account_id: str,
    exec_signer_near: str,
    business: Dict[str, Any],
    token_id: str,
    intents_deposit_address: str,
    amount_token_smallest: str,
    amount_burrow_inner: str,
) -> Dict[str, Any]:
    """
    Wallet-selector style single-tx preview: MCA `exec` with pre-built `business`.

    Aligns with `withdrawFromMca` when `chain == "near"`: `call_on_near` → `exec`
    with `signature: ""` (NEAR wallet signs the transaction).

    `business` must be the full map from `assemble_mca_withdraw_to_intents_business`
    (includes register + withdraw + ft_transfer to 1Click deposit).
    """
    mca = str(mca_account_id or "").strip()
    signer = str(exec_signer_near or "").strip()
    if not mca or not signer:
        raise ValueError("mcaAccountId and exec signer NEAR account are required")

    signer_wallet = {"Near": signer}
    exec_args = {
        "business": business,
        "signer_wallet": signer_wallet,
        "signature": "",
    }
    gas_outer = _tgas_yocto(300)
    return {
        "chainId": "near",
        "signerId": signer,
        "receiverId": mca,
        "standard": "mca-exec",
        "kind": "mca_withdraw_to_intents_deposit",
        "tokenId": str(token_id or "").strip(),
        "mcaAccountId": mca,
        "intentsDepositAddress": str(intents_deposit_address or "").strip(),
        "amountIn": str(amount_token_smallest or "").strip(),
        "amountBurrowInner": str(amount_burrow_inner or "").strip(),
        "actions": [
            {
                "type": "FunctionCall",
                "params": {
                    "methodName": "exec",
                    "args": exec_args,
                    "gas": gas_outer,
                    "deposit": "1",
                },
            }
        ],
    }
