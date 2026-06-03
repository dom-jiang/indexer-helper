"""
Assemble MCA `exec` business + wallet-signed tx shape for NEAR Lending withdraw → NEAR wallet.

Mirrors src/services/lending/actions/withdraw.ts (withdrawFromMca, chain == "near", targetChain "near")
and commonTx.tansfer_txs_query / wnear_withdraw_tx_query structures (non-Pyth path).
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from loguru import logger

from mca_burrow_auto import near_view_call, _burrow_logic_contract
from mca_withdraw_cross_intents import build_mca_register_token_tx_requests

# Align with src/services/constantConfig TOKEN_STORAGE_DEPOSIT_READ = 0.00125 NEAR for storage deposits
_TOKEN_STORAGE_DEPOSIT_READ = "0.00125"


def _tgas_yocto(gas_tgas: int) -> str:
    return str(int(Decimal(gas_tgas) * Decimal(10**12)))


def _ndeposit_yocto(near_human: str) -> str:
    """NDeposit(): multiply human NEAR by 1e24 (yocto)."""
    return str(int(Decimal(str(near_human)) * Decimal(10**24)))


def _wrap_near_contract_for_network(network_id: str) -> str:
    oid = str(network_id or "").upper()
    if "TEST" in oid:
        return "wrap.testnet"
    return "wrap.near"


def _serialize_obj(params: Dict[str, Any]) -> str:
    return json.dumps(params or {}, separators=(",", ":"), ensure_ascii=False)


def _near_view_optional(
    network_id: str, contract_id: str, method: str, args: Dict[str, Any]
) -> Any:
    try:
        return near_view_call(network_id, contract_id, method, args)
    except Exception as e:
        logger.warning(f"_near_view_optional {contract_id}.{method}: {e}")
        return None


def build_near_mca_withdraw_exec_tx_payload(
    *,
    network_id: str,
    mca_account_id: str,
    token_id: str,
    amount_token_smallest: str,
    amount_burrow: str,
    recipient_near: str,
    exec_signer_near: str,
    need_decrease_collateral: bool = False,
    decrease_collateral_amount_burrow: Optional[str] = None,
    withdraw_all: bool = False,
    wrap_near_contract_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Returns a wallet-selector–style single-tx payload: sign from `exec_signer_near`, call MCA `exec`.

    `amount_token_smallest`: NEP-141 smallest units string (matches Trade `amountToken` / amountIn).
    `amount_burrow`: Burrow `Withdraw.max_amount` string (often differs — pass from client when possible).
    """
    mca = str(mca_account_id or "").strip()
    tid = str(token_id or "").strip()
    rec = str(recipient_near or "").strip()
    signer = str(exec_signer_near or "").strip()
    if not mca or not tid or not rec or not signer:
        raise ValueError("mcaAccountId, tokenId, recipient (NEAR), execSignerNear are required")

    wrap_near = (wrap_near_contract_id or _wrap_near_contract_for_network(network_id)).strip()

    nonce = _near_view_optional(network_id, mca, "get_nonce", {})
    if nonce is None:
        raise RuntimeError(f"NEAR view get_nonce failed for MCA {mca}")

    deadline = str(int(time.time() * 1000) + 10 * 60 * 1000)

    logic = _burrow_logic_contract(network_id)
    if not logic:
        raise RuntimeError("Burrow logic contract id not configured (MCA_BURROW_LOGIC / BURROW_CONTRACT)")

    amt_tok = str(amount_token_smallest).strip()
    amt_br = str(amount_burrow or "").strip()
    if not withdraw_all and not amt_br:
        raise ValueError(
            "amountBurrow is required: Burrow Withdraw.max_amount uses internal decimal units, "
            "not NEP-141 smallest-unit amountIn."
        )
    withdraw_action = {"Withdraw": {"token_id": tid}}
    if not withdraw_all:
        withdraw_action["Withdraw"]["max_amount"] = amt_br
    actions: List[Dict[str, Any]] = []
    method_name = "execute"
    if bool(need_decrease_collateral):
        dec_action = {"DecreaseCollateral": {"token_id": tid}}
        if not withdraw_all:
            dec_amt = str(decrease_collateral_amount_burrow or "").strip()
            if not dec_amt:
                raise ValueError(
                    "mca.decreaseCollateralAmountBurrow is required when mca.needDecreaseCollateral is true and mca.withdrawAll is false"
                )
            dec_action["DecreaseCollateral"]["amount"] = dec_amt
        actions.append(dec_action)
        method_name = "execute_with_pyth"
    actions.append(withdraw_action)

    withdraw_fn = {
        "method_name": method_name,
        "args": _serialize_obj({"actions": actions}),
        "gas": _tgas_yocto(120),
        "deposit": "1",
    }
    withdraw_tx_req = {
        "FunctionCall": {
            "receiver_id": logic,
            "function_calls": [withdraw_fn],
        }
    }

    transfer_tx_reqs = _build_transfer_tx_requests_near(
        network_id=network_id,
        token_id=tid,
        deposit_address=rec,
        amount_token=amt_tok,
        wrap_near_contract_id=wrap_near,
    )

    reg = build_mca_register_token_tx_requests(network_id, tid, mca)
    tx_requests = [*reg, withdraw_tx_req, *transfer_tx_reqs]

    business = {
        "nonce": str(nonce),
        "deadline": str(deadline),
        "tx_requests": tx_requests,
    }

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
        "kind": "mca_withdraw_to_near_wallet",
        "tokenId": tid,
        "mcaAccountId": mca,
        "recipientNear": rec,
        # Mirrors withdraw.ts `businessMap` before call_on_near — UI can render tx_requests steps.
        "business": business,
        # Same shape as src/services/chains/near.ts `call_on_near({ transactions })`.
        "transactions": [
            {
                "contractId": mca,
                "methodName": "exec",
                "args": exec_args,
                "gas": "300",
            }
        ],
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


def _build_transfer_tx_requests_near(
    *,
    network_id: str,
    token_id: str,
    deposit_address: str,
    amount_token: str,
    wrap_near_contract_id: str,
) -> List[Dict[str, Any]]:
    """Near-chain leg: wNEAR unwrap + native transfer, else NEP-141 ft_transfer (+ optional storage)."""
    tid = str(token_id or "").strip()
    dep = str(deposit_address or "").strip()
    amt = str(amount_token or "").strip()

    if tid == wrap_near_contract_id:
        wnear_fn = {
            "method_name": "near_withdraw",
            "args": _serialize_obj({"amount": str(int(Decimal(amt)))}),
            "gas": _tgas_yocto(10),
            "deposit": "1",
        }
        wnear_block = {
            "FunctionCall": {
                "receiver_id": wrap_near_contract_id,
                "function_calls": [wnear_fn],
                "interval_block": 3,
            }
        }
        xfer = {
            "Transfer": {
                "receiver_id": dep,
                "amount": amt,
            }
        }
        return [wnear_block, xfer]

    function_calls: List[Dict[str, Any]] = []
    bal = _near_view_optional(
        network_id,
        tid,
        "storage_balance_of",
        {"account_id": dep},
    )
    if not bal:
        function_calls.append(
            {
                "method_name": "storage_deposit",
                "args": _serialize_obj(
                    {"account_id": dep, "registration_only": True}
                ),
                "gas": _tgas_yocto(10),
                "deposit": _ndeposit_yocto(_TOKEN_STORAGE_DEPOSIT_READ),
            }
        )
    function_calls.append(
        {
            "method_name": "ft_transfer",
            "args": _serialize_obj(
                {"receiver_id": dep, "amount": amt, "memo": None}
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
