"""
MCA withdraw from Burrow + NEP-141 transfer to Near Intents 1Click deposit address.

Mirrors frontend `withdrawFromMca` (`src/services/lending/actions/withdraw.ts`) when:
  - Burrow oracle path disabled / no collateral decrease (single withdraw leg),
  - `tansfer_txs_query`-style transfer toward ``depositAddress``.

  Multichain relayer batches should follow App ordering: optional **small** ``simple_withdraw``
  prepay to ``MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID``, then Logic ``execute`` with ``Withdraw``
  (puts supplied assets onto the MCA for ``ft_transfer``). A **full-amount**
  ``simple_withdraw`` to the relayer **without** ``Withdraw`` skips moving funds onto the MCA,
  so the later MCA ``ft_transfer`` fails (insufficient token balance).

Used for NEAR Lending -> arbitrary 1Click destination: one signature + multichain relayer.

"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from config import Cfg
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


def nep141_ft_transfer_amount_minus_one(amount_token_smallest: str) -> str:
    """
    Match Lending ``Action.tsx`` ``getWithdrawData``: ``new Big(amountToken).minus(1)``
    used for intents ``ft_transfer`` only. Burrow ``Withdraw.max_amount`` / inner amount stays
    the full withdrawn size (passed separately as ``amount_burrow_inner``).
    """
    s = str(amount_token_smallest or "").strip()
    if not s:
        raise ValueError("amount_token_smallest is required")
    try:
        n = int(s)
    except ValueError as e:
        raise ValueError(
            f"nep141 amount must be base-10 integer string: {amount_token_smallest!r}"
        ) from e
    if n < 0:
        raise ValueError(f"nep141 amount cannot be negative: {amount_token_smallest!r}")
    return str(max(0, n - 1))


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


def build_execute_withdraw_tx_request(
    logic_contract_id: str,
    token_id: str,
    max_amount_burrow_inner: str,
    need_decrease_collateral: bool = False,
    decrease_collateral_amount_burrow: Optional[str] = None,
) -> Dict[str, Any]:
    """Burrow Logic ``execute({ actions: [Withdraw { token_id, max_amount }] })`` (see ``withdraw.ts``)."""
    logic = str(logic_contract_id or "").strip()
    tid = str(token_id or "").strip()
    amt = str(max_amount_burrow_inner or "").strip()
    if not logic or not tid or not amt:
        raise ValueError("Burrow_execute_withdraw requires logic contract, token_id, max_amount")

    actions: List[Dict[str, Any]] = []
    method_name = "execute"
    if bool(need_decrease_collateral):
        dec_amt = str(decrease_collateral_amount_burrow or "").strip()
        if not dec_amt:
            raise ValueError(
                "mca.decreaseCollateralAmountBurrow is required when mca.needDecreaseCollateral is true"
            )
        actions.append({"DecreaseCollateral": {"token_id": tid, "amount": dec_amt}})
        method_name = "execute_with_pyth"
    actions.append({"Withdraw": {"token_id": tid, "max_amount": amt}})

    wd_fn = {
        "method_name": method_name,
        "args": _serialize_args({"actions": actions}),
        "gas": _tgas_yocto(120),
        "deposit": "1",
    }
    return {
        "FunctionCall": {
            "receiver_id": logic,
            "function_calls": [wd_fn],
        }
    }


def build_withdraw_simple_withdraw_tx_request(
    logic_contract_id: str,
    token_id: str,
    amount_burrow_inner: str,
    withdraw_recipient_id: str,
) -> Dict[str, Any]:
    """Burrow Logic ``simple_withdraw`` (staging relayer **prepay** leg only — tiny amount).

    ``recipient_id`` must be the multichain relayer NEAR account
    (``Cfg.MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID`` / ``mca.relayerNearRecipient``), not the MCA.

    Full withdraw liquidity is routed through ``execute(Withdraw)``, not via a large
    ``simple_withdraw`` to the relayer.
    """
    logic = str(logic_contract_id or "").strip()
    rid = str(withdraw_recipient_id or "").strip()
    sw_fn = {
        "method_name": "simple_withdraw",
        "args": _serialize_args(
            {
                "token_id": str(token_id),
                "amount_with_inner_decimal": str(amount_burrow_inner),
                "recipient_id": rid,
            }
        ),
        # Match production multichain relayer batches (frontend / Relayer expectations).
        "gas": _tgas_yocto(100),
        "deposit": "1",
    }
    return {
        "FunctionCall": {
            "receiver_id": logic,
            "function_calls": [sw_fn],
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
    simple_withdraw_recipient_for_relayer: Optional[str] = None,
    relayer_prepay_simple_withdraw_inner: Optional[str] = None,
    need_decrease_collateral: bool = False,
    decrease_collateral_amount_burrow: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Returns the `businessMap` equivalent (nonce, deadline, tx_requests).

    When `sign_chain_is_near`, the batch avoids relayer-specific prepays: register + Withdraw + transfers.

    Args:
      `simple_withdraw_recipient_for_relayer`:
        Required when sending a prepay ``simple_withdraw`` (multichain relayer path &&
        nonempty ``relayer_prepay_simple_withdraw_inner`` / ``MCA_RELAYER_SIMPLE_WITHDRAW_FEE_INNER``).
        NEAR account id for ``recipient_id`` (e.g. ``am_relayer.stg.ref-dev-team.near``).
      `relayer_prepay_simple_withdraw_inner`:
        Optional tiny Burrow-inner string for Logic ``simple_withdraw`` → relayer **before**
        ``execute(Withdraw)``. Prefer matching ``simpleWithdrawData.amountBurrow`` from the App.
      `amount_burrow_inner`: **Required.** Burrow *internal* decimal string matching Lending
      ``Withdraw.max_amount`` (used for the ``execute`` leg), **not** NEP-141 ``amountIn``.
      `amount_token_smallest`: Raw NEP-141 smallest-unit string from the quote / ``amountToken``
      before the Lending UI adjustment. The intents ``ft_transfer`` leg uses ``max(0, amount - 1)``
      smallest units (matches ``Action.tsx`` ``getWithdrawData``).
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

    amt_sw_inner = str(amount_burrow_inner or "").strip()
    if not amt_sw_inner:
        raise ValueError(
            "Burrow withdraw requires amount_burrow_inner (pass mca.amountBurrow): "
            "Burrow internal decimal units, same scale as Lending Withdraw.max_amount — "
            "never substitute NEP-141 amountIn (e.g. \"199999\")."
        )

    cfg_fee = getattr(Cfg, "MCA_RELAYER_SIMPLE_WITHDRAW_FEE_INNER", None)
    fee_raw = str(relayer_prepay_simple_withdraw_inner or "").strip() or (
        str(cfg_fee).strip() if cfg_fee is not None and str(cfg_fee).strip() else ""
    )

    if not sign_chain_is_near and fee_raw:
        sw_recv = str(simple_withdraw_recipient_for_relayer or "").strip()
        if not sw_recv:
            raise ValueError(
                "MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID is not configured and mca.relayerNearRecipient "
                "was not provided — prepay simple_withdraw requires the multichain relayer NEAR account "
                'as recipient_id. Set MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID in db_info or pass relayerNearRecipient on quote.'
            )
        tx_requests.append(
            build_withdraw_simple_withdraw_tx_request(logic, tid, fee_raw, sw_recv)
        )
    elif not sign_chain_is_near and not (simple_withdraw_tx or []):
        logger.warning(
            "mca_withdraw relayer batch: no prepay `simple_withdraw` configured "
            "(set mca.relayerPrepayBurrowInner quote field or "
            "Cfg.MCA_RELAYER_SIMPLE_WITHDRAW_FEE_INNER / db_info — match App "
            "simpleWithdrawData.amountBurrow). Some staging relayers require this step."
        )

    tx_requests.append(
        build_execute_withdraw_tx_request(
            logic,
            tid,
            amt_sw_inner,
            need_decrease_collateral=need_decrease_collateral,
            decrease_collateral_amount_burrow=decrease_collateral_amount_burrow,
        )
    )

    amt_ft = nep141_ft_transfer_amount_minus_one(str(amount_token_smallest))
    tx_requests.extend(
        build_transfer_txs_to_intents_deposit(
            network_id=network_id,
            token_id=tid,
            deposit_address=intents_deposit_address,
            amount_token_smallest=str(amt_ft),
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
        # Same object as inside exec args — matches withdraw.ts `businessMap`, list UX steps via tx_requests.
        "business": business,
        # Drop-in for adapters that wrap src/services/chains/near.ts `call_on_near`.
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
