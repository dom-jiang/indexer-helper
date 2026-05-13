"""
Server-side assembly of Burrow / Intents `customRecipientMsg` defaults for MCA deposit.

Aligns with frontend `format_wallet` + `storage_balance_of` logic in
`src/hooks/useChainsSwapQuote.ts` and `src/services/lending/actions/commonAction.ts`.

Callers: `unified_quote` / `unified_swap` when `m.flow == "deposit"` and
`customRecipientMsg` is omitted.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger

from config import Cfg

# Internal keys only for building CRM; never sent to 1Click (merge is whitelisted anyway).
_INTERNAL_MCA_KEYS = frozenset({"signer", "depositSigner", "recipientMsgSignatures", "depositSignerProof"})


def _burrow_logic_contract(network_id: str) -> str:
    oid = str(network_id or "").upper()
    logic = getattr(Cfg, "MCA_BURROW_LOGIC_CONTRACT", None)
    if isinstance(logic, str) and logic.strip():
        return logic.strip()
    return str(Cfg.NETWORK.get(oid, {}).get("BURROW_CONTRACT") or "").strip()


def _near_rpc_urls(network_id: str) -> List[str]:
    oid = str(network_id or "").upper()
    raw = Cfg.NETWORK.get(oid, {}).get("NEAR_RPC_URL") or []
    if isinstance(raw, str):
        return [raw]
    return [str(u) for u in raw if u]


def near_view_call(network_id: str, contract_id: str, method_name: str, args: Dict[str, Any]) -> Any:
    """Return JSON-decoded result from a NEAR view call, or raises on hard failure."""
    if not contract_id:
        raise ValueError("empty Burrow logic contract id")

    payload_bin = json.dumps(args, separators=(",", ":")).encode("utf-8")
    body = {
        "jsonrpc": "2.0",
        "id": "mca-burrow-auto",
        "method": "query",
        "params": {
            "request_type": "call_function",
            "finality": "final",
            "account_id": contract_id,
            "method_name": method_name,
            "args_base64": base64.b64encode(payload_bin).decode("ascii"),
        },
    }
    last_err = None
    for url in _near_rpc_urls(network_id):
        try:
            r = requests.post(url, json=body, timeout=8)
            r.raise_for_status()
            data = r.json()
            if "error" in data:
                raise RuntimeError(str(data["error"]))
            result = data.get("result") or {}
            blobs = result.get("result")
            if blobs is None:
                raise RuntimeError("empty NEAR view result")
            if isinstance(blobs, list) and not blobs:
                return None  # convention: "", null contract returns sometimes
            s = "".join(chr(int(b)) for b in blobs)
            return json.loads(s)
        except Exception as e:
            last_err = e
            logger.warning(f"near_view_call fallback node err {url}: {e}")
            continue
    raise RuntimeError(f"NEAR RPC failed for {method_name}: {last_err}")


def _registered_on_burrow_logic(network_id: str, logic_contract: str, mca_account_id: str) -> Optional[bool]:
    """
    Returns True/False when view succeeds; None if ambiguous (logged).
    """
    try:
        bal = near_view_call(network_id, logic_contract, "storage_balance_of", {"account_id": mca_account_id})
        return bool(bal)
    except Exception as e:
        logger.warning(f"storage_balance_of failed for {mca_account_id}: {e}")
        return None


def format_wallet_wallet_object(chain_raw: str, identity_key_raw: str) -> Dict[str, str]:
    """
    Mirror src/utils/chainsUtil.ts format_wallet keys (single-object values).
    """
    ch = str(chain_raw or "").strip().lower()
    key = identity_key_raw or ""

    if ch == "evm":
        k = key[2:] if key.lower().startswith("0x") else key
        return {"EVM": k}
    if ch == "solana":
        return {"Solana": key}
    if ch == "btc":
        return {"Bitcoin": key}
    if ch == "zcash":
        return {"Zcash": key}
    if ch == "webauthn":
        return {"WebAuthn": key}
    if ch == "near":
        return {"Near": key}
    if ch == "aptos":
        return {"Aptos": key}
    if ch == "tron":
        return {"Tron": key}
    if ch == "sui":
        return {"Sui": key}

    raise ValueError(f"Unsupported signer.chain for MCA auto CRM: {chain_raw!r}")


def _resolve_burrow_action(
    *,
    explicit: str,
    use_collateral: bool,
    registered_opt: Optional[bool],
) -> str:
    explicit = str(explicit or "").strip()
    if explicit == "BurrowRepay":
        return "BurrowRepay"

    allowed_explicit = {
        "BurrowSupply",
        "BurrowCollateral",
        "BurrowRegisterAndSupply",
        "BurrowRegisterAndCollateral",
    }
    if explicit in allowed_explicit:
        return explicit

    if explicit:
        raise ValueError(f"Unknown burrowAction: {explicit!r}")

    if registered_opt is None:
        logger.warning(
            "MCA CRM auto-mode: NEAR storage_balance_of failed; assuming not registered "
            "(BurrowRegister* path). Override with explicit `burrowAction` if wrong."
        )
        registered_opt = False

    if registered_opt:
        return "BurrowCollateral" if use_collateral else "BurrowSupply"
    return "BurrowRegisterAndCollateral" if use_collateral else "BurrowRegisterAndSupply"


def enrich_mca_deposit_block(mca: Dict[str, Any], network_id: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    If `flow` is deposit and `customRecipientMsg` is absent, fills:
      - default `appFees`, `referral` (Cfg)
      - `customRecipientMsg` from signer + Burrow logic (and optional proofs)

    Returns (new_mca_dict, error_string).
    """
    if not isinstance(mca, dict) or not mca:
        return mca, None

    flow = str(mca.get("flow") or mca.get("mcaFlow") or "").strip().lower()
    if flow != "deposit":
        return mca, None

    out = dict(mca)

    # Defaults for 1Click (apply even when customRecipientMsg is pre-filled)
    recipient_cfg = getattr(Cfg, "MCA_INTENTS_APP_FEES_RECIPIENT", "") or ""
    fee_val = getattr(Cfg, "MCA_INTENTS_APP_FEES", None)
    if recipient_cfg and fee_val is not None:
        fees = out.get("appFees") or out.get("app_fees")
        if not (isinstance(fees, list) and len(fees) > 0):
            try:
                fee_int = int(fee_val)
            except (TypeError, ValueError):
                fee_int = 5
            out["appFees"] = [{"recipient": recipient_cfg, "fee": fee_int}]

    referral_default = getattr(Cfg, "MCA_DEFAULT_REFERRAL", "") or ""
    if referral_default and not (out.get("referral")):
        out["referral"] = referral_default

    cr_existing = out.get("customRecipientMsg") or out.get("custom_recipient_msg")
    if isinstance(cr_existing, str) and cr_existing.strip():
        return out, None

    signer_obj = out.get("signer") or out.get("depositSigner")
    if not isinstance(signer_obj, dict):
        return (
            out,
            "For mca.flow=deposit without customRecipientMsg, pass mca.signer: "
            '{"chain":"evm"|"near"|...,"identityKey":"<address or account>"}',
        )

    chain = signer_obj.get("chain") or signer_obj.get("signerChain") or signer_obj.get("signer_chain")
    ikey = signer_obj.get("identityKey") or signer_obj.get("identity_key")
    if not chain or not ikey:
        return out, "mca.signer must include `chain` and `identityKey`"

    try:
        wobj = format_wallet_wallet_object(chain, str(ikey))
    except ValueError as e:
        return out, str(e)

    explicit_action = str(out.get("burrowAction") or out.get("burrow_action") or "").strip()
    explicit_upper = explicit_action.upper() if explicit_action else ""

    sigs_early = (
        out.get("recipientMsgSignatures")
        or out.get("depositSignerProofSignatures")
        or out.get("recipient_msg_signatures")
    )
    sig_list_early: List[str] = (
        [str(s) for s in sigs_early if s is not None and str(s).strip()]
        if isinstance(sigs_early, list)
        else []
    )

    # Frontend `SupplyCreate` / `Create`: requires `s` against JSON.stringify([w])
    if explicit_upper in ("SUPPLYCREATE", "CREATE"):
        if not sig_list_early:
            return (
                out,
                "burrowAction SupplyCreate/Create needs mca.recipientMsgSignatures: "
                "sign JSON.stringify([w]) with the wallet on mca.signer.chain, then pass each signature string.",
            )
        msg_obj_early = {"w": [wobj], "b": {"r": "BurrowRegisterAndSupply"}, "s": sig_list_early}
        out["customRecipientMsg"] = json.dumps(msg_obj_early, separators=(",", ":"), ensure_ascii=False)
        out["depositCrmAutoFilled"] = True
        for k in _INTERNAL_MCA_KEYS:
            out.pop(k, None)
        return out, None

    logic = _burrow_logic_contract(network_id)
    mca_acc = out.get("mcaAccountId") or out.get("mca_id")
    registered: Optional[bool] = None

    if explicit_upper != "BURROWREPAY" and mca_acc and logic:
        registered = _registered_on_burrow_logic(network_id, logic, str(mca_acc))

    use_coll = bool(out.get("useAsCollateral"))
    try:
        r_action = _resolve_burrow_action(
            explicit=explicit_action,
            use_collateral=use_coll,
            registered_opt=registered,
        )
    except ValueError as e:
        return out, str(e)

    msg_obj: Dict[str, Any] = {"w": [wobj], "b": {"r": r_action}}

    sigs_raw = (
        out.get("recipientMsgSignatures")
        or out.get("depositSignerProofSignatures")
        or out.get("recipient_msg_signatures")
    )
    sig_list: List[str] = (
        [str(s) for s in sigs_raw if s is not None and str(s).strip()] if isinstance(sigs_raw, list) else []
    )
    if sig_list:
        msg_obj["s"] = sig_list

    proof_required = bool(out.get("depositSignerProof"))
    if proof_required and "s" not in msg_obj:
        return (
            out,
            "depositSignerProof=true requires mca.recipientMsgSignatures "
            "(sign JSON.stringify([w]) on signer chain).",
        )

    out["customRecipientMsg"] = json.dumps(msg_obj, separators=(",", ":"), ensure_ascii=False)
    out["depositCrmAutoFilled"] = True

    for k in _INTERNAL_MCA_KEYS:
        out.pop(k, None)

    return out, None