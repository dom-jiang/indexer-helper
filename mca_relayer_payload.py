# -*- coding:utf-8 -*-
"""
Normalize MCA Relayer submit payloads:
  - Legacy: wallet + request[] (each item JSON string or object).
  - Structured: wallet + business + signature (+ optional attachDeposit),
    or signedPackages: [{ business, signature, attachDeposit? }, ...].

Output matches executeBusinessTransaction rows:
  {"signer_wallet": <object>, "business": {...}, "signature": "...", "attach_deposit": "..."}
serialized with compact JSON (same separators as frontend JSON.stringify for signing).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple


def _row_compact(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def normalize_mca_relayer_wallet(wallet: Any) -> Tuple[str, Dict[str, Any]]:
    """
    Returns (wallet_json_for_db_column, wallet_object_for_signer_wallet_field).
    """
    if isinstance(wallet, dict):
        obj = wallet
        s = _row_compact(obj)
        return s, obj
    if isinstance(wallet, str):
        w = wallet.strip()
        if not w:
            raise ValueError("wallet is empty")
        try:
            obj = json.loads(w)
        except json.JSONDecodeError as e:
            raise ValueError(f"wallet must be a JSON object string: {e}") from e
        if not isinstance(obj, dict):
            raise ValueError("wallet JSON must decode to an object")
        s = _row_compact(obj)
        return s, obj
    raise ValueError("wallet must be a JSON object or JSON object string")


def build_relayer_request_row(
    *,
    signer_wallet: Dict[str, Any],
    business: Dict[str, Any],
    signature: str,
    attach_deposit: str = "0",
) -> str:
    sig = str(signature or "").strip()
    if not sig:
        raise ValueError("signature is required")
    row = {
        "signer_wallet": signer_wallet,
        "business": business,
        "signature": sig,
        "attach_deposit": str(attach_deposit or "0"),
    }
    return _row_compact(row)


def _normalize_legacy_request_list(raw: Any) -> List[str]:
    if not isinstance(raw, list) or not raw:
        return []
    out: List[str] = []
    for item in raw:
        if isinstance(item, str):
            s = item.strip()
            if s:
                out.append(s)
        elif isinstance(item, dict):
            out.append(_row_compact(item))
    return out


def resolve_mca_relayer_request_list(payload: Dict[str, Any]) -> List[str]:
    """
    Build the request string list for add_multichain_lending_requests.

    `payload` uses mcaRelayer-shaped keys: wallet, request?, signedPackages?,
    business?, signature?, attachDeposit? / attach_deposit?
    """
    legacy = _normalize_legacy_request_list(payload.get("request"))
    if legacy:
        return legacy

    _, signer_wallet = normalize_mca_relayer_wallet(payload.get("wallet"))

    packages = payload.get("signedPackages")
    if packages is None and payload.get("business") is not None:
        packages = [
            {
                "business": payload.get("business"),
                "signature": payload.get("signature"),
                "attachDeposit": payload.get("attachDeposit")
                if payload.get("attachDeposit") is not None
                else payload.get("attach_deposit"),
            }
        ]

    if not isinstance(packages, list) or not packages:
        raise ValueError(
            "Provide request (non-empty array), or signedPackages, or business + signature"
        )

    out: List[str] = []
    for i, pkg in enumerate(packages):
        if not isinstance(pkg, dict):
            raise ValueError(f"signedPackages[{i}] must be an object")
        biz = pkg.get("business")
        if not isinstance(biz, dict):
            raise ValueError(f"signedPackages[{i}].business must be an object")
        sig = pkg.get("signature") if pkg.get("signature") is not None else pkg.get("signedMessage")
        if sig is None or not str(sig).strip():
            raise ValueError(f"signedPackages[{i}].signature is required")
        att = pkg.get("attachDeposit")
        if att is None:
            att = pkg.get("attach_deposit", "0")
        out.append(
            build_relayer_request_row(
                signer_wallet=signer_wallet,
                business=biz,
                signature=str(sig).strip(),
                attach_deposit=str(att if att is not None else "0"),
            )
        )
    return out


def canonicalize_mca_relayer_block(mr: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expand structured fields into legacy storage shape (wallet string + request[] strings).
    Drops signedPackages / top-level business+signature from returned dict to avoid ambiguity.
    """
    if not isinstance(mr, dict) or not mr:
        raise ValueError("mcaRelayer must be a non-empty object")
    out = dict(mr)
    ws, _ = normalize_mca_relayer_wallet(out.get("wallet"))
    out["wallet"] = ws
    reqs = resolve_mca_relayer_request_list(out)
    out["request"] = reqs
    for k in ("signedPackages", "business", "signature", "signedMessage", "attachDeposit", "attach_deposit"):
        out.pop(k, None)
    return out


def canonicalize_multichain_lending_requests_body(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    For POST /multichain_lending_requests: mca_id, wallet, request/page_display_data
    plus optional structured signing fields on the same object.
    """
    if not isinstance(body, dict):
        raise ValueError("body must be an object")
    mca_id = body.get("mca_id") or body.get("mcaAccountId")
    if not mca_id:
        raise ValueError("mca_id is required")
    payload = dict(body)
    payload["mcaAccountId"] = mca_id
    ws, _ = normalize_mca_relayer_wallet(payload.get("wallet"))
    payload["wallet"] = ws
    reqs = resolve_mca_relayer_request_list(payload)
    page = str(
        payload.get("page_display_data") or payload.get("pageDisplayData") or ""
    )
    return {
        "mca_id": mca_id,
        "wallet": ws,
        "request": reqs,
        "page_display_data": page,
    }


def extract_intents_deposit_address_from_business(business: Any) -> str:
    """
    Best-effort: last `ft_transfer` receiver_id inside `business.tx_requests`
    (MCA withdraw → Near Intents deposit).
    """
    if not isinstance(business, dict):
        return ""
    txs = business.get("tx_requests")
    if not isinstance(txs, list):
        return ""
    last_recv = ""
    for tr in txs:
        if not isinstance(tr, dict):
            continue
        fc = tr.get("FunctionCall")
        if not isinstance(fc, dict):
            continue
        for call in fc.get("function_calls") or []:
            if not isinstance(call, dict):
                continue
            if call.get("method_name") != "ft_transfer":
                continue
            args_raw = call.get("args")
            try:
                if isinstance(args_raw, str):
                    ad = json.loads(args_raw)
                elif isinstance(args_raw, dict):
                    ad = args_raw
                else:
                    continue
                rid = str(ad.get("receiver_id") or "").strip()
                if rid:
                    last_recv = rid
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
    return last_recv


def extract_intents_deposit_from_relayer_payload(payload: Dict[str, Any]) -> str:
    """Structured (`business` / `signedPackages`) or legacy `request[]` rows."""
    if not isinstance(payload, dict):
        return ""
    biz = payload.get("business")
    if isinstance(biz, dict):
        dep = extract_intents_deposit_address_from_business(biz)
        if dep:
            return dep
    pkgs = payload.get("signedPackages")
    if isinstance(pkgs, list):
        for p in pkgs:
            if isinstance(p, dict) and isinstance(p.get("business"), dict):
                dep = extract_intents_deposit_address_from_business(p["business"])
                if dep:
                    return dep
    legacy = payload.get("request")
    if isinstance(legacy, list):
        for item in legacy:
            try:
                if isinstance(item, str):
                    obj = json.loads(item)
                elif isinstance(item, dict):
                    obj = item
                else:
                    continue
                b = obj.get("business") if isinstance(obj, dict) else None
                if isinstance(b, dict):
                    dep = extract_intents_deposit_address_from_business(b)
                    if dep:
                        return dep
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
    return ""


def iter_business_objects_from_relayer_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Yield every `business` map present on an mcaRelayer-shaped payload."""
    out: List[Dict[str, Any]] = []
    if not isinstance(payload, dict):
        return out
    biz = payload.get("business")
    if isinstance(biz, dict):
        out.append(biz)
    pkgs = payload.get("signedPackages")
    if isinstance(pkgs, list):
        for p in pkgs:
            if isinstance(p, dict) and isinstance(p.get("business"), dict):
                out.append(p["business"])
    legacy = payload.get("request")
    if isinstance(legacy, list):
        for item in legacy:
            try:
                if isinstance(item, str):
                    obj = json.loads(item)
                elif isinstance(item, dict):
                    obj = item
                else:
                    continue
                b = obj.get("business") if isinstance(obj, dict) else None
                if isinstance(b, dict):
                    out.append(b)
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
    return out


def _parse_function_call_args(args_raw: Any) -> Optional[Dict[str, Any]]:
    try:
        if isinstance(args_raw, str):
            ad = json.loads(args_raw)
        elif isinstance(args_raw, dict):
            ad = args_raw
        else:
            return None
        return ad if isinstance(ad, dict) else None
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def extract_withdraw_token_id_from_business(business: Any) -> str:
    """Burrow ``execute`` → ``Withdraw.token_id`` from signed relayer business."""
    if not isinstance(business, dict):
        return ""
    txs = business.get("tx_requests")
    if not isinstance(txs, list):
        return ""
    for tr in txs:
        if not isinstance(tr, dict):
            continue
        fc = tr.get("FunctionCall")
        if not isinstance(fc, dict):
            continue
        for call in fc.get("function_calls") or []:
            if not isinstance(call, dict) or call.get("method_name") != "execute":
                continue
            ad = _parse_function_call_args(call.get("args"))
            if not ad:
                continue
            for action in ad.get("actions") or []:
                if not isinstance(action, dict):
                    continue
                wd = action.get("Withdraw")
                if isinstance(wd, dict):
                    tid = str(wd.get("token_id") or "").strip()
                    if tid:
                        return tid
    return ""


def extract_last_ft_transfer_fields_from_business(
    business: Any,
) -> Tuple[str, str]:
    """Last ``ft_transfer`` in business → (receiver_id, amount) smallest units."""
    if not isinstance(business, dict):
        return "", ""
    txs = business.get("tx_requests")
    if not isinstance(txs, list):
        return "", ""
    last_recv = ""
    last_amt = ""
    for tr in txs:
        if not isinstance(tr, dict):
            continue
        fc = tr.get("FunctionCall")
        if not isinstance(fc, dict):
            continue
        for call in fc.get("function_calls") or []:
            if not isinstance(call, dict) or call.get("method_name") != "ft_transfer":
                continue
            ad = _parse_function_call_args(call.get("args"))
            if not ad:
                continue
            rid = str(ad.get("receiver_id") or "").strip()
            amt = str(ad.get("amount") or "").strip()
            if rid:
                last_recv = rid
            if amt:
                last_amt = amt
    return last_recv, last_amt


def looks_like_1click_deposit_address(addr: str) -> bool:
    a = str(addr or "").strip()
    return a.lower().startswith("0x") and len(a) >= 42


def parse_swap_history_hints_from_relayer_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Best-effort fields for swap_transactions when the swap POST only includes mcaRelayer.
    Keys: from_token, amount_in, deposit_address, recipient_hint (may be 1Click or NEAR).
    """
    hints: Dict[str, str] = {
        "from_token": "",
        "amount_in": "",
        "deposit_address": "",
        "recipient_hint": "",
    }
    for biz in iter_business_objects_from_relayer_payload(payload):
        if not hints["from_token"]:
            hints["from_token"] = extract_withdraw_token_id_from_business(biz)
        recv, amt = extract_last_ft_transfer_fields_from_business(biz)
        if recv:
            hints["recipient_hint"] = recv
        if amt:
            hints["amount_in"] = amt
        dep = extract_intents_deposit_address_from_business(biz)
        if dep:
            hints["deposit_address"] = dep
    return hints


def summarize_multichain_lending_batch(rows: Optional[List[Any]]) -> Dict[str, Any]:
    """
    Interpret `multichain_lending_requests` (+ history union) rows for API / worker.

    Returns keys: pending, complete, success, tx_hashes, error
    """
    empty_hashes: List[str] = []

    if not rows:
        return {
            "pending": False,
            "complete": False,
            "success": False,
            "tx_hashes": empty_hashes,
            "error": "no multichain_lending rows yet",
        }

    try:
        complete = all(int(r.get("batch_status") or 0) == 2 for r in rows)
    except Exception:
        complete = False

    if not complete:
        return {
            "pending": True,
            "complete": False,
            "success": False,
            "tx_hashes": empty_hashes,
            "error": "",
        }

    errors: List[str] = []
    tx_hashes: List[str] = []
    for row in rows:
        rr = row.get("request_result")
        if rr is None:
            continue
        if isinstance(rr, str):
            try:
                obj = json.loads(rr)
            except Exception:
                obj = {}
        elif isinstance(rr, dict):
            obj = rr
        else:
            obj = {}
        if isinstance(obj, dict):
            if obj.get("other_err_msg") or obj.get("tx_err_msg"):
                errors.append(
                    str(obj.get("other_err_msg") or obj.get("tx_err_msg") or "")[:512]
                )
            if obj.get("tx_hash"):
                tx_hashes.append(str(obj["tx_hash"]))
        else:
            errors.append(str(obj)[:512])

    if errors:
        return {
            "pending": False,
            "complete": True,
            "success": False,
            "tx_hashes": tx_hashes,
            "error": "; ".join(errors)[:2000],
        }
    return {
        "pending": False,
        "complete": True,
        "success": True,
        "tx_hashes": tx_hashes,
        "error": "",
    }
