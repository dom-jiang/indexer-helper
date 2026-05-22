#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Resolve MCA relayer EVM signatures (r/s/v) for Hyperliquid Perps workers.

Mirrors multi-chain-lending `mcaSignature.ts`: poll lending batch, read
request / request_result, and fall back to NEAR tx SuccessValue when needed.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, List, Optional, Sequence

import requests

from config import Cfg

try:
    from db_info import MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID
except ImportError:
    MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID = ""


def _parse_json_cell(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _normalize_sig_v(v: Any) -> int:
    if isinstance(v, bool):
        raise ValueError("invalid signature v")
    if isinstance(v, int):
        return int(v)
    s = str(v).strip()
    if s.startswith("0x") or s.startswith("0X"):
        return int(s, 16)
    return int(s)


def _rsv_from_dict(obj: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return None
    if obj.get("r") and obj.get("s") and "v" in obj:
        try:
            return {
                "r": str(obj.get("r")),
                "s": str(obj.get("s")),
                "v": _normalize_sig_v(obj.get("v")),
            }
        except Exception:
            return None
    sp = obj.get("signatureParts")
    if isinstance(sp, dict) and sp.get("r") and sp.get("s") and "v" in sp:
        try:
            return {
                "r": str(sp.get("r")),
                "s": str(sp.get("s")),
                "v": _normalize_sig_v(sp.get("v")),
            }
        except Exception:
            return None
    return None


def _format_evm_signature_from_near(near_sig: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(near_sig, dict):
        return None
    big_r = near_sig.get("big_r") or {}
    s_obj = near_sig.get("s") or {}
    recovery_id = near_sig.get("recovery_id")
    affine = big_r.get("affine_point") if isinstance(big_r, dict) else None
    scalar = s_obj.get("scalar") if isinstance(s_obj, dict) else None
    if not affine or not scalar or recovery_id is None:
        return None
    try:
        rid = int(recovery_id)
    except Exception:
        return None
    pt = str(affine).strip()
    if pt.startswith("0x") or pt.startswith("0X"):
        pt = pt[2:]
    sc = str(scalar).strip()
    return {
        "r": "0x" + pt.zfill(64)[-64:],
        "s": "0x" + sc.zfill(64)[-64:],
        "v": rid + 27,
    }


def _decode_success_value(status: Any) -> Any:
    if not isinstance(status, dict):
        return None
    success_value = status.get("SuccessValue")
    if success_value is None or success_value == "":
        return None
    padding = "=" * ((4 - len(str(success_value)) % 4) % 4)
    try:
        decoded_bytes = base64.b64decode(str(success_value) + padding)
    except Exception:
        return None
    if not decoded_bytes:
        return None
    try:
        return json.loads(decoded_bytes.decode("utf-8"))
    except Exception:
        try:
            return decoded_bytes.decode("utf-8")
        except Exception:
            return None


def _near_rpc_urls(network_id: str) -> List[str]:
    urls = Cfg.NETWORK[network_id]["NEAR_RPC_URL"]
    if isinstance(urls, (list, tuple)):
        return [str(u) for u in urls if u]
    return [str(urls)]


def _mca_near_sender_ids(
    network_id: str,
    *,
    lending_rows: Optional[Sequence[Dict[str, Any]]] = None,
    mca_id: Optional[str] = None,
    signature_task: Optional[Dict[str, Any]] = None,
) -> List[str]:
    out: List[str] = []
    st = signature_task or {}

    def _add(v: Any) -> None:
        s = str(v or "").strip()
        if s and s not in out:
            out.append(s)

    _add(st.get("signerIdentityKey"))
    _add(st.get("signerAccountId"))
    if lending_rows:
        for row in lending_rows:
            _add(row.get("leased_to"))
            _add(row.get("mca_id"))
    _add(mca_id)
    _add(MULTICHAIN_RELAYER_NEAR_ACCOUNT_ID)
    net = Cfg.NETWORK.get(network_id) or {}
    _add(net.get("ZCASH_MA_CONTRACT"))
    return out


def fetch_evm_rsv_from_near_tx(
    network_id: str,
    tx_hash: str,
    sender_ids: Sequence[str],
) -> Optional[Dict[str, Any]]:
    tx_hash = str(tx_hash or "").strip()
    if not tx_hash:
        return None

    for sender in sender_ids:
        sender = str(sender or "").strip()
        if not sender:
            continue
        for rpc_url in _near_rpc_urls(network_id):
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": "dontcare",
                    "method": "EXPERIMENTAL_tx_status",
                    "params": {
                        "tx_hash": tx_hash,
                        "sender_account_id": sender,
                        "wait_until": "NONE",
                    },
                }
                resp = requests.post(rpc_url, json=payload, timeout=20)
                resp.raise_for_status()
                result = resp.json()
                if "error" in result:
                    continue
                tx_result = result.get("result") or {}
                outcomes = [tx_result.get("transaction_outcome")]
                outcomes.extend(tx_result.get("receipts_outcome") or [])
                for item in outcomes:
                    if not isinstance(item, dict):
                        continue
                    outcome = item.get("outcome") or {}
                    status = outcome.get("status") or {}
                    decoded = _decode_success_value(status)
                    rsv = _format_evm_signature_from_near(decoded)
                    if rsv:
                        return rsv
            except Exception:
                continue
    return None


def lending_batch_complete(rows: Sequence[Dict[str, Any]]) -> bool:
    if not rows:
        return False
    for row in rows:
        try:
            if int(row.get("batch_status") if row.get("batch_status") is not None else -1) != 2:
                return False
        except Exception:
            return False
    return True


def lending_batch_error(rows: Sequence[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return "multichain_lending_data empty"
    if not lending_batch_complete(rows):
        return None
    for row in rows:
        rr = _parse_json_cell(row.get("request_result"))
        if rr.get("tx_err_msg"):
            return str(rr["tx_err_msg"])
        if rr.get("other_err_msg"):
            return str(rr["other_err_msg"])
    return None


def _rsv_from_lending_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    req = _parse_json_cell(row.get("request"))
    rsv = _rsv_from_dict(req)
    if rsv:
        return rsv
    if isinstance(req.get("permitSignature"), dict):
        ps = req["permitSignature"]
        rsv = _rsv_from_dict(ps)
        if rsv:
            return rsv
        rsv = _rsv_from_dict(ps.get("signatureParts"))
        if rsv:
            return rsv

    rr = _parse_json_cell(row.get("request_result"))
    rsv = _rsv_from_dict(rr)
    if rsv:
        return rsv
    rsv = _rsv_from_dict(rr.get("signature"))
    if rsv:
        return rsv
    rsv = _rsv_from_dict(rr.get("signatureParts"))
    if rsv:
        return rsv

    tx_record = row.get("tx_record")
    if tx_record:
        tr = _parse_json_cell(tx_record) if not isinstance(tx_record, dict) else tx_record
        rsv = _rsv_from_dict(tr)
        if rsv:
            return rsv

    return None


def extract_mca_evm_rsv_from_lending_batch(
    network_id: str,
    rows: Sequence[Dict[str, Any]],
    *,
    mca_id: Optional[str] = None,
    signature_task: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    if not lending_batch_complete(rows):
        return None
    if lending_batch_error(rows):
        return None

    senders = _mca_near_sender_ids(
        network_id,
        lending_rows=rows,
        mca_id=mca_id,
        signature_task=signature_task,
    )

    for row in rows:
        rsv = _rsv_from_lending_row(row)
        if rsv:
            return rsv
        rr = _parse_json_cell(row.get("request_result"))
        tx_hash = rr.get("tx_hash")
        if tx_hash:
            rsv = fetch_evm_rsv_from_near_tx(network_id, str(tx_hash), senders)
            if rsv:
                return rsv

    return None


def resolve_mca_evm_rsv(
    network_id: str,
    *,
    lending_rows: Optional[Sequence[Dict[str, Any]]] = None,
    signature_task: Optional[Dict[str, Any]] = None,
    mca_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    st = signature_task or {}
    senders = _mca_near_sender_ids(
        network_id,
        lending_rows=lending_rows or [],
        mca_id=mca_id,
        signature_task=st,
    )

    tx_hash_direct = st.get("txHash")
    if tx_hash_direct:
        rsv = fetch_evm_rsv_from_near_tx(network_id, str(tx_hash_direct), senders)
        if rsv:
            return rsv

    if lending_rows:
        return extract_mca_evm_rsv_from_lending_batch(
            network_id,
            lending_rows,
            mca_id=mca_id,
            signature_task=st,
        )
    return None


def build_permit_body_from_mca_request(
    permit_request: Dict[str, Any],
    rsv: Dict[str, Any],
    token: str,
    default_spender: str,
) -> Dict[str, Any]:
    pr = permit_request or {}
    body = {
        "deadline": str(pr.get("deadline", "")),
        "owner": str(pr.get("owner", "")),
        "r": str(rsv.get("r", "")),
        "s": str(rsv.get("s", "")),
        "spender": str(pr.get("spender") or default_spender),
        "token": str(pr.get("token") or token),
        "v": _normalize_sig_v(rsv.get("v")),
        "value": str(pr.get("value", "")),
    }
    for k in ("deadline", "owner", "r", "s", "value"):
        if not body.get(k):
            raise ValueError(f"permit field {k} is empty")
    return body


def extract_deposit_hash_from_permit_records(rec: Dict[str, Any]) -> Optional[str]:
    data = rec.get("data") if isinstance(rec, dict) else None
    if isinstance(data, list) and data:
        item = data[0]
        if isinstance(item, dict):
            tx_hash = item.get("tx_hash")
            if tx_hash:
                return str(tx_hash)
    return None


def _ledger_updates_list(updates: Any) -> List[Dict[str, Any]]:
    if isinstance(updates, list):
        return [x for x in updates if isinstance(x, dict)]
    if isinstance(updates, dict):
        for k in ("updates", "data"):
            v = updates.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def extract_withdraw_hash_from_ledger(updates: Any, withdraw_nonce: Any) -> Optional[str]:
    target = str(withdraw_nonce)
    for item in _ledger_updates_list(updates):
        delta = item.get("delta") or {}
        if str(delta.get("type", "")).lower() != "withdraw":
            continue
        if str(delta.get("nonce")) != target:
            continue
        h = item.get("hash")
        if h:
            return str(h)
    return None
