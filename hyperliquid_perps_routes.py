#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Perps Hyperliquid API — /api/v1/perps/hyperliquid

POST /deposits, POST /withdrawals, GET /transfer-jobs/<id>, GET /transfer-history
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request
from loguru import logger
from pymysql.err import IntegrityError

from config import Cfg
from db_provider import (
    ensure_hyperliquid_transfer_jobs_table,
    get_hyperliquid_transfer_job_by_client_request_id,
    get_hyperliquid_transfer_job_by_job_id,
    insert_hyperliquid_transfer_job,
    list_hyperliquid_transfer_history,
)

bp = Blueprint("hyperliquid_perps", __name__, url_prefix="/api/v1/perps/hyperliquid")

NEXT_POLL_MS = 3000


def _norm_addr(a: Optional[str]) -> str:
    if not a:
        return ""
    s = str(a).strip()
    if s.startswith("0x") or s.startswith("0X"):
        return "0x" + s[2:].lower()
    return s.lower()


def _dt_ms(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    if isinstance(v, str) and len(v) >= 19:
        try:
            d = datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S")
            return int(d.timestamp() * 1000)
        except Exception:
            return None
    return None


def _ok(data: Dict[str, Any]):
    out = {"code": 0, "data": data}
    out["data"]["nextPollMs"] = NEXT_POLL_MS
    return jsonify(out)


def _bad(msg: str, http: int = 400):
    return jsonify({"code": -1, "msg": msg}), http


def _new_job_id(kind: str) -> str:
    prefix = "dep" if kind == "deposit" else "wd"
    return f"{prefix}_{secrets.token_hex(16)}"


def _tx_list_from_row(row: Dict[str, Any]) -> List[str]:
    raw = row.get("tx_hashes_json")
    if not raw:
        return []
    try:
        v = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(v, list):
            return [str(x) for x in v if x]
        return []
    except Exception:
        return []


def _external_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw = row.get("external_status_json")
    if not raw:
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return None


def _message_for_status(transfer_type: str, status: str, last_error: Optional[str]) -> str:
    if status == "FAILED":
        return (last_error or "").strip() or "Failed"
    if transfer_type == "deposit":
        m = {
            "SUBMITTED": "Job accepted",
            "WAITING_SIGNATURE": "Waiting for relayer signature",
            "WAITING_BRIDGE": "Waiting for bridge settlement",
            "SUBMITTING_PERMIT": "Submitting permit",
            "WAITING_PERMIT": "Confirming deposit",
            "SUCCESS": "Success",
        }
    else:
        m = {
            "SUBMITTED": "Job accepted",
            "WAITING_SIGNATURE": "Waiting for relayer signature",
            "SUBMITTING_EXCHANGE": "Submitting withdrawal",
            "WAITING_LEDGER": "Waiting for ledger confirmation",
            "WAITING_BRIDGE": "Waiting for bridge settlement",
            "SUCCESS": "Success",
        }
    return m.get(status, status)


def _job_to_api(row: Dict[str, Any]) -> Dict[str, Any]:
    t = row.get("transfer_type") or ""
    st = row.get("status") or ""
    return {
        "jobId": row.get("job_id"),
        "clientRequestId": row.get("client_request_id"),
        "transferType": t,
        "accountMode": row.get("account_mode"),
        "hyperliquidUserAddress": row.get("hyperliquid_user_address"),
        "destinationAddress": row.get("destination_address"),
        "status": st,
        "message": _message_for_status(t, st, row.get("last_error")),
        "progress": int(row.get("progress") or 0),
        "txHashes": _tx_list_from_row(row),
        "externalStatus": _external_from_row(row),
        "permitId": row.get("permit_id"),
        "createdAt": _dt_ms(row.get("created_at")),
        "updatedAt": _dt_ms(row.get("updated_at")),
        "finishedAt": _dt_ms(row.get("finished_at")),
    }


def _validate_transfer_block(body: Dict[str, Any]) -> Optional[str]:
    tr = body.get("transfer")
    if tr is None:
        return "transfer is required"
    if not isinstance(tr, dict):
        return "transfer must be an object"
    sk = tr.get("skipped")
    if not isinstance(sk, bool):
        return "transfer.skipped must be a boolean"
    if not sk:
        h = tr.get("txHash")
        if not h or not str(h).strip():
            return "transfer.txHash is required when transfer.skipped is false"
    return None


def _validate_quote(body: Dict[str, Any]) -> Optional[str]:
    q = body.get("quote")
    if q is None or not isinstance(q, dict):
        return "quote object is required"
    if "needsBridge" not in q:
        return "quote.needsBridge is required"
    if not isinstance(q["needsBridge"], bool):
        return "quote.needsBridge must be a boolean"
    if q["needsBridge"]:
        da = q.get("depositAddress")
        if not da or not str(da).strip():
            return "quote.depositAddress is required when quote.needsBridge is true"
    return None


def _validate_deposit(body: Dict[str, Any]) -> Optional[str]:
    for k in ("hyperliquidUserAddress", "clientRequestId"):
        if not body.get(k):
            return f"{k} is required"
    am = (body.get("accountMode") or "").strip().lower()
    if am not in ("evm", "mca"):
        return "accountMode must be evm or mca"
    err = _validate_quote(body)
    if err:
        return err
    err = _validate_transfer_block(body)
    if err:
        return err

    ps = body.get("permitSignature")
    stask = body.get("signatureTask") or {}
    if am == "mca":
        if (stask.get("type") or "").strip() != "mca_relayer":
            return "signatureTask.type must be mca_relayer for accountMode mca"
        if not stask.get("batchId"):
            return "signatureTask.batchId is required for accountMode mca"
    if am == "evm":
        if not isinstance(ps, dict):
            return "permitSignature object is required for accountMode evm"
        owner = _norm_addr((ps.get("owner") or body.get("hyperliquidUserAddress")))
        exp = _norm_addr(body.get("hyperliquidUserAddress"))
        if owner != exp:
            return "permitSignature.owner must match hyperliquidUserAddress"
    return None


def _validate_withdraw(body: Dict[str, Any]) -> Optional[str]:
    for k in ("hyperliquidUserAddress", "clientRequestId", "withdrawAction", "destinationAddress"):
        if body.get(k) is None or (isinstance(body.get(k), str) and not str(body.get(k)).strip()):
            return f"{k} is required"
    am = (body.get("accountMode") or "").strip().lower()
    if am not in ("evm", "mca"):
        return "accountMode must be evm or mca"
    err = _validate_quote(body)
    if err:
        return err
    err = _validate_transfer_block(body)
    if err:
        return err

    wa = body["withdrawAction"]
    if not isinstance(wa, dict):
        return "withdrawAction must be an object"
    for k in ("destination", "amount", "time"):
        if wa.get(k) is None:
            return f"withdrawAction.{k} is required"
    q = body["quote"]
    needs_bridge = bool(q.get("needsBridge"))
    dest_user = _norm_addr(body.get("destinationAddress"))
    whl_dest = _norm_addr(wa.get("destination"))
    if needs_bridge:
        da = _norm_addr(q.get("depositAddress"))
        if dest_user != da:
            return "destinationAddress must match quote.depositAddress when quote.needsBridge is true"
        if whl_dest != da:
            return "withdrawAction.destination must match quote.depositAddress when quote.needsBridge is true"
    else:
        if dest_user != whl_dest:
            return "destinationAddress must match withdrawAction.destination when quote.needsBridge is false"

    stask = body.get("signatureTask") or {}
    sig = body.get("signature")
    if am == "mca":
        if (stask.get("type") or "").strip() != "mca_relayer":
            return "signatureTask.type must be mca_relayer for accountMode mca"
        if not stask.get("batchId"):
            return "signatureTask.batchId is required for accountMode mca"
    else:
        if not isinstance(sig, dict):
            return "signature {r,s,v} is required for accountMode evm"
        for k in ("r", "s", "v"):
            if sig.get(k) is None or str(sig.get(k)).strip() == "":
                return f"signature.{k} is required for accountMode evm"

    exp = _norm_addr(body.get("hyperliquidUserAddress"))
    if exp and am == "evm" and isinstance(sig, dict):
        pass
    return None


def _intent_nonces_json(body: Dict[str, Any]) -> Optional[str]:
    v = body.get("intentNonces")
    if v is None:
        return None
    try:
        return json.dumps(v, ensure_ascii=False, default=str)
    except Exception:
        return None


def _create_or_get_job(
    transfer_type: str,
    body: Dict[str, Any],
    *,
    destination_address: Optional[str],
) -> Tuple[Dict[str, Any], bool]:
    """
    Returns (row_dict, created).
    """
    network_id = Cfg.NETWORK_ID
    cid = str(body["clientRequestId"]).strip()
    existing = get_hyperliquid_transfer_job_by_client_request_id(network_id, cid)
    if existing:
        return existing, False

    am = str(body.get("accountMode")).strip().lower()
    hl = str(body.get("hyperliquidUserAddress")).strip()
    q = body.get("quote") or {}
    batch_id = None
    stask = body.get("signatureTask") or {}
    if isinstance(stask, dict) and stask.get("batchId"):
        batch_id = str(stask.get("batchId")).strip()

    dep_addr = None
    if isinstance(q, dict) and q.get("needsBridge"):
        dep_addr = (q.get("depositAddress") or "").strip() or None

    tr = body.get("transfer") or {}
    tx_hashes = []
    if tr.get("txHash"):
        tx_hashes.append(str(tr.get("txHash")).strip())

    payload = json.dumps(body, ensure_ascii=False, default=str)
    row_in = {
        "job_id": _new_job_id(transfer_type),
        "client_request_id": cid[:128],
        "transfer_type": transfer_type,
        "account_mode": am,
        "hyperliquid_user_address": hl[:128],
        "destination_address": (destination_address or "")[:256] or None,
        "status": "SUBMITTED",
        "message": "Job accepted",
        "progress": 5,
        "request_payload": payload,
        "tx_hashes_json": json.dumps(tx_hashes) if tx_hashes else None,
        "external_status_json": None,
        "last_error": None,
        "permit_id": None,
        "deposit_address": dep_addr,
        "batch_id": batch_id,
        "intent_nonces_json": _intent_nonces_json(body),
    }
    try:
        new_id = insert_hyperliquid_transfer_job(network_id, row_in)
        if not new_id:
            ex2 = get_hyperliquid_transfer_job_by_client_request_id(network_id, cid)
            if ex2:
                return ex2, False
            raise RuntimeError("insert failed")
    except IntegrityError:
        ex2 = get_hyperliquid_transfer_job_by_client_request_id(network_id, cid)
        if ex2:
            return ex2, False
        raise

    row = get_hyperliquid_transfer_job_by_job_id(network_id, row_in["job_id"])
    return row, True


@bp.route("/deposits", methods=["POST"])
def perps_hl_deposits():
    try:
        body = request.get_json(force=True, silent=False)
    except Exception:
        return _bad("invalid JSON body")
    if not isinstance(body, dict):
        return _bad("body must be a JSON object")

    err = _validate_deposit(body)
    if err:
        return _bad(err)

    ensure_hyperliquid_transfer_jobs_table(Cfg.NETWORK_ID)
    try:
        row, _created = _create_or_get_job("deposit", body, destination_address=None)
    except Exception as e:
        logger.error(f"perps_hl_deposits: {e}")
        return _bad("failed to create job", 500)

    return _ok(_job_to_api(row))


@bp.route("/withdrawals", methods=["POST"])
def perps_hl_withdrawals():
    try:
        body = request.get_json(force=True, silent=False)
    except Exception:
        return _bad("invalid JSON body")
    if not isinstance(body, dict):
        return _bad("body must be a JSON object")

    err = _validate_withdraw(body)
    if err:
        return _bad(err)

    ensure_hyperliquid_transfer_jobs_table(Cfg.NETWORK_ID)
    dest = str(body.get("destinationAddress")).strip()
    try:
        row, _created = _create_or_get_job("withdrawal", body, destination_address=dest)
    except Exception as e:
        logger.error(f"perps_hl_withdrawals: {e}")
        return _bad("failed to create job", 500)

    return _ok(_job_to_api(row))


@bp.route("/transfer-jobs/<job_id>", methods=["GET"])
def perps_hl_transfer_job(job_id: str):
    jid = (job_id or "").strip()
    if not jid:
        return _bad("jobId is required")
    ensure_hyperliquid_transfer_jobs_table(Cfg.NETWORK_ID)
    row = get_hyperliquid_transfer_job_by_job_id(Cfg.NETWORK_ID, jid)
    if not row:
        return _bad("job not found", 404)
    return _ok(_job_to_api(row))


@bp.route("/transfer-history", methods=["GET"])
def perps_hl_transfer_history():
    hl = request.args.get("hyperliquidUserAddress") or request.args.get("hyperliquid_user_address")
    if not hl or not str(hl).strip():
        return _bad("hyperliquidUserAddress query parameter is required")
    try:
        page = int(request.args.get("page", "1"))
    except Exception:
        page = 1
    try:
        page_size = int(request.args.get("pageSize", request.args.get("page_size", "20")))
    except Exception:
        page_size = 20

    ensure_hyperliquid_transfer_jobs_table(Cfg.NETWORK_ID)
    rows, total = list_hyperliquid_transfer_history(Cfg.NETWORK_ID, hl, page, page_size)
    items = []
    for r in rows:
        items.append(
            {
                "historyId": r.get("job_id"),
                "jobId": r.get("job_id"),
                "transferType": r.get("transfer_type"),
                "accountMode": r.get("account_mode"),
                "status": r.get("status"),
                "message": _message_for_status(
                    r.get("transfer_type") or "",
                    r.get("status") or "",
                    r.get("last_error"),
                ),
                "progress": int(r.get("progress") or 0),
                "permitId": r.get("permit_id"),
                "txHashes": _tx_list_from_row(r),
                "createdAt": _dt_ms(r.get("created_at")),
                "updatedAt": _dt_ms(r.get("updated_at")),
                "finishedAt": _dt_ms(r.get("finished_at")),
            }
        )

    data = {
        "items": items,
        "page": page,
        "pageSize": page_size,
        "total": total,
        "nextPollMs": NEXT_POLL_MS,
    }
    return jsonify({"code": 0, "data": data})


def register_hyperliquid_perps(app):
    """Register blueprint (table ensure is done on first request / app startup)."""
    app.register_blueprint(bp)
