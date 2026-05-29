#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Perps Hyperliquid API — /api/v1/perps/hyperliquid

POST /deposits, POST /withdrawals, GET /transfer-jobs/<id>, GET /transfer-history
"""

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


def _hashes_from_row(row: Dict[str, Any]) -> Dict[str, Optional[str]]:
    tx_list = _tx_list_from_row(row)
    ext = _external_from_row(row) or {}
    return {
        "transferHash": tx_list[0] if tx_list else None,
        "depositHash": ext.get("depositHash") or None,
        "withdrawHash": ext.get("withdrawHash") or None,
    }


_EVM_EXPLORER_BASE = {
    "arbitrum": "https://arbiscan.io",
    "arb": "https://arbiscan.io",
    "aurora": "https://explorer.aurora.dev",
    "avalanche": "https://cchain.explorer.avax.network",
    "avax": "https://cchain.explorer.avax.network",
    "base": "https://basescan.org",
    "ethereum": "https://etherscan.io",
    "eth": "https://etherscan.io",
    "flare": "https://flare-explorer.flare.network",
    "mantle": "https://explorer.mantle.xyz",
    "optimism": "https://optimistic.etherscan.io",
    "op": "https://optimistic.etherscan.io",
    "polygon": "https://polygonscan.com",
    "pol": "https://polygonscan.com",
    "scroll": "https://scrollscan.com",
    "sei": "https://seitrace.com",
    "taiko": "https://taikoscan.io",
    "bsc": "https://bscscan.com",
    "gravity": "https://explorer.gravity.xyz",
    "bera": "https://berascan.com",
    "monad": "https://monadvision.com",
    "xlayer": "https://www.oklink.com/xlayer",
    "plasma": "https://plasmascan.to",
}


def _empty_explorer() -> Dict[str, Any]:
    return {
        "displayHash": None,
        "displayHashType": None,
        "chain": None,
        "url": None,
    }


def _explorer_chain_id(
    chain: Optional[str],
    *,
    chain_label: Optional[str] = None,
    token_chain: Optional[str] = None,
) -> Optional[str]:
    c = (chain or "").strip().lower()
    if c and c != "evm":
        return c
    for cand in (chain_label, token_chain):
        if cand:
            return str(cand).strip().lower()
    return c or None


def _explorer_tx_url(
    chain: Optional[str],
    tx_hash: Optional[str],
    *,
    chain_label: Optional[str] = None,
    token_chain: Optional[str] = None,
) -> Optional[str]:
    if not tx_hash or not str(tx_hash).strip():
        return None
    h = str(tx_hash).strip()
    c = (chain or "").strip().lower()

    if c == "intents":
        return "https://explorer.near-intents.org/?search={0}".format(h)
    if c == "near":
        return "https://nearblocks.io/txns/{0}".format(h)
    if c == "btc":
        return "https://mempool.space/tx/{0}".format(h)
    if c in ("solana", "sol"):
        return "https://explorer.solana.com/tx/{0}".format(h)
    if c == "zcash":
        return "https://mainnet.zcashexplorer.app/transactions/{0}".format(h)
    if c == "aptos":
        return "https://explorer.aptoslabs.com/txn/{0}".format(h)
    if c == "tron":
        return "https://tronscan.org/#/transaction/{0}".format(h)
    if c == "hyperliquid":
        return "https://app.hyperliquid.xyz/explorer/tx/{0}".format(h)

    sub = None
    if c == "evm":
        sub = chain_label or token_chain
    elif c:
        sub = c

    if sub:
        sk = str(sub).strip().lower()
        base = _EVM_EXPLORER_BASE.get(sk)
        if base:
            return "{0}/tx/{1}".format(base.rstrip("/"), h)

    return None


def _display_meta_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw = row.get("display_meta_json")
    if raw:
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    payload_raw = row.get("request_payload")
    if payload_raw:
        try:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
            if isinstance(payload, dict):
                dm = payload.get("displayMeta")
                if isinstance(dm, dict):
                    return dm
        except Exception:
            pass
    return None


def _explorer_from_row(
    row: Dict[str, Any],
    display_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if display_meta is None:
        display_meta = _display_meta_from_row(row)
    hashes = _hashes_from_row(row)
    t = (row.get("transfer_type") or "").lower()
    empty = _empty_explorer()

    if t == "deposit":
        h = hashes.get("depositHash")
        if not h:
            return empty
        return {
            "displayHash": h,
            "displayHashType": "depositHash",
            "chain": "arbitrum",
            "url": _explorer_tx_url("arbitrum", h),
        }

    if t == "withdrawal":
        h = hashes.get("withdrawHash")
        if not h:
            return empty
        target = (display_meta or {}).get("target") or {}
        chain = (target.get("chain") or "").strip() or None
        chain_label = target.get("chainLabel")
        token_chain = None
        output = (display_meta or {}).get("output") or {}
        tok = output.get("token") or {}
        if isinstance(tok, dict):
            token_chain = tok.get("chain")
        return {
            "displayHash": h,
            "displayHashType": "withdrawHash",
            "chain": _explorer_chain_id(
                chain,
                chain_label=chain_label,
                token_chain=token_chain,
            ),
            "url": _explorer_tx_url(
                chain,
                h,
                chain_label=chain_label,
                token_chain=token_chain,
            ),
        }

    return empty


_ARBISCAN_TX_PREFIX = "https://arbiscan.io/tx/"


def _explorer_for_api_response(
    row: Dict[str, Any],
    display_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """API 返回：withdrawHash 外链固定 Arbiscan（与 depositHash 一致）。"""
    explorer = _explorer_from_row(row, display_meta)
    if explorer.get("displayHashType") != "withdrawHash":
        return explorer
    h = explorer.get("displayHash")
    if not h or not str(h).strip():
        return explorer
    out = dict(explorer)
    out["chain"] = "arbitrum"
    out["url"] = "{0}{1}".format(_ARBISCAN_TX_PREFIX, str(h).strip())
    return out


def _validate_display_meta_endpoint(dm: Any, endpoint: str) -> Optional[str]:
    if not isinstance(dm, dict):
        return "displayMeta object is required"
    if dm.get("version") != 1:
        return "displayMeta.version must be 1"

    for side in ("source", "target"):
        block = dm.get(side)
        if not isinstance(block, dict):
            return "displayMeta.{0} is required".format(side)
        st = (block.get("type") or "").strip()
        if st not in ("external_wallet", "trading_account"):
            return "displayMeta.{0}.type is invalid".format(side)
        for k in ("chain", "chainLabel"):
            if not block.get(k) or not str(block.get(k)).strip():
                return "displayMeta.{0}.{1} is required".format(side, k)

    if endpoint == "deposit":
        src = dm.get("source") or {}
        tgt = dm.get("target") or {}
        if src.get("type") != "external_wallet":
            return "displayMeta.source.type must be external_wallet for deposit"
        if tgt.get("type") != "trading_account":
            return "displayMeta.target.type must be trading_account for deposit"
    elif endpoint == "withdrawal":
        src = dm.get("source") or {}
        tgt = dm.get("target") or {}
        if src.get("type") != "trading_account":
            return "displayMeta.source.type must be trading_account for withdrawal"
        if tgt.get("type") != "external_wallet":
            return "displayMeta.target.type must be external_wallet for withdrawal"

    for side in ("input", "output"):
        block = dm.get(side)
        if not isinstance(block, dict):
            return "displayMeta.{0} is required".format(side)
        tok = block.get("token")
        if not isinstance(tok, dict):
            return "displayMeta.{0}.token is required".format(side)
        if not tok.get("symbol") or not str(tok.get("symbol")).strip():
            return "displayMeta.{0}.token.symbol is required".format(side)
        for k in ("amountRaw", "amountFormatted"):
            if block.get(k) is None or not str(block.get(k)).strip():
                return "displayMeta.{0}.{1} is required".format(side, k)

    fee = dm.get("fee")
    if not isinstance(fee, dict):
        return "displayMeta.fee is required"
    for k in ("amountUsd", "formatted"):
        if fee.get(k) is None or not str(fee.get(k)).strip():
            return "displayMeta.fee.{0} is required".format(k)
    return None


def _display_meta_json(body: Dict[str, Any]) -> Optional[str]:
    dm = body.get("displayMeta")
    if dm is None:
        return None
    return json.dumps(dm, ensure_ascii=False, default=str)


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


def _progress_for_transfer(status: str, transfer_type: str) -> int:
    """Deposit/withdrawal 对 WAITING_BRIDGE / WAITING_LEDGER 的 progress 映射不同。"""
    st = (status or "").strip()
    t = (transfer_type or "").strip().lower()
    if t == "withdrawal":
        dmap = {
            "SUBMITTED": 5,
            "WAITING_SIGNATURE": 20,
            "WAITING_LEDGER": 40,
            "SUBMITTING_EXCHANGE": 55,
            "WAITING_BRIDGE": 75,
            "SUCCESS": 100,
            "FAILED": 0,
        }
    else:
        dmap = {
            "SUBMITTED": 5,
            "WAITING_SIGNATURE": 20,
            "WAITING_BRIDGE": 40,
            "SUBMITTING_PERMIT": 60,
            "WAITING_PERMIT": 80,
            "SUCCESS": 100,
            "FAILED": 0,
        }
    return int(dmap.get(st, int(10)))


def _job_to_api(row: Dict[str, Any]) -> Dict[str, Any]:
    t = row.get("transfer_type") or ""
    st = row.get("status") or ""
    display_meta = _display_meta_from_row(row)
    return {
        "jobId": row.get("job_id"),
        "clientRequestId": row.get("client_request_id"),
        "transferType": t,
        "accountMode": row.get("account_mode"),
        "hyperliquidUserAddress": row.get("hyperliquid_user_address"),
        "destinationAddress": row.get("destination_address"),
        "status": st,
        "message": _message_for_status(t, st, row.get("last_error")),
        "progress": _progress_for_transfer(st, t),
        "txHashes": _tx_list_from_row(row),
        "hashes": _hashes_from_row(row),
        "displayMeta": display_meta,
        "explorer": _explorer_for_api_response(row, display_meta),
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


def _validate_permit_signature_owner(ps: Dict[str, Any], hyperliquid_user_address: str) -> Optional[str]:
    if not isinstance(ps, dict):
        return "permitSignature object is required"
    owner = _norm_addr(ps.get("owner") or hyperliquid_user_address)
    exp = _norm_addr(hyperliquid_user_address)
    if owner != exp:
        return "permitSignature.owner must match hyperliquidUserAddress"
    return None


def _validate_exchange_signature(sig: Any, *, label: str = "signature") -> Optional[str]:
    if not isinstance(sig, dict):
        return f"{label} {{r,s,v}} is required"
    for k in ("r", "s", "v"):
        if sig.get(k) is None or str(sig.get(k)).strip() == "":
            return f"{label}.{k} is required"
    return None


def _validate_mca_signature_task(stask: Any) -> Optional[str]:
    if not isinstance(stask, dict):
        return "signatureTask object is required"
    if (stask.get("type") or "").strip() != "mca_relayer":
        return "signatureTask.type must be mca_relayer for accountMode mca"
    batch_id = stask.get("batchId")
    tx_hash = stask.get("txHash")
    zcash_addr = stask.get("zcashDepositAddress")
    signer_chain = (stask.get("signerChain") or "").strip().lower()
    if batch_id or tx_hash or zcash_addr:
        if signer_chain == "zcash":
            if not tx_hash and not zcash_addr:
                return (
                    "signatureTask.zcashDepositAddress or txHash is required "
                    "for signerChain zcash"
                )
        return None
    return (
        "signatureTask.batchId, txHash, or zcashDepositAddress is required "
        "for accountMode mca"
    )


def _validate_permit_request(pr: Any, hyperliquid_user_address: str) -> Optional[str]:
    if not isinstance(pr, dict):
        return "permitRequest object is required for accountMode mca"
    for k in ("spender", "token", "value", "nonce", "deadline"):
        val = pr.get(k)
        if val is None or (isinstance(val, str) and not str(val).strip()):
            return f"permitRequest.{k} is required for accountMode mca"
    owner = _norm_addr(pr.get("owner") or hyperliquid_user_address)
    exp = _norm_addr(hyperliquid_user_address)
    if owner != exp:
        return "permitRequest.owner must match hyperliquidUserAddress"
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
    pr = body.get("permitRequest")
    hl = body.get("hyperliquidUserAddress")
    if am == "mca":
        has_ps = isinstance(ps, dict) and bool(ps)
        has_relayer = isinstance(pr, dict) and (
            isinstance(stask, dict)
            and (
                stask.get("batchId")
                or stask.get("txHash")
                or stask.get("zcashDepositAddress")
            )
        )
        if not has_ps and not has_relayer:
            return (
                "permitSignature or (permitRequest + signatureTask) "
                "is required for accountMode mca"
            )
        if has_ps:
            err = _validate_permit_signature_owner(ps, hl)
            if err:
                return err
        if has_relayer and not has_ps:
            err = _validate_permit_request(pr, hl)
            if err:
                return err
            err = _validate_mca_signature_task(stask)
            if err:
                return err
    if am == "evm":
        if not isinstance(ps, dict):
            return "permitSignature object is required for accountMode evm"
        err = _validate_permit_signature_owner(ps, hl)
        if err:
            return err
    err = _validate_display_meta_endpoint(body.get("displayMeta"), "deposit")
    if err:
        return err
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
        if whl_dest != da:
            return "withdrawAction.destination must match quote.depositAddress when quote.needsBridge is true"
    else:
        if dest_user != whl_dest:
            return "destinationAddress must match withdrawAction.destination when quote.needsBridge is false"

    stask = body.get("signatureTask") or {}
    sig = body.get("signature")
    if am == "mca":
        has_sig = isinstance(sig, dict) and bool(sig.get("r"))
        has_stask = isinstance(stask, dict) and (
            stask.get("batchId")
            or stask.get("txHash")
            or stask.get("zcashDepositAddress")
        )
        if not has_sig and not has_stask:
            return "signature or signatureTask is required for accountMode mca"
        if has_sig:
            err = _validate_exchange_signature(sig, label="signature")
            if err:
                return err
        elif has_stask:
            err = _validate_mca_signature_task(stask)
            if err:
                return err
    else:
        err = _validate_exchange_signature(sig, label="signature")
        if err:
            return f"{err} for accountMode evm"
    err = _validate_display_meta_endpoint(body.get("displayMeta"), "withdrawal")
    if err:
        return err
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
        "display_meta_json": _display_meta_json(body),
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
        display_meta = _display_meta_from_row(r)
        t = r.get("transfer_type") or ""
        st = r.get("status") or ""
        items.append(
            {
                "historyId": r.get("job_id"),
                "jobId": r.get("job_id"),
                "transferType": t,
                "accountMode": r.get("account_mode"),
                "status": st,
                "message": _message_for_status(
                    t,
                    st,
                    r.get("last_error"),
                ),
                "progress": _progress_for_transfer(st, t),
                "permitId": r.get("permit_id"),
                "txHashes": _tx_list_from_row(r),
                "hashes": _hashes_from_row(r),
                "displayMeta": display_meta,
                "explorer": _explorer_for_api_response(r, display_meta),
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
