#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Advances `hyperliquid_transfer_jobs` for Perps Hyperliquid API.

Usage: python hyperliquid_perps_worker.py MAINNET
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.path.append("../")
from config import Cfg

STEP_GATE_SEC = 600
STEP_PERMIT_SEC = 600
STEP_CONFIRM_SEC = 600
STEP_LEDGER_SEC = 600
STEP_BRIDGE_SEC = 86400
# Absolute anti-zombie backstop for a bridge deposit. Even if 1Click never
# returns a terminal status and polling keeps failing, the job is reaped after
# this. Sized above the longest 1Click deposit window (~3 days) so it never
# fires during a legitimate, still-valid quote.
STEP_DEPOSIT_ABS_CAP_SEC = 3 * 86400 + 3600
# Extra slack added on top of the 1Click quote deadline before failing locally.
DEPOSIT_DEADLINE_GRACE_SEC = 600

from hyperliquid_deposit_worker import (  # noqa: E402
    HL_TERMINAL_HISTORY,
    build_permit_body_from_lending_first_row,
    extract_origin_tx_hash_from_oneclick,
    get_permit_records,
    poll_oneclick_status,
    post_permit,
    _format_history_status,
)
from mca_evm_signature import (  # noqa: E402
    build_permit_body_from_mca_request,
    extract_deposit_hash_from_permit_records,
    extract_withdraw_hash_from_ledger,
    is_zcash_bridge_deposit_body,
    is_zcash_legacy_signature_task,
    lending_batch_complete,
    lending_batch_error,
    resolve_mca_evm_rsv,
    resolve_zcash_signature_task,
)


def _utcnow() -> datetime:
    return datetime.utcnow()


def _parse_dt(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.strptime(val[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    return None


def _parse_iso_dt(val) -> Optional[datetime]:
    """Parse a 1Click ISO-8601 timestamp (UTC, possibly 'Z'-suffixed) to naive UTC."""
    if not isinstance(val, str) or not val.strip():
        return None
    s = val.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _loads_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("request_payload") or "{}"
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _ext(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("external_status_json")
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return {}


def _merge_ext(row: Dict[str, Any], patch: Dict[str, Any]) -> str:
    base = _ext(row)
    base.update(patch)
    return json.dumps(base, ensure_ascii=False, default=str)[:65000]


def _progress_for(status: str, transfer_type: str) -> int:
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
    return int(dmap.get(status, 10))


def _fail_job(
    network_id: str,
    job_id: str,
    msg: str,
    *,
    ext_patch: Optional[Dict[str, Any]] = None,
) -> None:
    from db_provider import get_hyperliquid_transfer_job_by_job_id, update_hyperliquid_transfer_job

    cur = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or {}
    ex = _ext(cur)
    if ext_patch:
        ex.update(ext_patch)
    patch: Dict[str, Any] = {
        "status": "FAILED",
        "message": "Failed",
        "progress": 0,
        "last_error": (msg or "")[:4000],
        "finished_at": _utcnow(),
        "external_status_json": json.dumps(ex, ensure_ascii=False, default=str)[:65000],
    }
    update_hyperliquid_transfer_job(network_id, job_id, patch)


def _update_job(
    network_id: str,
    job_id: str,
    fields: Dict[str, Any],
    *,
    transfer_type: Optional[str] = None,
) -> None:
    from db_provider import update_hyperliquid_transfer_job

    if "status" in fields:
        tt = transfer_type
        if not tt:
            from db_provider import get_hyperliquid_transfer_job_by_job_id

            row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or {}
            tt = row.get("transfer_type") or ""
        fields["progress"] = _progress_for(str(fields["status"]), tt)
    update_hyperliquid_transfer_job(network_id, job_id, fields)


def build_permit_body_from_deposit_payload(
    payload: Dict[str, Any], owner: str, token: str, spender: str
) -> Dict[str, Any]:
    ps = payload.get("permitSignature")
    if isinstance(ps, dict):
        return build_permit_body_from_lending_first_row(
            {"request": ps},
            owner,
            token,
            spender,
        )
    return build_permit_body_from_lending_first_row(
        {"request": payload},
        owner,
        token,
        spender,
    )


def _hl_info_post(url: str, body: Dict[str, Any]) -> Any:
    r = requests.post(
        url,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=45,
    )
    r.raise_for_status()
    return r.json()


def _hl_exchange_post(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(
        url,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=45,
    )
    text = r.text
    try:
        data = r.json()
    except Exception:
        data = {"raw": text}
    if not r.ok:
        raise RuntimeError(str(data)[:2000])
    if data.get("status") == "err":
        resp = data.get("response")
        err = resp if isinstance(resp, str) else (resp or {}).get("error") or str(resp)
        raise RuntimeError(str(err)[:2000])
    if data.get("status") == "ok":
        resp = data.get("response")
        if isinstance(resp, dict):
            st = resp.get("data", {}).get("statuses") if isinstance(resp.get("data"), dict) else None
            if isinstance(st, list):
                for item in st:
                    if isinstance(item, dict) and "error" in item:
                        raise RuntimeError(str(item.get("error"))[:2000])
    return data


def _has_withdraw_ledger(updates: Any) -> bool:
    if isinstance(updates, dict):
        for k in ("updates", "data"):
            v = updates.get(k)
            if isinstance(v, list):
                updates = v
                break
        else:
            updates = []
    if not isinstance(updates, list):
        return False
    for item in updates:
        delta = item.get("delta") if isinstance(item, dict) else None
        d = delta if isinstance(delta, dict) else item
        if not isinstance(d, dict):
            continue
        t = str(d.get("type") or "").lower()
        if "withdraw" in t:
            return True
    return False


def _poll_withdraw_hash_via_oneclick(
    deposit_address: str,
    ext: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    """Return (withdraw_hash, oneclick_status, ext_patch)."""
    patch: Dict[str, Any] = {}
    try:
        oc_st, oc_data = poll_oneclick_status(deposit_address)
    except Exception as e:
        patch["bridgeOneClickPollError"] = str(e)[:500]
        return None, None, patch

    patch["bridgeOneClickPoll"] = oc_st
    if oc_st == "SUCCESS":
        wh = extract_origin_tx_hash_from_oneclick(oc_data)
        if wh:
            patch["withdrawHashSource"] = "oneclick_status"
            return wh, oc_st, patch
    return None, oc_st, patch


def _advance_withdraw_after_hash(
    network_id: str,
    job_id: str,
    *,
    withdraw_hash: str,
    updates: Any,
    ext: Dict[str, Any],
    needs_bridge: bool,
    deposit_address: str,
) -> None:
    if updates is not None:
        ext["ledgerUpdates"] = json.dumps(updates, default=str)[:4000]
    ext["withdrawHash"] = withdraw_hash

    if needs_bridge and deposit_address:
        try:
            oc_st, oc_data = poll_oneclick_status(deposit_address)
        except Exception as e:
            ext["bridgeOneClickPollError"] = str(e)[:500]
            _update_job(
                network_id,
                job_id,
                {
                    "status": "WAITING_BRIDGE",
                    "message": "Waiting for bridge settlement",
                    "external_status_json": json.dumps(ext, ensure_ascii=False)[:65000],
                },
            )
            return

        ext["bridgeOneClick"] = oc_st
        if oc_st == "SUCCESS":
            dest_hashes = []
            sd = oc_data.get("swapDetails") if isinstance(oc_data.get("swapDetails"), dict) else {}
            for item in sd.get("destinationChainTxHashes") or []:
                if isinstance(item, dict) and item.get("hash"):
                    dest_hashes.append(str(item["hash"]))
            if dest_hashes:
                ext["bridgeDestinationTxHashes"] = dest_hashes[:5]
            fin = _utcnow()
            _update_job(
                network_id,
                job_id,
                {
                    "status": "SUCCESS",
                    "message": "Success",
                    "progress": 100,
                    "finished_at": fin,
                    "external_status_json": json.dumps(ext, ensure_ascii=False)[:65000],
                },
            )
            return
        if oc_st in ("FAILED", "REFUNDED", "EXPIRED"):
            _fail_job(network_id, job_id, f"bridge 1Click terminal: {oc_st}", ext_patch=ext)
            return
        _update_job(
            network_id,
            job_id,
            {
                "status": "WAITING_BRIDGE",
                "message": "Waiting for bridge settlement",
                "external_status_json": json.dumps(ext, ensure_ascii=False)[:65000],
            },
        )
        return

    fin = _utcnow()
    _update_job(
        network_id,
        job_id,
        {
            "status": "SUCCESS",
            "message": "Success",
            "progress": 100,
            "finished_at": fin,
            "external_status_json": json.dumps(ext, ensure_ascii=False)[:65000],
        },
    )


def _deposit_skip_lending(payload: Dict[str, Any], account_mode: str) -> bool:
    ps = payload.get("permitSignature")
    if not isinstance(ps, dict) or not ps:
        return False
    return account_mode in ("evm", "mca")


def _deposit_prereq_timeout_sec(payload: Dict[str, Any]) -> int:
    """Zcash bridge deposits allow longer for 1Click / business signature polling."""
    if is_zcash_bridge_deposit_body(payload):
        return STEP_BRIDGE_SEC
    return STEP_GATE_SEC


def _deposit_prereq_timeout_fail(
    network_id: str,
    job_id: str,
    payload: Dict[str, Any],
    start: datetime,
    now: datetime,
) -> bool:
    """Gate for internal post-bridge prerequisites (e.g. MCA business signature).

    Measured from `start` (when this step was entered), NOT from job creation, so
    a long upstream bridge wait does not eat into this internal step's budget.
    """
    limit = _deposit_prereq_timeout_sec(payload)
    if (now - start).total_seconds() > limit:
        mins = max(1, limit // 60)
        _fail_job(
            network_id,
            job_id,
            "timeout waiting for deposit prerequisites ({0}m)".format(mins),
        )
        return True
    return False


def _oneclick_deposit_deadline(
    oc_data: Optional[Dict[str, Any]], row: Dict[str, Any]
) -> Optional[datetime]:
    """Authoritative deposit window from the 1Click quote.

    Prefers the live poll payload, then the stored snapshot. Uses
    `quote.timeWhenInactive` (when the deposit address stops accepting funds),
    falling back to `quote.deadline`.
    """
    candidates: List[Any] = []
    if isinstance(oc_data, dict):
        candidates.append(oc_data)
    snap = _ext(row).get("oneClickSnapshot")
    if isinstance(snap, str) and snap:
        try:
            candidates.append(json.loads(snap))
        except Exception:
            pass
    elif isinstance(snap, dict):
        candidates.append(snap)
    for data in candidates:
        if not isinstance(data, dict):
            continue
        qr = data.get("quoteResponse")
        quote = qr.get("quote") if isinstance(qr, dict) else None
        if not isinstance(quote, dict):
            continue
        for key in ("timeWhenInactive", "deadline"):
            dt = _parse_iso_dt(quote.get(key))
            if dt is not None:
                return dt
    return None


def _bridge_deposit_abscap_fail(
    network_id: str, job_id: str, created: datetime, now: datetime
) -> bool:
    """Absolute backstop so a stuck bridge deposit can never live forever."""
    if (now - created).total_seconds() > STEP_DEPOSIT_ABS_CAP_SEC:
        _fail_job(
            network_id, job_id, "timeout waiting for bridge deposit (absolute cap)"
        )
        return True
    return False


def _bridge_deposit_deadline_fail(
    network_id: str,
    job_id: str,
    oc_data: Optional[Dict[str, Any]],
    row: Dict[str, Any],
    created: datetime,
    now: datetime,
) -> bool:
    """Fail a still-pending bridge deposit only after the 1Click quote deadline
    (plus grace) has passed, or the absolute cap is hit.

    While the quote is still valid we defer to 1Click's own terminal status
    (SUCCESS / EXPIRED / REFUNDED), so legitimate slow deposits (e.g. BTC waiting
    for on-chain confirmations, up to the quote deadline) are never killed early.
    """
    if _bridge_deposit_abscap_fail(network_id, job_id, created, now):
        return True
    deadline = _oneclick_deposit_deadline(oc_data, row)
    if deadline is None:
        # No discoverable deadline yet: keep waiting; the absolute cap is the backstop.
        return False
    if now > deadline + timedelta(seconds=DEPOSIT_DEADLINE_GRACE_SEC):
        _fail_job(
            network_id,
            job_id,
            "timeout waiting for bridge deposit (past 1Click quote deadline)",
        )
        return True
    return False


def _refresh_zcash_signature_external_status(
    network_id: str,
    job_id: str,
    row: Dict[str, Any],
    signature_task: Dict[str, Any],
) -> None:
    """Merge Zcash business poll into externalStatus while bridge is still pending."""
    if not is_zcash_legacy_signature_task(signature_task):
        return
    zr = resolve_zcash_signature_task(network_id, signature_task)
    ext_patch = dict(zr.get("ext_patch") or {})
    existing_oc = (_ext(row).get("oneClickStatus") or "").strip()
    if existing_oc:
        ext_patch["oneClickStatus"] = existing_oc
    if not ext_patch:
        return
    _update_job(
        network_id,
        job_id,
        {"external_status_json": _merge_ext(row, ext_patch)},
        transfer_type="deposit",
    )


def _resolve_mca_b_path_rsv(
    network_id: str,
    signature_task: Dict[str, Any],
    batch_id: str,
    lending_rows: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve r/s/v for MCA B-path (batch, Zcash deposit address, or txHash)."""
    stask = signature_task if isinstance(signature_task, dict) else {}
    if is_zcash_legacy_signature_task(stask):
        zr = resolve_zcash_signature_task(network_id, stask)
        if zr.get("error_msg"):
            raise ValueError(str(zr["error_msg"]))
        return zr.get("rsv")
    rows = lending_rows or []
    if batch_id and not rows:
        rows = []
    return resolve_mca_evm_rsv(
        network_id,
        lending_rows=rows,
        signature_task=stask,
    )


def _wait_for_mca_signature(
    network_id: str,
    job_id: str,
    row: Dict[str, Any],
    signature_task: Dict[str, Any],
    batch_id: str,
    *,
    transfer_type: str,
    timeout_gate,
) -> bool:
    """
    Handle WAITING_SIGNATURE polling for MCA B-path.

    Returns True if caller should return early (pending or failed).
    """
    from db_provider import query_multichain_lending_data

    stask = signature_task if isinstance(signature_task, dict) else {}
    tx_hash_direct = (stask.get("txHash") or "").strip()
    zcash_deposit = (stask.get("zcashDepositAddress") or "").strip()

    if not batch_id and not tx_hash_direct and not zcash_deposit:
        _fail_job(
            network_id,
            job_id,
            "batch_id, signatureTask.txHash, or zcashDepositAddress is required for mca path",
        )
        return True

    if timeout_gate():
        return True

    if is_zcash_legacy_signature_task(stask):
        zr = resolve_zcash_signature_task(network_id, stask)
        ext_patch = zr.get("ext_patch") or {}
        if zr.get("error_msg"):
            _fail_job(network_id, job_id, str(zr["error_msg"]), ext_patch=ext_patch)
            return True
        if zr.get("pending"):
            sig_msg = (
                "Waiting for Zcash business signature"
                if is_zcash_legacy_signature_task(stask)
                else "Waiting for relayer signature"
            )
            _update_job(
                network_id,
                job_id,
                {
                    "status": "WAITING_SIGNATURE",
                    "message": sig_msg,
                    "external_status_json": _merge_ext(row, ext_patch),
                },
                transfer_type=transfer_type,
            )
            return True
        return False

    if batch_id:
        lending_rows = query_multichain_lending_data(network_id, batch_id) or []
        if not lending_rows:
            _fail_job(network_id, job_id, "multichain_lending_data empty for batch_id")
            return True
        relayer_err = lending_batch_error(lending_rows)
        if relayer_err:
            _fail_job(network_id, job_id, "relayer batch failed: {0}".format(relayer_err))
            return True
        if not lending_batch_complete(lending_rows):
            first = lending_rows[0] or {}
            bs_raw = first.get("batch_status")
            try:
                bs = int(bs_raw) if bs_raw is not None else -1
            except Exception:
                bs = -1
            _update_job(
                network_id,
                job_id,
                {
                    "status": "WAITING_SIGNATURE",
                    "message": "Waiting for relayer signature",
                    "external_status_json": _merge_ext(row, {"batchStatus": bs}),
                },
                transfer_type=transfer_type,
            )
            return True
        if not resolve_mca_evm_rsv(
            network_id,
            lending_rows=lending_rows,
            signature_task=stask,
        ):
            _update_job(
                network_id,
                job_id,
                {
                    "status": "WAITING_SIGNATURE",
                    "message": "Waiting for relayer signature",
                },
                transfer_type=transfer_type,
            )
            return True
        return False

    if tx_hash_direct:
        rsv_probe = resolve_mca_evm_rsv(
            network_id,
            lending_rows=[],
            signature_task=stask,
        )
        if not rsv_probe:
            _update_job(
                network_id,
                job_id,
                {
                    "status": "WAITING_SIGNATURE",
                    "message": "Waiting for relayer signature",
                },
                transfer_type=transfer_type,
            )
            return True
        return False

    return False


def process_deposit_row(network_id: str, row: Dict[str, Any]) -> None:
    from db_provider import (
        get_hyperliquid_transfer_job_by_job_id,
        query_multichain_lending_data,
    )

    job_id = str(row["job_id"])
    row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
    st = row.get("status") or ""
    if st in ("SUCCESS", "FAILED"):
        return

    payload = _loads_payload(row)
    q = payload.get("quote") or {}
    needs_bridge = bool(q.get("needsBridge"))
    deposit_address = (
        row.get("deposit_address")
        or (q.get("depositAddress") if isinstance(q, dict) else None)
        or ""
    )
    deposit_address = str(deposit_address).strip()
    batch_id = row.get("batch_id") or (payload.get("signatureTask") or {}).get("batchId")
    batch_id = str(batch_id).strip() if batch_id else ""

    account_mode = str(row.get("account_mode") or "").lower()
    hl_user = str(row.get("hyperliquid_user_address") or "").strip()
    skip_lending = _deposit_skip_lending(payload, account_mode)
    base = Cfg.HYPERLIQUID_PERMIT_API_BASE
    token = Cfg.HYPERLIQUID_USDC_TOKEN_ARBITRUM
    spender = Cfg.HYPERLIQUID_PERMIT_SPENDER

    created = _parse_dt(row.get("created_at")) or _utcnow()
    now = _utcnow()

    def _timeout_gate(msg: str) -> bool:
        if (now - created).total_seconds() > STEP_GATE_SEC:
            _fail_job(network_id, job_id, msg)
            return True
        return False

    def _deposit_sig_gate() -> bool:
        """Timeout gate for the post-bridge MCA business signature wait.

        Measured from when this step is first entered (recorded once in
        externalStatus), so a long upstream bridge wait does not eat into it.
        """
        nonlocal row
        ex = _ext(row)
        skey = "depositSigWaitStartedAt"
        if skey not in ex:
            ex[skey] = now.strftime("%Y-%m-%d %H:%M:%S")
            _update_job(
                network_id,
                job_id,
                {"external_status_json": json.dumps(ex, ensure_ascii=False)[:65000]},
            )
            row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
        s0 = _parse_dt(_ext(row).get(skey)) or now
        return _deposit_prereq_timeout_fail(network_id, job_id, payload, s0, now)

    permit_id = row.get("permit_id")
    if st == "WAITING_PERMIT" and permit_id:
        ex = _ext(row)
        ckey = "depositConfirmStartedAt"
        if ckey not in ex:
            ex[ckey] = now.strftime("%Y-%m-%d %H:%M:%S")
            _update_job(
                network_id,
                job_id,
                {"external_status_json": json.dumps(ex, ensure_ascii=False)[:65000]},
            )
            row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
            ex = _ext(row)
        c0 = _parse_dt(ex.get(ckey)) or now
        if (now - c0).total_seconds() > STEP_CONFIRM_SEC:
            _fail_job(network_id, job_id, "timeout waiting for deposit confirmation (5m)")
            return
        try:
            rec = get_permit_records(base, str(permit_id))
        except Exception:
            return
        deposit_hash = extract_deposit_hash_from_permit_records(rec)
        ext_patch: Dict[str, Any] = {"permitRecords": json.dumps(rec, default=str)[:4000]}
        if deposit_hash:
            ext_patch["depositHash"] = deposit_hash
        _update_job(
            network_id,
            job_id,
            {"external_status_json": _merge_ext(row, ext_patch)},
        )
        item = None
        data = rec.get("data")
        if isinstance(data, list) and data:
            item = data[0]
        hist_st = _format_history_status(item)
        if hist_st == "TRANSFER_SUCCESS":
            fin = _utcnow()
            _update_job(
                network_id,
                job_id,
                {
                    "status": "SUCCESS",
                    "message": "Success",
                    "progress": 100,
                    "finished_at": fin,
                    "last_error": None,
                },
            )
        elif hist_st in HL_TERMINAL_HISTORY and hist_st != "TRANSFER_SUCCESS":
            _fail_job(
                network_id,
                job_id,
                f"deposit history terminal: {hist_st}",
                ext_patch=ext_patch,
            )
        return

    if st in ("SUBMITTING_PERMIT",) and not permit_id:
        ps0 = _parse_dt(row.get("permit_submitted_at")) or created
        if (now - ps0).total_seconds() > STEP_PERMIT_SEC:
            _fail_job(network_id, job_id, "timeout submitting permit (5m)")
            return

    if st in (
        "SUBMITTED",
        "WAITING_BRIDGE",
        "WAITING_SIGNATURE",
        "SUBMITTING_PERMIT",
    ):
        if needs_bridge and deposit_address:
            # Pre-poll only the absolute backstop, so persistent poll failures
            # still get reaped. The real "still pending" decision is made after
            # polling, driven by the 1Click quote deadline (see below).
            if _bridge_deposit_abscap_fail(network_id, job_id, created, now):
                return
            try:
                oc_st, oc_data = poll_oneclick_status(deposit_address)
            except Exception:
                return
            snap = json.dumps(oc_data, ensure_ascii=False, default=str)[:65000]
            _update_job(
                network_id,
                job_id,
                {
                    "status": "WAITING_BRIDGE",
                    "message": "Waiting for bridge settlement",
                    "external_status_json": _merge_ext(
                        row,
                        {"oneClickStatus": oc_st, "oneClickSnapshot": snap[:5000]},
                    ),
                },
            )
            row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
            if oc_st in ("FAILED", "REFUNDED", "EXPIRED"):
                _fail_job(
                    network_id,
                    job_id,
                    f"1Click terminal status: {oc_st}",
                    ext_patch={"oneClickSnapshot": snap[:5000]},
                )
                return
            if oc_st != "SUCCESS":
                # Bridge still pending: defer to 1Click's own terminal status
                # while the quote is valid; only fail locally once we are past
                # the quote deadline (plus grace) or the absolute cap.
                if _bridge_deposit_deadline_fail(
                    network_id, job_id, oc_data, row, created, now
                ):
                    return
                if not skip_lending:
                    stask = payload.get("signatureTask") or {}
                    if isinstance(stask, dict):
                        _refresh_zcash_signature_external_status(
                            network_id, job_id, row, stask
                        )
                return
        elif needs_bridge and not deposit_address:
            _fail_job(network_id, job_id, "depositAddress missing for bridge deposit")
            return

        if not skip_lending:
            stask = payload.get("signatureTask") or {}
            if not isinstance(stask, dict):
                stask = {}
            if _wait_for_mca_signature(
                network_id,
                job_id,
                row,
                stask,
                batch_id,
                transfer_type="deposit",
                timeout_gate=_deposit_sig_gate,
            ):
                return

        row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
        st = row.get("status") or ""
        permit_id = row.get("permit_id")
        if st in ("FAILED", "SUCCESS"):
            return
        if permit_id or st == "WAITING_PERMIT":
            return

        _update_job(
            network_id,
            job_id,
            {
                "status": "SUBMITTING_PERMIT",
                "message": "Submitting permit",
                "permit_submitted_at": now,
            },
        )
        row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
        try:
            if skip_lending:
                permit_body = build_permit_body_from_deposit_payload(
                    payload, hl_user, token, spender
                )
            else:
                stask = payload.get("signatureTask") or {}
                if not isinstance(stask, dict):
                    stask = {}
                lending_rows: List[Dict[str, Any]] = []
                if batch_id and not is_zcash_legacy_signature_task(stask):
                    lending_rows = query_multichain_lending_data(network_id, batch_id) or []
                try:
                    rsv = _resolve_mca_b_path_rsv(
                        network_id,
                        stask,
                        batch_id,
                        lending_rows=lending_rows,
                    )
                except ValueError as e:
                    _fail_job(network_id, job_id, str(e))
                    return
                if not rsv:
                    _fail_job(network_id, job_id, "failed to resolve MCA permit signature from relayer")
                    return
                permit_request = payload.get("permitRequest")
                if not isinstance(permit_request, dict):
                    _fail_job(network_id, job_id, "permitRequest is required for mca deposit")
                    return
                permit_body = build_permit_body_from_mca_request(
                    permit_request,
                    rsv,
                    token,
                    spender,
                )
            result = post_permit(base, permit_body)
        except Exception as e:
            _fail_job(network_id, job_id, f"permit API error: {e}")
            return

        pid = result.get("data")
        if pid is None or pid == "":
            _fail_job(
                network_id,
                job_id,
                "permit response missing data (permit_id)",
                ext_patch={"permitResponse": str(result)[:2000]},
            )
            return
        ex2 = _ext(row)
        ex2["depositConfirmStartedAt"] = now.strftime("%Y-%m-%d %H:%M:%S")
        _update_job(
            network_id,
            job_id,
            {
                "status": "WAITING_PERMIT",
                "message": "Confirming deposit",
                "permit_id": str(pid),
                "external_status_json": json.dumps(
                    {**ex2, "permitResponse": json.dumps(result, default=str)[:4000]},
                    ensure_ascii=False,
                )[:65000],
            },
        )


def _normalize_sig_v(v: Any) -> int:
    if isinstance(v, bool):
        raise ValueError("v")
    if isinstance(v, int):
        return int(v)
    s = str(v).strip()
    if s.startswith("0x") or s.startswith("0X"):
        return int(s, 16)
    return int(s)


def process_withdraw_row(network_id: str, row: Dict[str, Any]) -> None:
    from db_provider import get_hyperliquid_transfer_job_by_job_id, query_multichain_lending_data

    job_id = str(row["job_id"])
    row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
    st = row.get("status") or ""
    if st in ("SUCCESS", "FAILED"):
        return

    payload = _loads_payload(row)
    account_mode = str(row.get("account_mode") or "").lower()
    hl_user = str(row.get("hyperliquid_user_address") or "").strip()
    q = payload.get("quote") or {}
    needs_bridge = bool(q.get("needsBridge"))
    deposit_address = (
        str(row.get("deposit_address") or q.get("depositAddress") or "").strip()
    )
    batch_id = str(
        row.get("batch_id") or (payload.get("signatureTask") or {}).get("batchId") or ""
    ).strip()
    wa = payload.get("withdrawAction") or {}
    if not isinstance(wa, dict):
        _fail_job(network_id, job_id, "invalid withdrawAction")
        return

    info_url = Cfg.HYPERLIQUID_MAINNET_INFO_URL
    ex_url = Cfg.HYPERLIQUID_MAINNET_EXCHANGE_URL

    created = _parse_dt(row.get("created_at")) or _utcnow()
    now = _utcnow()
    ext = _ext(row)

    def _gate_timeout(msg: str) -> bool:
        if (now - created).total_seconds() > STEP_GATE_SEC:
            _fail_job(network_id, job_id, msg)
            return True
        return False

    if st in ("SUBMITTED", "WAITING_SIGNATURE"):
        stask = payload.get("signatureTask") or {}
        sig = payload.get("signature")
        has_frontend_sig = isinstance(sig, dict) and bool(sig.get("r"))
        if has_frontend_sig:
            _update_job(
                network_id,
                job_id,
                {"status": "SUBMITTING_EXCHANGE", "message": "Submitting withdrawal"},
            )
        elif account_mode == "mca":
            stask = payload.get("signatureTask") or {}
            if not isinstance(stask, dict):
                stask = {}
            tx_hash_direct = (stask.get("txHash") or "").strip()
            zcash_deposit = (stask.get("zcashDepositAddress") or "").strip()
            if batch_id or tx_hash_direct or zcash_deposit:
                if _wait_for_mca_signature(
                    network_id,
                    job_id,
                    row,
                    stask,
                    batch_id,
                    transfer_type="withdrawal",
                    timeout_gate=lambda: _gate_timeout(
                        "timeout waiting for withdraw signature (5m)"
                    ),
                ):
                    return
                lending_rows: List[Dict[str, Any]] = []
                if batch_id and not is_zcash_legacy_signature_task(stask):
                    lending_rows = query_multichain_lending_data(network_id, batch_id) or []
                try:
                    rsv = _resolve_mca_b_path_rsv(
                        network_id,
                        stask,
                        batch_id,
                        lending_rows=lending_rows,
                    )
                except ValueError as e:
                    _fail_job(network_id, job_id, str(e))
                    return
                if not rsv:
                    _update_job(
                        network_id,
                        job_id,
                        {
                            "status": "WAITING_SIGNATURE",
                            "message": "Waiting for relayer signature",
                        },
                        transfer_type="withdrawal",
                    )
                    return
                ext = _ext(row)
                ext["withdrawSignature"] = rsv
                if is_zcash_legacy_signature_task(stask):
                    zr = resolve_zcash_signature_task(network_id, stask)
                    zpatch = zr.get("ext_patch") or {}
                    if zpatch:
                        ext.update(zpatch)
                _update_job(
                    network_id,
                    job_id,
                    {
                        "external_status_json": json.dumps(ext, ensure_ascii=False)[:65000],
                        "status": "SUBMITTING_EXCHANGE",
                        "message": "Submitting withdrawal",
                    },
                    transfer_type="withdrawal",
                )
            else:
                _update_job(
                    network_id,
                    job_id,
                    {"status": "SUBMITTING_EXCHANGE", "message": "Submitting withdrawal"},
                    transfer_type="withdrawal",
                )
        row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
        st = row.get("status") or ""

    if st == "SUBMITTING_EXCHANGE":
        ext = _ext(row)
        try:
            nonce = int(wa.get("time"))
        except Exception:
            _fail_job(network_id, job_id, "withdrawAction.time must be integer nonce")
            return

        sig = ext.get("withdrawSignature") or payload.get("signature")
        if not isinstance(sig, dict) or not sig.get("r"):
            _fail_job(network_id, job_id, "missing exchange signature")
            return
        try:
            vv = _normalize_sig_v(sig.get("v"))
        except Exception:
            _fail_job(network_id, job_id, "invalid signature.v")
            return
        body = {
            "action": wa,
            "nonce": nonce,
            "signature": {
                "r": str(sig.get("r")),
                "s": str(sig.get("s")),
                "v": vv,
            },
            "vaultAddress": None,
            "expiresAfter": None,
        }
        try:
            ex_resp = _hl_exchange_post(ex_url, body)
        except Exception as e:
            _fail_job(network_id, job_id, f"Hyperliquid exchange error: {e}")
            return

        ex = _ext(row)
        ex["withdrawExchangePostedNonce"] = nonce
        ex["exchangeResponse"] = json.dumps(ex_resp, default=str)[:4000]
        t0 = int(wa.get("time") or nonce) - 120_000
        ex["ledgerStartTime"] = t0
        _update_job(
            network_id,
            job_id,
            {
                "status": "WAITING_LEDGER",
                "message": "Waiting for ledger confirmation",
                "exchange_submitted_at": now,
                "external_status_json": json.dumps(ex, ensure_ascii=False)[:65000],
            },
        )
        row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
        st = row.get("status") or ""

    if st == "WAITING_LEDGER":
        ext = _ext(row)
        t0 = int(ext.get("ledgerStartTime") or 0)
        es = _parse_dt(row.get("exchange_submitted_at")) or now
        ledger_timed_out = (now - es).total_seconds() > STEP_LEDGER_SEC

        updates = None
        ledger_err = None
        try:
            updates = _hl_info_post(
                info_url,
                {"type": "userNonFundingLedgerUpdates", "user": hl_user, "startTime": t0},
            )
        except Exception as e:
            ledger_err = str(e)[:500]

        withdraw_hash = None
        if updates is not None:
            withdraw_hash = extract_withdraw_hash_from_ledger(updates, wa.get("time"))
            if withdraw_hash:
                ext["withdrawHashSource"] = "hyperliquid_ledger"

        if not withdraw_hash and needs_bridge and deposit_address:
            wh_oc, oc_st, oc_patch = _poll_withdraw_hash_via_oneclick(deposit_address, ext)
            ext.update(oc_patch)
            if wh_oc:
                withdraw_hash = wh_oc
            elif oc_st in ("FAILED", "REFUNDED", "EXPIRED"):
                _fail_job(
                    network_id,
                    job_id,
                    f"bridge 1Click terminal: {oc_st}",
                    ext_patch=ext,
                )
                return

        if ledger_err:
            ext["ledgerPollError"] = ledger_err

        if withdraw_hash:
            _advance_withdraw_after_hash(
                network_id,
                job_id,
                withdraw_hash=withdraw_hash,
                updates=updates,
                ext=ext,
                needs_bridge=needs_bridge,
                deposit_address=deposit_address,
            )
            row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
            st = row.get("status") or ""
        elif ledger_timed_out:
            if needs_bridge and deposit_address:
                wh_oc, oc_st, oc_patch = _poll_withdraw_hash_via_oneclick(deposit_address, ext)
                ext.update(oc_patch)
                if wh_oc and oc_st == "SUCCESS":
                    _advance_withdraw_after_hash(
                        network_id,
                        job_id,
                        withdraw_hash=wh_oc,
                        updates=updates,
                        ext=ext,
                        needs_bridge=needs_bridge,
                        deposit_address=deposit_address,
                    )
                    row = get_hyperliquid_transfer_job_by_job_id(network_id, job_id) or row
                    st = row.get("status") or ""
                else:
                    _fail_job(
                        network_id,
                        job_id,
                        "timeout waiting for ledger withdraw (5m)",
                        ext_patch=ext,
                    )
                    return
            else:
                _fail_job(
                    network_id,
                    job_id,
                    "timeout waiting for ledger withdraw (5m)",
                    ext_patch=ext,
                )
                return
        else:
            if updates is not None and not _has_withdraw_ledger(updates):
                _update_job(
                    network_id,
                    job_id,
                    {
                        "external_status_json": json.dumps(ext, ensure_ascii=False)[:65000],
                    },
                )
            return

    if st == "WAITING_BRIDGE":
        if not deposit_address:
            _fail_job(network_id, job_id, "depositAddress missing for bridge")
            return
        bs = _parse_dt(row.get("exchange_submitted_at")) or created
        if (now - bs).total_seconds() > STEP_BRIDGE_SEC:
            _fail_job(network_id, job_id, "timeout waiting for bridge after withdraw (10m)")
            return
        try:
            oc_st, oc_data = poll_oneclick_status(deposit_address)
        except Exception:
            return
        ex = _ext(row)
        ex["bridgeOneClick"] = oc_st
        _update_job(
            network_id,
            job_id,
            {
                "external_status_json": json.dumps(ex, ensure_ascii=False)[:65000],
            },
        )
        if oc_st in ("FAILED", "REFUNDED", "EXPIRED"):
            _fail_job(network_id, job_id, f"bridge 1Click terminal: {oc_st}")
            return
        if oc_st == "SUCCESS":
            fin = _utcnow()
            _update_job(
                network_id,
                job_id,
                {
                    "status": "SUCCESS",
                    "message": "Success",
                    "progress": 100,
                    "finished_at": fin,
                },
            )


def run_worker(network_id: str) -> None:
    from db_provider import ensure_hyperliquid_transfer_jobs_table, fetch_hyperliquid_transfer_jobs_active

    ensure_hyperliquid_transfer_jobs_table(network_id)
    rows = fetch_hyperliquid_transfer_jobs_active(network_id, limit=50)
    if not rows:
        return
    print(f"[hyperliquid_perps_worker] processing {len(rows)} job(s)")
    for row in rows:
        try:
            t = (row.get("transfer_type") or "").lower()
            if t == "deposit":
                process_deposit_row(network_id, row)
            elif t == "withdrawal":
                process_withdraw_row(network_id, row)
        except Exception as e:
            print(f"[hyperliquid_perps_worker] job {row.get('job_id')} error: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python hyperliquid_perps_worker.py MAINNET|TESTNET|DEVNET")
        sys.exit(1)
    nid = str(sys.argv[1]).upper()
    if nid not in ("MAINNET", "TESTNET", "DEVNET"):
        print("Invalid NETWORK_ID")
        sys.exit(1)
    print(f"--- hyperliquid_perps_worker ({nid}) ---")
    run_worker(nid)
    print("--- done ---")
