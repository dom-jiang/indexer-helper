#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
HyperLiquid deposit orchestration worker (cron).

Advances rows in `hyperliquid_deposit_orders`:
  1) Until 1Click /status = SUCCESS and multichain_lending first row batch_status = 2
  2) POST /v3/arb/permit
  3) Poll /v3/arb/permit/records until terminal

Usage: python hyperliquid_deposit_worker.py MAINNET
"""

import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests

sys.path.append("../")
from config import Cfg

ONECLICK_STATUS_URL = "https://1click.chaindefuser.com/v0/status"
STEP1_DEADLINE_SEC = 300
STEP_PERMIT_SEC = 300
STEP_CONFIRM_SEC = 300

HL_TERMINAL_HISTORY = frozenset({"TRANSFER_SUCCESS", "FAILED", "TRANSFER_FAILED"})

HL_Failed = 91
HL_Success = 7


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


def _fail(
    network_id: str,
    hl_id: int,
    msg: str,
    *,
    snap: Optional[str] = None,
    snap_col: str = "oneclick_status_snapshot",
) -> None:
    from db_provider import update_hyperliquid_deposit_order

    fields: Dict[str, Any] = {
        "hl_status": HL_Failed,
        "hl_status_text": "FAILED",
        "error_message": (msg or "")[:4000],
    }
    if snap is not None:
        fields[snap_col] = snap[:65000]
    update_hyperliquid_deposit_order(network_id, hl_id, fields)


def _format_history_status(item: Any) -> str:
    if not item:
        return "PENDING_DEPOSIT"
    st = item.get("status")
    if st == "success":
        return "TRANSFER_SUCCESS"
    if st in ("failed", "error"):
        return "FAILED"
    if st == "refunded":
        return "FAILED"
    if st == "bridged":
        return "WAITING_FOR_TRANSFER"
    if st == "signing":
        return "TRANSFERING"
    if item.get("permit_id") and st == "init":
        return "WAITING_FOR_TRANSFER"
    if st == "permit_failed":
        return "TRANSFER_FAILED"
    return "PENDING_DEPOSIT"


def parse_lending_request_cell(raw) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def build_permit_body_from_lending_first_row(
    first_row: Dict[str, Any], mca_mapped_evm: str, token: str, default_spender: str
) -> Dict[str, Any]:
    """
    Mirrors multi-chain-lending `submitHyperliquidDepositPermit` body.
    Accepts flat keys, nested `permitSignature`, or top-level `signatureParts` (EVM deposit).
    """
    req = parse_lending_request_cell(first_row.get("request"))
    if not req:
        raise ValueError("lending row request JSON is empty")

    def _norm_v(v):
        if v is None:
            raise ValueError("permit v missing")
        if isinstance(v, bool):
            raise ValueError("invalid permit v")
        if isinstance(v, int):
            return v
        s = str(v).strip()
        if s.startswith("0x") or s.startswith("0X"):
            return int(s, 16)
        return int(s)

    owner = (mca_mapped_evm or "").strip()
    if not owner:
        raise ValueError("mca_mapped_evm_account is empty")

    if isinstance(req.get("permitSignature"), dict):
        ps = req["permitSignature"]
        sp = ps.get("signatureParts") or {}
        body = {
            "deadline": str(ps.get("deadline", "")),
            "owner": owner,
            "r": str(sp.get("r", "")),
            "s": str(sp.get("s", "")),
            "spender": str(ps.get("spender") or default_spender),
            "token": str(token),
            "v": _norm_v(sp.get("v")),
            "value": str(ps.get("value", "")),
        }
    elif isinstance(req.get("signatureParts"), dict):
        sp = req["signatureParts"]
        body = {
            "deadline": str(req.get("deadline", "")),
            "owner": owner,
            "r": str(sp.get("r", "")),
            "s": str(sp.get("s", "")),
            "spender": str(req.get("spender") or default_spender),
            "token": str(req.get("token") or token),
            "v": _norm_v(sp.get("v")),
            "value": str(req.get("value", "")),
        }
    elif all(k in req for k in ("deadline", "r", "s", "v", "value")):
        body = {
            "deadline": str(req.get("deadline", "")),
            "owner": owner,
            "r": str(req.get("r", "")),
            "s": str(req.get("s", "")),
            "spender": str(req.get("spender") or default_spender),
            "token": str(req.get("token") or token),
            "v": _norm_v(req.get("v")),
            "value": str(req.get("value", "")),
        }
    else:
        raise ValueError("lending request JSON missing permit fields")

    for k in ("deadline", "r", "s", "value"):
        if not body.get(k):
            raise ValueError(f"permit field {k} is empty")
    return body


def poll_oneclick_status(deposit_address: str) -> Tuple[str, Dict[str, Any]]:
    r = requests.get(
        ONECLICK_STATUS_URL,
        params={"depositAddress": deposit_address},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    st = (data.get("status") or "").strip().upper()
    return st, data


def extract_origin_tx_hash_from_oneclick(data: Dict[str, Any]) -> Optional[str]:
    """First origin-chain tx hash from 1Click status `swapDetails` (e.g. HL withdraw on Arbitrum)."""
    sd = data.get("swapDetails") if isinstance(data.get("swapDetails"), dict) else {}
    for item in sd.get("originChainTxHashes") or []:
        if isinstance(item, dict):
            h = item.get("hash")
            if h:
                return str(h).strip()
    return None


def post_permit(base: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = base.rstrip("/") + "/v3/arb/permit"
    r = requests.post(url, json=body, headers={"Content-Type": "application/json"}, timeout=45)
    text = r.text
    try:
        js = r.json()
    except Exception:
        js = {"raw": text}
    if not r.ok:
        raise RuntimeError(js.get("msg") or js.get("message") or text[:2000])
    return js


def get_permit_records(base: str, permit_id: str) -> Dict[str, Any]:
    url = base.rstrip("/") + "/v3/arb/permit/records"
    r = requests.get(url, params={"permit_id": str(permit_id)}, timeout=25)
    try:
        js = r.json()
    except Exception:
        js = {}
    if not r.ok:
        raise RuntimeError(js.get("msg") or js.get("message") or r.text[:2000])
    return js


def refresh_granted_row(network_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
    from db_provider import get_hyperliquid_deposit_order_by_id

    return get_hyperliquid_deposit_order_by_id(network_id, row["id"]) or row


def process_hyperliquid_deposit_row(network_id: str, row: Dict[str, Any]) -> None:
    from db_provider import (
        query_multichain_lending_data,
        update_hyperliquid_deposit_order,
    )

    hid = int(row["id"])
    st = int(row.get("hl_status") or 0)
    if st in (HL_Success, HL_Failed):
        return

    base = Cfg.HYPERLIQUID_PERMIT_API_BASE
    token = Cfg.HYPERLIQUID_USDC_TOKEN_ARBITRUM
    spender = Cfg.HYPERLIQUID_PERMIT_SPENDER

    created = _parse_dt(row.get("created_at")) or _utcnow()
    now = _utcnow()

    if st < 5:
        if (now - created).total_seconds() > STEP1_DEADLINE_SEC:
            _fail(network_id, hid, "timeout waiting for transfer and relayer signature (5m)")
            return

        try:
            oc_st, oc_data = poll_oneclick_status(row["deposit_address"])
        except Exception:
            return

        snap = json.dumps(oc_data, ensure_ascii=False, default=str)
        update_hyperliquid_deposit_order(
            network_id,
            hid,
            {"oneclick_status_snapshot": snap[:65000]},
        )

        if oc_st in ("FAILED", "REFUNDED", "EXPIRED"):
            _fail(network_id, hid, f"1Click terminal status: {oc_st}", snap=snap)
            return

        oneclick_ok = oc_st == "SUCCESS"

        try:
            lending_rows = query_multichain_lending_data(network_id, str(row["batch_id"])) or []
        except Exception:
            return

        if not lending_rows:
            _fail(network_id, hid, "multichain_lending_data empty for batch_id")
            return

        first = lending_rows[0] or {}
        bs_raw = first.get("batch_status")
        if bs_raw is None:
            _fail(network_id, hid, "batch_status missing on multichain_lending row")
            return

        try:
            bs = int(bs_raw)
        except Exception:
            _fail(network_id, hid, f"invalid batch_status: {bs_raw!r}")
            return

        lending_ok = bs == 2

        if not oneclick_ok:
            update_hyperliquid_deposit_order(
                network_id,
                hid,
                {"hl_status": 1, "hl_status_text": "TRANSFER_PENDING"},
            )
            return

        if oneclick_ok and not lending_ok:
            sub = 3 if bs in (0, 1) else 2
            txt = "SIGNATURE_PENDING" if bs in (0, 1) else "TRANSFER_CONFIRMED"
            update_hyperliquid_deposit_order(
                network_id,
                hid,
                {"hl_status": sub, "hl_status_text": txt},
            )
            return

        update_hyperliquid_deposit_order(
            network_id,
            hid,
            {"hl_status": 4, "hl_status_text": "SIGNATURE_READY"},
        )
        row = refresh_granted_row(network_id, row)

    permit_id = row.get("permit_id")
    st = int(row.get("hl_status") or 0)

    if not permit_id and st in (4, 5):
        ps = _parse_dt(row.get("permit_started_at"))
        if ps and (now - ps).total_seconds() > STEP_PERMIT_SEC:
            _fail(network_id, hid, "timeout submitting permit (5m)", snap_col="permit_response_snapshot")
            return

    if not permit_id:
        try:
            lending_rows = query_multichain_lending_data(network_id, str(row["batch_id"])) or []
            if not lending_rows:
                _fail(network_id, hid, "multichain_lending_data empty before permit")
                return
            body = build_permit_body_from_lending_first_row(
                lending_rows[0],
                row["mca_mapped_evm_account"],
                token,
                spender,
            )
        except Exception as e:
            _fail(network_id, hid, f"build permit body: {e}")
            return

        update_hyperliquid_deposit_order(
            network_id,
            hid,
            {
                "hl_status": 5,
                "hl_status_text": "PERMIT_SUBMITTING",
                "permit_started_at": now,
            },
        )
        try:
            result = post_permit(base, body)
        except Exception as e:
            _fail(
                network_id,
                hid,
                f"permit API error: {e}",
                snap=json.dumps({"err": str(e)}, ensure_ascii=False)[:65000],
                snap_col="permit_response_snapshot",
            )
            return

        pid = result.get("data")
        if pid is None or pid == "":
            _fail(
                network_id,
                hid,
                "permit response missing data (permit_id)",
                snap=json.dumps(result, ensure_ascii=False)[:65000],
                snap_col="permit_response_snapshot",
            )
            return

        update_hyperliquid_deposit_order(
            network_id,
            hid,
            {
                "hl_status": 6,
                "hl_status_text": "DEPOSIT_CONFIRMING",
                "permit_id": str(pid),
                "permit_response_snapshot": json.dumps(result, ensure_ascii=False, default=str)[:65000],
                "confirm_started_at": _utcnow(),
            },
        )
        row = refresh_granted_row(network_id, row)

    st = int(row.get("hl_status") or 0)
    permit_id = row.get("permit_id")
    if st == 6 and permit_id:
        c0 = _parse_dt(row.get("confirm_started_at")) or now
        if (now - c0).total_seconds() > STEP_CONFIRM_SEC:
            _fail(network_id, hid, "timeout waiting for deposit confirmation (5m)")
            return

        try:
            rec = get_permit_records(base, str(permit_id))
        except Exception:
            return

        update_hyperliquid_deposit_order(
            network_id,
            hid,
            {"records_snapshot": json.dumps(rec, ensure_ascii=False, default=str)[:65000]},
        )
        item = None
        data = rec.get("data")
        if isinstance(data, list) and data:
            item = data[0]
        hist_st = _format_history_status(item)
        if hist_st == "TRANSFER_SUCCESS":
            update_hyperliquid_deposit_order(
                network_id,
                hid,
                {
                    "hl_status": HL_Success,
                    "hl_status_text": "SUCCESS",
                    "error_message": None,
                },
            )
        elif hist_st in HL_TERMINAL_HISTORY and hist_st != "TRANSFER_SUCCESS":
            _fail(
                network_id,
                hid,
                f"deposit history terminal: {hist_st}",
                snap=json.dumps(rec, ensure_ascii=False)[:65000],
                snap_col="records_snapshot",
            )


def run_worker(network_id: str) -> None:
    from db_provider import fetch_hyperliquid_deposit_orders_active

    rows = fetch_hyperliquid_deposit_orders_active(network_id, limit=40)
    if not rows:
        return
    print(f"[hyperliquid_deposit_worker] processing {len(rows)} row(s)")
    for row in rows:
        try:
            process_hyperliquid_deposit_row(network_id, row)
        except Exception as e:
            print(f"[hyperliquid_deposit_worker] row {row.get('id')} error: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python hyperliquid_deposit_worker.py MAINNET|TESTNET|DEVNET")
        sys.exit(1)
    nid = str(sys.argv[1]).upper()
    if nid not in ("MAINNET", "TESTNET", "DEVNET"):
        print("Invalid NETWORK_ID")
        sys.exit(1)
    print(f"--- hyperliquid_deposit_worker ({nid}) ---")
    run_worker(nid)
    print("--- done ---")
