"""
MCA withdraw async orchestration — enqueue via /api/swap/swap, process via cron worker.

Uses table swap_mca_withdraw_jobs (see migrations/2026_05_12_swap_mca_withdraw_jobs.sql).
"""

from __future__ import annotations

import sys
import json
from typing import Any, Dict, Optional

from loguru import logger

sys.path.append('../')
from db_provider import (
    insert_swap_mca_withdraw_job,
    select_swap_mca_withdraw_job_by_client_request,
    select_swap_mca_withdraw_job_by_id,
)

_INTENTS_TERMINAL_GOOD = {"SUCCESS"}
_INTENTS_TERMINAL_BAD = {"FAILED", "REFUNDED", "EXPIRED"}
_INTENTS_TERMINAL_SKIP = {"TIMEOUT"}


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def enqueue_mca_withdraw_orchestration_job(network_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    body shape (recommended under POST /api/swap/swap as `mcaWithdrawOrchestration`):
    {
      "clientRequestId": "...",         // optional, idempotent
      "mcaRelayer": {                   // forwarded to multichain_lending_requests
        "mcaAccountId", "wallet", "request": [...],
        "page_display_data"|"pageDisplayData" optional
      },
      "afterRelayer": {                  // optional; worker runs after lending batch succeeds
        "intentsPoll": {"depositAddress": "...", "maxPolls": 200},    // poll 1Click /status (optional router hint unused)
        "unifiedSwap": { ...same keys as POST /api/swap/swap body... , no mcaRelayer }
      }
    }
    """
    if not isinstance(body, dict):
        return {"code": -1, "msg": "mcaWithdrawOrchestration must be an object", "data": None}

    mr = body.get("mcaRelayer")
    if not isinstance(mr, dict) or not mr:
        return {"code": -1, "msg": "mcaWithdrawOrchestration.mcaRelayer is required", "data": None}

    try:
        from mca_relayer_payload import canonicalize_mca_relayer_block

        mr = canonicalize_mca_relayer_block(mr)
    except ValueError as ve:
        return {"code": -1, "msg": str(ve), "data": None}

    if not mr.get("mcaAccountId") and not mr.get("mca_id"):
        return {"code": -1, "msg": "mcaRelayer.mcaAccountId is required", "data": None}
    if not mr.get("wallet"):
        return {"code": -1, "msg": "mcaRelayer.wallet is required", "data": None}
    req = mr.get("request")
    if not isinstance(req, list) or not req:
        return {"code": -1, "msg": "mcaRelayer.request must be a non-empty array", "data": None}

    cid = body.get("clientRequestId") or body.get("client_request_id")
    if cid:
        existing = select_swap_mca_withdraw_job_by_client_request(network_id, str(cid))
        if existing:
            return {
                "code": 0,
                "msg": "success",
                "data": {
                    "jobId": existing["id"],
                    "status": existing["status"],
                    "batchId": existing.get("batch_id"),
                    "deduplicated": True,
                    "pollPath": "/api/swap/mca-withdraw-jobs",
                },
            }

    after = body.get("afterRelayer") or body.get("after_relayer")
    try:
        jid = insert_swap_mca_withdraw_job(
            network_id,
            str(cid).strip() if cid else None,
            _json_dumps(mr),
            _json_dumps(after) if after is not None else None,
        )
    except Exception as e:
        if cid and ("Duplicate entry" in str(e) or "1062" in str(e)):
            existing = select_swap_mca_withdraw_job_by_client_request(network_id, str(cid))
            if existing:
                return {
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "jobId": existing["id"],
                        "status": existing["status"],
                        "deduplicated": True,
                        "pollPath": "/api/swap/mca-withdraw-jobs",
                    },
                }
        logger.exception(f"enqueue_mca_withdraw_orchestration_job: {e}")
        return {"code": -1, "msg": str(e), "data": None}

    return {
        "code": 0,
        "msg": "accepted",
        "data": {
            "jobId": jid,
            "status": "queued",
            "pollPath": "/api/swap/mca-withdraw-jobs",
            "note": "Process with cron: job/mca_withdraw_orchestration_worker.py MAINNET",
        },
    }


def serialize_job_public(row: Optional[Dict]) -> Optional[Dict]:
    """Strip internal blobs for JWT clients; frontend polls this shape."""
    if not row:
        return None

    def _loads(s, default=None):
        if s is None or s == "":
            return default
        try:
            return json.loads(s)
        except Exception:
            return default

    out = {
        "jobId": row.get("id"),
        "status": row.get("status"),
        "clientRequestId": row.get("client_request_id"),
        "batchId": row.get("batch_id"),
        "relayerSummary": _loads(row.get("relayer_result_summary")),
        "bridgeSwap": _loads(row.get("bridge_swap_result_json")),
        "intentsStatus": _loads(row.get("intents_status_snapshot")),
        "intentsPollCount": row.get("intents_poll_count"),
        "attempts": row.get("attempts"),
        "lastError": row.get("last_error"),
        "createdAt": str(row.get("created_at")) if row.get("created_at") else None,
        "updatedAt": str(row.get("updated_at")) if row.get("updated_at") else None,
    }
    terminal = row.get("status") in (
        "done",
        "relayer_failed",
        "follow_up_failed",
        "swap_build_ready",
        "intents_terminal_success",
        "intents_terminal_failed",
        "partial_failed",
        "failed",
    )
    out["terminal"] = bool(terminal)

    hints = []
    if out["status"] == "swap_build_ready" and isinstance(out["bridgeSwap"], dict):
        hints.append("Sign and broadcast deposit tx client-side using bridgeSwap.data (same as synchronous /swap).")
    if out["status"] == "intents_terminal_failed":
        hints.append("Relayer succeeded but 1Click reported a terminal failure — manual check recommended.")
    if hints:
        out["hints"] = hints
    return out


def public_job(network_id: str, job_id) -> Dict[str, Any]:
    row = select_swap_mca_withdraw_job_by_id(network_id, job_id)
    if not row:
        return {"code": -1, "msg": "job not found", "data": None}
    return {"code": 0, "msg": "success", "data": serialize_job_public(row)}


def process_mca_withdraw_jobs_once(network_id: str, *, max_jobs: int = 25) -> int:
    """
    Advance queued / relayer_waiting / follow_up jobs (called from cron worker).
    Returns number of job rows touched.
    """
    from db_provider import (
        add_multichain_lending_requests,
        fetch_swap_mca_withdraw_jobs_active,
        query_multichain_lending_data,
        update_swap_mca_withdraw_job_row,
    )

    touched = 0
    rows = fetch_swap_mca_withdraw_jobs_active(network_id, limit=max_jobs)

    for job in rows:
        jid = int(job["id"])
        try:
            update_swap_mca_withdraw_job_row(
                network_id,
                jid,
                fields={
                    "attempts": int(job.get("attempts") or 0) + 1,
                },
            )
            touched += 1
            status = job.get("status")

            if status == "queued":
                _wj_process_queued(network_id, job)
            elif status == "relayer_waiting":
                _wj_process_relayer_waiting(network_id, job)
            elif status == "follow_up":
                _wj_process_follow_up(network_id, job)
        except Exception as e:
            logger.exception(f"MCA job {jid} worker error: {e}")
            update_swap_mca_withdraw_job_row(
                network_id,
                jid,
                fields={"status": "failed", "last_error": str(e)[:2048]},
            )

    return touched


def _wj_process_queued(network_id: str, job: Dict) -> None:
    from db_provider import add_multichain_lending_requests, update_swap_mca_withdraw_job_row

    jid = int(job["id"])
    mr = json.loads(job["mca_relayer_json"])
    mca_id = mr.get("mcaAccountId") or mr.get("mca_id")
    wallet = mr.get("wallet")
    requests_list = mr.get("request")
    page_display_data = str(mr.get("page_display_data") or mr.get("pageDisplayData") or "")

    batch_id = add_multichain_lending_requests(network_id, mca_id, wallet, requests_list, page_display_data)
    update_swap_mca_withdraw_job_row(
        network_id,
        jid,
        fields={
            "status": "relayer_waiting",
            "batch_id": str(batch_id),
            "last_error": None,
        },
    )


def _wj_relayer_rows_ok(rows) -> Dict[str, Any]:
    """Return {ok bool, summary dict, error str}. Mirrors frontend polling."""
    from mca_relayer_payload import summarize_multichain_lending_batch

    s = summarize_multichain_lending_batch(rows or [])
    if s.get("error") == "no multichain_lending rows yet":
        return {"ok": False, "summary": {}, "error": s["error"]}
    if s.get("pending"):
        return {"ok": False, "summary": {"pending": True}, "error": ""}
    if not s.get("success"):
        return {
            "ok": False,
            "summary": {"tx_hashes": s.get("tx_hashes") or []},
            "error": s.get("error") or "relayer batch failed",
        }
    return {"ok": True, "summary": {"tx_hashes": s.get("tx_hashes") or []}, "error": ""}


def _wj_process_relayer_waiting(network_id: str, job: Dict) -> None:
    from db_provider import update_swap_mca_withdraw_job_row
    from db_provider import query_multichain_lending_data

    jid = int(job["id"])
    bid = job.get("batch_id")
    if not bid:
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={"status": "relayer_failed", "last_error": "missing batch_id"},
        )
        return

    rows = query_multichain_lending_data(network_id, bid) or []

    chk = _wj_relayer_rows_ok(rows)
    if not chk["ok"] and chk.get("summary", {}).get("pending"):
        return

    if not chk["ok"]:
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={
                "status": "relayer_failed",
                "relayer_result_summary": _json_dumps(chk.get("summary") or {}),
                "last_error": chk.get("error") or "relayer batch failed",
            },
        )
        return

    after_raw = job.get("after_relayer_json")
    after = json.loads(after_raw) if after_raw else {}
    summary = chk.get("summary") or {}

    if not after:
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={
                "status": "done",
                "relayer_result_summary": _json_dumps(summary),
                "last_error": None,
            },
        )
        return

    update_swap_mca_withdraw_job_row(
        network_id,
        jid,
        fields={
            "status": "follow_up",
            "relayer_result_summary": _json_dumps(summary),
            "last_error": None,
        },
    )


def _wj_process_follow_up(network_id: str, job: Dict) -> None:
    from db_provider import update_swap_mca_withdraw_job_row
    from unified_swap import unified_swap

    jid = int(job["id"])
    after_raw = job.get("after_relayer_json")
    after = json.loads(after_raw) if after_raw else {}

    ip = after.get("intentsPoll") or {}
    dp = ip.get("depositAddress") or ip.get("deposit_address")
    if dp:
        _wj_follow_intents_poll(network_id, job, str(dp))
        return

    us = after.get("unifiedSwap") or {}
    if not isinstance(us, dict) or not us.get("router"):
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={
                "status": "done",
                "last_error": "afterRelayer has no intentsPoll or unifiedSwap.router — nothing to run",
            },
        )
        return

    swap_res = unified_swap(
        from_chain=us.get("fromChain", us.get("chainId", "")),
        to_chain=us.get("toChain", us.get("fromChain", us.get("chainId", ""))),
        token_in_address=us.get("tokenIn", ""),
        token_out_address=us.get("tokenOut", ""),
        amount_in=str(us.get("amountIn", "")),
        slippage=float(us.get("slippage", 0.5)),
        sender=us.get("sender", ""),
        recipient=us.get("recipient", ""),
        router=us.get("router", ""),
        market=us.get("market", ""),
        quote_expected_out=str(us.get("expectedOut", us.get("quoteExpectedOut", ""))),
        quote_min_amount_out=str(us.get("minAmountOut", us.get("quoteMinAmountOut", ""))),
        pre_swap=us.get("preSwap") if isinstance(us.get("preSwap"), dict) else None,
        bridge=us.get("bridge") if isinstance(us.get("bridge"), dict) else None,
        mca_relayer=None,
        mca_oneclick=us.get("mcaOneclick") or us.get("mca"),
    )

    if swap_res.get("code") != 0:
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={
                "status": "follow_up_failed",
                "bridge_swap_result_json": _json_dumps(swap_res),
                "last_error": str(swap_res.get("msg", "swap build failed")),
            },
        )
        return

    update_swap_mca_withdraw_job_row(
        network_id,
        jid,
        fields={
            "status": "swap_build_ready",
            "bridge_swap_result_json": _json_dumps(swap_res),
            "last_error": None,
        },
    )


def _wj_follow_intents_poll(network_id: str, job: Dict, deposit_address: str) -> None:
    """One 1Click status poll per cron tick (avoid long blocking)."""
    from nearintents_utils import nearintents_order_status
    from db_provider import update_swap_mca_withdraw_job_row

    jid = int(job["id"])
    max_poll = 200
    try:
        raw_max = json.loads(job.get("after_relayer_json") or "{}").get("intentsPoll") or {}
        max_poll = int(raw_max.get("maxPolls") or raw_max.get("max_polls") or 200)
    except Exception:
        pass

    cnt = int(job.get("intents_poll_count") or 0)
    if cnt >= max_poll:
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={
                "status": "partial_failed",
                "last_error": f"intentsPoll exceeds maxPolls ({max_poll})",
            },
        )
        return

    st = nearintents_order_status(deposit_address)
    snap = st.get("data") if isinstance(st.get("data"), dict) else {}
    snap_json = _json_dumps({"query": deposit_address, "response": snap, "upstreamOk": st.get("success")})

    raw_status = ""
    try:
        raw_status = str(snap.get("status") or snap.get("state") or "").upper()
    except Exception:
        raw_status = ""

    next_cnt = cnt + 1
    terminal_good = raw_status and raw_status in {s.upper() for s in _INTENTS_TERMINAL_GOOD}
    terminal_bad = raw_status and raw_status in {s.upper() for s in _INTENTS_TERMINAL_BAD}
    terminal_extra = raw_status and raw_status in {s.upper() for s in _INTENTS_TERMINAL_SKIP}

    if terminal_good:
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={
                "status": "done",
                "intents_status_snapshot": snap_json,
                "intents_poll_count": next_cnt,
                "last_error": None,
            },
        )
        return

    if terminal_bad or terminal_extra:
        st_label = "intents_terminal_failed" if terminal_bad else "partial_failed"
        update_swap_mca_withdraw_job_row(
            network_id,
            jid,
            fields={
                "status": st_label,
                "intents_status_snapshot": snap_json,
                "intents_poll_count": next_cnt,
                "last_error": raw_status or "intents terminal",
            },
        )
        return

    update_swap_mca_withdraw_job_row(
        network_id,
        jid,
        fields={
            "intents_status_snapshot": snap_json,
            "intents_poll_count": next_cnt,
        },
    )


if __name__ == '__main__':
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            print(f"--- mca_withdraw_orchestration_worker ({network_id}) ---")
            n = process_mca_withdraw_jobs_once(network_id)
            print(f"touched_jobs={n}")
            print("--- done ---")
        else:
            print("Error: network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error: must put NETWORK_ID as arg")
        exit(1)
