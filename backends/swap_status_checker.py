"""
Background job: poll 1Click / OmniBridge status for pending cross-chain swap
transactions reported via POST /api/swap/report, and update swap_transactions.

Scope:
  - Only rows with is_cross_chain = 1
  - Only non-terminal statuses (not SUCCESS / FAILED / REFUNDED / EXPIRED)
  - Only rows within the last 70 minutes (to bound job runtime)

Scheduling: run every minute via cron.
"""

import sys
sys.path.append('../')

import json
import time
from loguru import logger

from config import Cfg
from db_provider import get_pending_cross_chain_swaps, update_swap_transaction
from nearintents_utils import nearintents_order_status
from omnibridge_utils import omni_get_order_status


TERMINAL_STATUSES = {"SUCCESS", "FAILED", "REFUNDED", "EXPIRED"}


def _extract_to_hash(router: str, status_data: dict) -> str:
    """Pull the best-effort destination chain tx hash from a status payload."""
    if not isinstance(status_data, dict):
        return ""
    if router == "nearintents":
        swap = status_data.get("swapDetails") or {}
        hashes = swap.get("destinationChainTxHashes") or []
        if hashes and isinstance(hashes, list):
            first = hashes[0]
            if isinstance(first, dict):
                return first.get("hash", "")
            if isinstance(first, str):
                return first
    if router == "omnibridge":
        return status_data.get("destinationTxHash") or status_data.get("toHash") or ""
    return ""


def _extract_actual_out(router: str, status_data: dict) -> str:
    if not isinstance(status_data, dict):
        return ""
    if router == "nearintents":
        swap = status_data.get("swapDetails") or {}
        return swap.get("amountOut") or ""
    if router == "omnibridge":
        return status_data.get("receivedAmount") or status_data.get("actualOut") or ""
    return ""


def _query_status(router: str, deposit_address: str) -> dict:
    if router == "nearintents":
        return nearintents_order_status(deposit_address)
    if router == "omnibridge":
        return omni_get_order_status(deposit_address)
    return {"success": False, "error": f"Unsupported router: {router}"}


def check_once(network_id: str) -> int:
    rows = get_pending_cross_chain_swaps(network_id, interval_minutes=70)
    if not rows:
        return 0

    updated = 0
    for row in rows:
        rec_id = row.get("id")
        router = (row.get("router") or "").lower()
        deposit_address = row.get("deposit_address") or ""
        old_status = (row.get("status") or "PENDING").upper()

        if not deposit_address or not router:
            continue

        result = _query_status(router, deposit_address)
        if not result.get("success"):
            logger.warning(
                f"[swap_status_checker] status query failed id={rec_id} router={router} "
                f"deposit={deposit_address} err={result.get('error')}"
            )
            continue

        data = result.get("data", {}) or {}
        new_status_raw = data.get("status") or data.get("state") or ""
        new_status = str(new_status_raw).upper() if new_status_raw else ""

        if not new_status or new_status == old_status:
            continue

        to_hash = _extract_to_hash(router, data)
        actual_out = _extract_actual_out(router, data)

        update_kwargs = {
            "status": new_status,
            "status_response": data,
        }
        if to_hash:
            update_kwargs["to_hash"] = to_hash
        if actual_out:
            update_kwargs["actual_out"] = str(actual_out)

        try:
            update_swap_transaction(network_id, rec_id, **update_kwargs)
            updated += 1
            logger.info(
                f"[swap_status_checker] updated id={rec_id} {old_status}->{new_status} "
                f"to_hash={to_hash} actual_out={actual_out}"
            )
        except Exception as e:
            logger.error(f"[swap_status_checker] update failed id={rec_id}: {e}")

    return updated


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python swap_status_checker.py <NETWORK_ID>")
        sys.exit(1)

    network_id = str(sys.argv[1]).upper()
    if network_id not in ("MAINNET", "TESTNET", "DEVNET"):
        print("Error, network_id should be MAINNET, TESTNET or DEVNET")
        sys.exit(1)

    start = time.time()
    try:
        n = check_once(network_id)
        logger.info(f"[swap_status_checker] cycle done, updated={n}, elapsed={time.time() - start:.2f}s")
    except Exception as e:
        logger.error(f"[swap_status_checker] fatal: {e}")
        sys.exit(2)
