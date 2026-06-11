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
from decimal import Decimal, InvalidOperation
from loguru import logger

from config import Cfg
from db_provider import (
    get_pending_cross_chain_swaps,
    update_swap_transaction,
    insert_boss_app_fee_ledger,
)
from nearintents_utils import nearintents_order_status
from omnibridge_utils import omni_get_order_status


TERMINAL_STATUSES = {"SUCCESS", "FAILED", "REFUNDED", "EXPIRED"}

# Routers whose cross-chain phase is operated by 1Click and therefore share the
# nearintents status payload shape. `preswap-nearintents` is the two-stage
# (OKX preswap + 1Click bridge) route — the cross-chain leg is still 1Click,
# so its status payload looks identical to a direct nearintents order.
_NEARINTENTS_LIKE_ROUTERS = {"nearintents", "preswap-nearintents"}


def _extract_to_hash(router: str, status_data: dict) -> str:
    """Pull the best-effort destination chain tx hash from a status payload."""
    if not isinstance(status_data, dict):
        return ""
    if router in _NEARINTENTS_LIKE_ROUTERS:
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


def _extract_from_hash(router: str, status_data: dict) -> str:
    """Pull the best-effort origin (source) chain tx hash from a status payload.

    Used to backfill the synthetic `mca_relayer:{batch_id}` placeholder once the
    relayer has broadcast and 1Click reports the real origin tx.
    """
    if not isinstance(status_data, dict):
        return ""
    if router in _NEARINTENTS_LIKE_ROUTERS:
        swap = status_data.get("swapDetails") or {}
        hashes = swap.get("originChainTxHashes") or []
        if hashes and isinstance(hashes, list):
            first = hashes[0]
            if isinstance(first, dict) and first.get("hash"):
                return first.get("hash", "")
            if isinstance(first, str) and first:
                return first
        near_hashes = swap.get("nearTxHashes") or []
        if near_hashes and isinstance(near_hashes, list) and isinstance(near_hashes[0], str):
            return near_hashes[0]
    if router == "omnibridge":
        return status_data.get("originTxHash") or status_data.get("fromHash") or ""
    return ""


def _extract_actual_out(router: str, status_data: dict) -> str:
    if not isinstance(status_data, dict):
        return ""
    if router in _NEARINTENTS_LIKE_ROUTERS:
        swap = status_data.get("swapDetails") or {}
        return swap.get("amountOut") or ""
    if router == "omnibridge":
        return status_data.get("receivedAmount") or status_data.get("actualOut") or ""
    return ""


def _extract_bridge_amount_in(router: str, status_data: dict) -> str:
    if not isinstance(status_data, dict):
        return ""
    if router in _NEARINTENTS_LIKE_ROUTERS:
        swap = status_data.get("swapDetails") or {}
        for key in ("amountIn", "originAmount", "depositAmount"):
            v = swap.get(key)
            if v not in (None, ""):
                return str(v)
    if router == "omnibridge":
        return str(status_data.get("amountIn") or status_data.get("sentAmount") or "")
    return ""


def _extract_fee_asset(router: str, status_data: dict) -> dict:
    if not isinstance(status_data, dict):
        return {}
    if router in _NEARINTENTS_LIKE_ROUTERS:
        swap = status_data.get("swapDetails") or {}
        token = swap.get("originToken") or {}
        return {
            "asset": str(swap.get("originAsset") or swap.get("tokenIn") or ""),
            "symbol": str(token.get("symbol") or ""),
            "decimals": token.get("decimals"),
        }
    return {}


def _extract_boss_fee_meta(row: dict) -> dict:
    raw = row.get("extra")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        return {}
    meta = raw.get("bossFeeMeta") if isinstance(raw.get("bossFeeMeta"), dict) else {}
    return meta if isinstance(meta, dict) else {}


def _to_int(val, default=0):
    try:
        return int(str(val))
    except Exception:
        return int(default)


def _as_decimal_ratio(val, default: Decimal) -> Decimal:
    try:
        d = Decimal(str(val))
        if d <= 0 or d >= 1:
            return default
        return d
    except (InvalidOperation, ValueError, TypeError):
        return default


def _try_accrue_boss_fee(network_id: str, row: dict, router: str, status_data: dict) -> None:
    meta = _extract_boss_fee_meta(row)
    app_id = str(meta.get("bossAppId") or "").strip()
    if not app_id:
        return

    app_fee_bps = max(0, _to_int(meta.get("appFeeBps"), 0))
    platform_bps = max(0, _to_int(meta.get("platformBaseFeeBps"), 0))
    total_fee_bps = max(0, _to_int(meta.get("totalFeeBps"), platform_bps + app_fee_bps))
    bridge_amount_in = max(0, _to_int(_extract_bridge_amount_in(router, status_data), 0))
    if total_fee_bps <= 0 or bridge_amount_in <= 0:
        return

    total_fee_amount = bridge_amount_in * total_fee_bps // 10000
    app_component_amount = bridge_amount_in * app_fee_bps // 10000
    partner_ratio = _as_decimal_ratio(meta.get("partnerShareRatio"), Decimal("0.8"))
    partner_fee_amount = int((Decimal(app_component_amount) * partner_ratio).to_integral_value())
    partner_fee_amount = max(0, min(partner_fee_amount, total_fee_amount))
    platform_fee_amount = max(0, total_fee_amount - partner_fee_amount)
    fee_asset = _extract_fee_asset(router, status_data)

    try:
        ledger_id = insert_boss_app_fee_ledger(
            network_id,
            swap_transaction_id=row.get("id"),
            app_id=app_id,
            boss_user_id=meta.get("bossUserId"),
            from_hash=row.get("from_hash"),
            deposit_address=row.get("deposit_address"),
            router=router,
            fee_token_asset=fee_asset.get("asset"),
            fee_token_symbol=fee_asset.get("symbol"),
            fee_token_decimals=fee_asset.get("decimals"),
            bridge_amount_in=str(bridge_amount_in),
            total_fee_bps=int(total_fee_bps),
            app_fee_bps=int(app_fee_bps),
            platform_base_fee_bps=int(platform_bps),
            total_fee_amount=str(total_fee_amount),
            partner_fee_amount=str(partner_fee_amount),
            platform_fee_amount=str(platform_fee_amount),
            status_response=status_data,
        )
        logger.info(
            "[swap_status_checker] fee ledger upserted id={} swap_id={} app_id={} total_fee_amount={}",
            ledger_id,
            row.get("id"),
            app_id,
            total_fee_amount,
        )
    except Exception as e:
        logger.warning(
            "[swap_status_checker] fee ledger insert skipped swap_id={} app_id={} err={}",
            row.get("id"),
            app_id,
            e,
        )


def _query_status(router: str, deposit_address: str) -> dict:
    # preswap-nearintents shares 1Click status semantics (see _NEARINTENTS_LIKE_ROUTERS).
    if router in _NEARINTENTS_LIKE_ROUTERS:
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

        if new_status == "SUCCESS":
            _try_accrue_boss_fee(network_id, row, router, data)

        # Backfill the synthetic `mca_relayer:{batch_id}` placeholder with the real
        # origin tx hash once available. Done as a separate, best-effort update so a
        # uk_from_hash conflict can never roll back the status update above. Only
        # relayer-enqueued rows are touched (avoids clobbering reported from_hash).
        if str(row.get("from_hash") or "").startswith("mca_relayer:"):
            real_from_hash = _extract_from_hash(router, data)
            if real_from_hash:
                try:
                    update_swap_transaction(network_id, rec_id, from_hash=real_from_hash)
                    logger.info(
                        f"[swap_status_checker] backfilled from_hash id={rec_id} "
                        f"-> {real_from_hash}"
                    )
                except Exception as e:
                    logger.warning(
                        f"[swap_status_checker] from_hash backfill skipped id={rec_id}: {e}"
                    )

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
