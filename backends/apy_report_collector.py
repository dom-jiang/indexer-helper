#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Collect daily APY report rows and upsert into DB.

Usage:
  python apy_report_collector.py MAINNET
  python apy_report_collector.py MAINNET 2026-06-04
"""

from __future__ import annotations

import json
import requests
import sys
from datetime import datetime, timedelta

sys.path.append("../")

from config import Cfg
from db_provider import (
    ensure_apy_daily_reports_table,
    upsert_apy_daily_report,
)

APY_LOOKBACK_DAYS = 30
RPC_TIMEOUT_SECONDS = 20


def _resolve_rnear_contract() -> str:
    if Cfg.LST_CONTRACT_ID:
        return str(Cfg.LST_CONTRACT_ID).strip()
    return "lst.rhealab.near"


def _calc_apy(new_price: str, old_price: str, day_number: int = 1) -> str:
    apy = ((int(new_price) / int(old_price)) ** (365 / day_number) - 1) * 100
    return "{:.6f}".format(apy)


def _rpc_post(payload):
    url = Cfg.LST_RPC
    if not url:
        print("empty LST_RPC")
        return None
    try:
        response = requests.post(url, json=payload, timeout=RPC_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"rpc request failed: {e}")
        return None


def _get_latest_block():
    payload = {
        "jsonrpc": "2.0",
        "id": "apy_latest_block",
        "method": "block",
        "params": {"finality": "final"},
    }
    data = _rpc_post(payload)
    if not data or "result" not in data:
        return None, None
    header = data["result"].get("header", {})
    return header.get("height"), header.get("timestamp")


def _get_block_timestamp_ns(block_height: int, cache: dict) -> int | None:
    if block_height in cache:
        return cache[block_height]
    payload = {
        "jsonrpc": "2.0",
        "id": "apy_block_by_height",
        "method": "block",
        "params": {"block_id": int(block_height)},
    }
    data = _rpc_post(payload)
    if not data or "result" not in data:
        return None
    ts = data["result"].get("header", {}).get("timestamp")
    if ts is None:
        return None
    cache[block_height] = int(ts)
    return int(ts)


def _find_block_at_or_before(target_ts_ns: int, latest_height: int, latest_ts_ns: int, cache: dict) -> int | None:
    if target_ts_ns >= latest_ts_ns:
        return int(latest_height)

    lo, hi = 1, int(latest_height)
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        ts = _get_block_timestamp_ns(mid, cache)
        if ts is None:
            return None
        if ts <= target_ts_ns:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def _query_contract_price_at_block(contract_id: str, method_name: str, block_height: int) -> str | None:
    payload = {
        "jsonrpc": "2.0",
        "id": "apy_query_price",
        "method": "query",
        "params": {
            "request_type": "call_function",
            "account_id": contract_id,
            "method_name": method_name,
            "args_base64": "e30=",
            "block_id": int(block_height),
        },
    }
    data = _rpc_post(payload)
    if not data or "result" not in data:
        return None
    try:
        raw = data["result"]["result"]
        return json.loads("".join([chr(x) for x in raw]))
    except Exception as e:
        print(f"decode price failed ({contract_id}.{method_name}@{block_height}): {e}")
        return None


def _validate_report_date(report_date: str) -> bool:
    try:
        datetime.strptime(report_date, "%Y-%m-%d")
        return True
    except Exception as e:
        print(f"invalid report_date format ({report_date}), expected YYYY-MM-DD: {e}")
        return False


def collect_apy_daily(network_id: str, report_date: str) -> int:
    ensure_apy_daily_reports_table(network_id)

    latest_height, latest_ts_ns = _get_latest_block()
    if latest_height is None or latest_ts_ns is None:
        print("failed to resolve latest block")
        return 0

    if not _validate_report_date(report_date):
        return 0

    ts_cache = {}
    new_height = int(latest_height)
    target_new_ts_ns = int(latest_ts_ns)
    target_old_ts_ns = target_new_ts_ns - int(timedelta(days=APY_LOOKBACK_DAYS).total_seconds() * 1_000_000_000)
    old_height = _find_block_at_or_before(target_old_ts_ns, latest_height, latest_ts_ns, ts_cache)
    if new_height is None or old_height is None:
        print("failed to resolve target block heights")
        return 0
    print(
        f"report_date={report_date}, lookback_days={APY_LOOKBACK_DAYS}, "
        f"new_ts_ns={target_new_ts_ns}, old_ts_ns={target_old_ts_ns}, "
        f"new_height={new_height}, old_height={old_height}"
    )

    token_specs = [
        ("rnear", _resolve_rnear_contract(), "ft_price"),
        ("linear", "linear-protocol.near", "ft_price"),
        ("stnear", "meta-pool.near", "get_st_near_price"),
    ]

    success = 0
    for token, contract_id, method_name in token_specs:
        new_price = _query_contract_price_at_block(contract_id, method_name, new_height)
        old_price = _query_contract_price_at_block(contract_id, method_name, old_height)
        if new_price is None or old_price is None:
            print(f"[{token}] failed to fetch historical prices from {contract_id}.{method_name}")
            continue
        try:
            apy = _calc_apy(new_price, old_price, APY_LOOKBACK_DAYS)
        except Exception as e:
            print(f"[{token}] failed to compute apy: {e}")
            continue

        ok = upsert_apy_daily_report(
            network_id=network_id,
            report_date=report_date,
            token=token,
            contract_id=contract_id,
            apy=apy,
        )
        if ok:
            success += 1
            print(f"[{token}] APY={apy} saved for {report_date}")
        else:
            print(f"[{token}] failed to save APY={apy}")

    return success


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: must put NETWORK_ID as arg")
        sys.exit(1)

    network_id = str(sys.argv[1]).upper()
    if network_id not in ["MAINNET", "TESTNET", "DEVNET"]:
        print("Error: network_id should be MAINNET, TESTNET or DEVNET")
        sys.exit(1)

    if len(sys.argv) >= 3:
        report_date = sys.argv[2]
    else:
        report_date = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"--- apy_report_collector start ({network_id}, {report_date}) ---")
    affected = collect_apy_daily(network_id, report_date)
    print(f"--- apy_report_collector done, affected={affected} ---")
