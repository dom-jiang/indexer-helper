#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Collect daily APY report rows and upsert into DB.

Usage:
  python apy_report_collector.py MAINNET
  python apy_report_collector.py MAINNET 2026-06-04
"""

from __future__ import annotations

import sys
from datetime import datetime

sys.path.append("../")

from config import Cfg
from db_provider import (
    ensure_apy_daily_reports_table,
    upsert_apy_daily_report,
)
from utils import get_staking_token_price


def _resolve_rnear_contract() -> str:
    if Cfg.LST_CONTRACT_ID:
        return str(Cfg.LST_CONTRACT_ID).strip()
    return "lst.rhealab.near"


def _calc_apy(new_price: str, old_price: str, day_number: int = 1) -> str:
    apy = (int(new_price) - int(old_price)) / (int(old_price) / (10 ** 24)) / (10 ** 24) / day_number * 365 * 100
    return "{:.6f}".format(apy)


def collect_apy_daily(network_id: str, report_date: str) -> int:
    ensure_apy_daily_reports_table(network_id)

    token_specs = [
        ("rnear", _resolve_rnear_contract(), "ft_price"),
        ("linear", "linear-protocol.near", "ft_price"),
        ("stnear", "meta-pool.near", "get_st_near_price"),
    ]

    success = 0
    for token, contract_id, method_name in token_specs:
        price_result = get_staking_token_price(1, contract_id, method_name)
        if price_result is None:
            print(f"[{token}] failed to fetch price from {contract_id}.{method_name}")
            continue
        new_price, old_price = price_result
        try:
            apy = _calc_apy(new_price, old_price, 1)
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
