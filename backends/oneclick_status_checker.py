#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
1Click Swap Order Status Checker (cron script)

Polls pending oneclick_orders (created within the last hour, not in terminal state),
queries 1Click /v0/status for each, and updates the DB when status changes.

Usage: python oneclick_status_checker.py MAINNET
Recommended cron: * * * * * /path/to/backend_oneclick_status_checker.sh
"""

import sys
import json
import requests

sys.path.append('../')
from db_provider import get_pending_oneclick_orders, update_oneclick_order_status

ONECLICK_STATUS_URL = "https://1click.chaindefuser.com/v0/status"


def check_order_status(network_id):
    pending_orders = get_pending_oneclick_orders(network_id)
    if not pending_orders:
        return

    print(f"Found {len(pending_orders)} pending orders to check")

    for order in pending_orders:
        order_id = order["id"]
        deposit_address = order["deposit_address"]
        old_status = order["status"]

        try:
            resp = requests.get(
                ONECLICK_STATUS_URL,
                params={"depositAddress": deposit_address},
                timeout=15
            )
            resp.raise_for_status()
            status_data = resp.json()
        except Exception as e:
            print(f"  Order {order_id}: failed to query status for {deposit_address}: {e}")
            continue

        new_status = status_data.get("status", "")
        if not new_status:
            print(f"  Order {order_id}: empty status returned, skipping")
            continue

        if new_status != old_status:
            try:
                update_oneclick_order_status(
                    network_id, order_id, new_status, json.dumps(status_data)
                )
                print(f"  Order {order_id}: {old_status} -> {new_status}")
            except Exception as e:
                print(f"  Order {order_id}: failed to update status: {e}")
        else:
            print(f"  Order {order_id}: status unchanged ({old_status})")


if __name__ == '__main__':
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            print(f"--- oneclick_status_checker start ({network_id}) ---")
            check_order_status(network_id)
            print("--- oneclick_status_checker done ---")
        else:
            print("Error: network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error: must put NETWORK_ID as arg")
        exit(1)
