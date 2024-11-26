import sys
sys.path.append('../')
import requests
from config import Cfg
import json
import time
import base64
from loguru import logger
from near_db_provider import add_limit_order_log, add_limit_order_swap_log, \
    add_burrow_event_log, add_swap_log, add_swap, add_swap_desire, add_liquidity_added, add_liquidity_removed, \
    add_lostfound, add_order_added, add_order_cancelled, add_order_completed, add_claim_charged_fee, \
    add_account_not_registered_logs, add_liquidity_pools, add_liquidity_log, add_xref_log, add_farm_log, \
    add_withdraw_reward_data


def get_near_transaction_data(network_id, start_id):
    args = "/api/v1/near/%s/getTransactions?id=%s&limit=20" % (network_id, start_id)
    try:
        headers = {
            'AccessKey': Cfg.DB3_ACCESS_KEY
        }
        response = requests.get(Cfg.DB3_URL + args, headers=headers)
        ret_data = response.text
        near_transaction_data = json.loads(ret_data)
        if near_transaction_data["returnCode"] != 20000:
            print("db3 transaction list error:", near_transaction_data)
        else:
            transaction_data_list = near_transaction_data["data"]["transactions"]
            start_id = handel_transaction_data(transaction_data_list, start_id)
    except Exception as e:
        print("get_near_transaction_data error: ", e)
    return start_id


def handel_transaction_data(transaction_data_list, start_id):
    xref_data_list = []
    swap_data_list = []
    liquidity_data_list = []
    farm_data_list = []
    burrow_date_list = []
    not_registered_data_list = []
    liquidity_pools_list = []
    withdraw_reward_insert_data = []
    for transaction_data in transaction_data_list:
        data_id = transaction_data["ID"]
        data_block_number = transaction_data["block_number"]
        if int(data_id) >= start_id:
            start_id = int(data_id) + 1
        tx_hash = transaction_data["tx_hash"]
        tx_time = transaction_data["tx_time"]
        logs = transaction_data["logs"]
        receiver_id = transaction_data["receiver_id"]
        receipt_id = transaction_data["receipt_id"]
        receipt = transaction_data["receipt"]
        predecessor_id = transaction_data["predecessor_id"]
        receipt_status = transaction_data["receipt_status"]
        if '"Success' in receipt_status:
            handle_log_content(receipt_id, data_block_number, predecessor_id, receiver_id, logs, tx_time,
                               receipt, xref_data_list, swap_data_list, liquidity_data_list,
                               farm_data_list)
            handle_limit_order_content(data_block_number, receipt_id, logs, tx_time, receipt, network_id)
            handle_withdraw_reward_content(receipt_id, data_block_number, logs, tx_time, withdraw_reward_insert_data)
            handle_not_registered_logs_content(logs, receipt, tx_hash, receipt_id, tx_time, data_block_number,
                                               receiver_id, not_registered_data_list)
        if "contract.main.burrow.near" == receiver_id and '"Success' in receipt_status:
            handle_burrow_log(logs, receipt_id, data_block_number, tx_time, predecessor_id, burrow_date_list,
                              receipt)
        if "dclv2.ref-labs.near" == receiver_id and '"Success' in receipt_status:
            handle_dcl_log(logs, receipt_id, data_block_number, tx_time, network_id, receipt, predecessor_id, receiver_id)
        if receiver_id == "v2.ref-finance.near" and '"Success' in receipt_status:
            handle_liquidity_pools_content(receipt, predecessor_id, receipt_id, liquidity_pools_list)
    if len(swap_data_list) > 0:
        add_swap_log(swap_data_list, network_id)
    if len(liquidity_data_list) > 0:
        add_liquidity_log(liquidity_data_list, network_id)
    if len(xref_data_list) > 0:
        add_xref_log(xref_data_list, network_id)
    if len(farm_data_list) > 0:
        add_farm_log(farm_data_list, network_id)
    if len(burrow_date_list) > 0:
        add_burrow_event_log(burrow_date_list, network_id)
    if len(not_registered_data_list) > 0:
        add_account_not_registered_logs(not_registered_data_list, network_id)
    if len(liquidity_pools_list) > 0:
        add_liquidity_pools(liquidity_pools_list, network_id)
    if len(withdraw_reward_insert_data) > 0:
        add_withdraw_reward_data(withdraw_reward_insert_data, network_id)
    return start_id


def handle_liquidity_pools_content(receipt, predecessor_id, receipt_id, liquidity_pools_list):
    liquidity_pools_args = handle_liquidity_pools_receipt(receipt)
    for args in liquidity_pools_args:
        if "pool_id" in args:
            liquidity_pools_data = {
                "pool_id": args["pool_id"],
                "account_id": predecessor_id,
                "receipt_id": receipt_id,
            }
            liquidity_pools_list.append(liquidity_pools_data)


def handle_liquidity_pools_receipt(content):
    res = []
    try:
        actions = content["Action"]["actions"]
        for action in actions:
            if 'FunctionCall' in action:
                if "method_name" in action["FunctionCall"]:
                    method_name = action["FunctionCall"]["method_name"]
                    if method_name == "add_liquidity" or method_name == "add_stable_liquidity":
                        args = json.loads(base64.b64decode(action["FunctionCall"]["args"]))
                        res.append(args)
    except Exception as e:
        logger.error("handle_liquidity_pools_receipt error:{}", e)
        res = res
    return res


def handle_latest_actions_content(receipt, predecessor_id, receipt_id, timestamp, receiver_id, status,
                                  latest_actions_list, tx_hash):
    latest_actions_args = handle_latest_actions_receipt(receipt)
    for args in latest_actions_args:
        latest_actions_data = {
            "timestamp": timestamp,
            "tx_id": tx_hash,
            "receiver_account_id": receiver_id,
            "method_name": args["method_name"],
            "args": args["args"],
            "deposit": args["deposit"],
            "status": to_under_line(status),
            "predecessor_account_id": predecessor_id,
            "receiver_id": args["receipt_receiver_id"],
            "receipt_id": receipt_id,
        }
        if ((latest_actions_data["receiver_account_id"] == "v2.ref-finance.near"
             or latest_actions_data["receiver_account_id"] == "v2.ref-farming.near"
             or latest_actions_data["receiver_account_id"] == "xtoken.ref-finance.near"
             or latest_actions_data["receiver_account_id"] == "wrap.near"
             or latest_actions_data["receiver_account_id"] == "boostfarm.ref-labs.near"
             or latest_actions_data["receiver_account_id"] == "usn"
             or latest_actions_data["receiver_account_id"] == "dclv2.ref-labs.near")
            or ((latest_actions_data["receiver_id"] == "aurora" or latest_actions_data["receiver_id"] == "usn")
                and latest_actions_data["method_name"] == "ft_transfer_call")
            or (latest_actions_data["receiver_account_id"] == "aurora"
                and latest_actions_data["method_name"] == "call")
            or (latest_actions_data["receiver_id"] == "v2.ref-finance.near"
                or latest_actions_data["receiver_id"] == "xtoken.ref-finance.near"
                or latest_actions_data["receiver_id"] == "dclv2.ref-labs.near")) \
                or ((latest_actions_data["receiver_account_id"] == "asset-manager.orderly-network.near"
                     or latest_actions_data["receiver_id"] == "asset-manager.orderly-network.near")
                    and (latest_actions_data["method_name"] == "user_request_withdraw"
                         or latest_actions_data["method_name"] == "storage_deposit"
                         or latest_actions_data["method_name"] == "ft_transfer_call"
                         or latest_actions_data["method_name"] == "user_deposit_native_token"
                         or latest_actions_data["method_name"] == "user_request_settlement")) \
                or ((latest_actions_data["receiver_account_id"] == "contract.main.burrow.near"
                     or latest_actions_data["receiver_id"] == "contract.main.burrow.near")
                    and (latest_actions_data["method_name"] == "ft_transfer_call"
                         or latest_actions_data["method_name"] == "ft_on_transfer"
                         or latest_actions_data["method_name"] == "ft_resolve_transfer"
                         or latest_actions_data["method_name"] == "execute"
                         or latest_actions_data["method_name"] == "oracle_call"
                         or latest_actions_data["method_name"] == "oracle_on_call"
                         or latest_actions_data["method_name"] == "ft_transfer"
                         or latest_actions_data["method_name"] == "after_ft_transfer"
                         or latest_actions_data["method_name"] == "account_farm_claim_all")):
            latest_actions_list.append(latest_actions_data)


def handle_latest_actions_receipt(content):
    action_list = []
    try:
        actions = content["Action"]["actions"]
        for action in actions:
            action_data = {
                "method_name": "",
                "args": "",
                "deposit": "",
                "receipt_receiver_id": "",
            }
            if 'FunctionCall' in action:
                try:
                    if "args" in action["FunctionCall"]:
                        args = json.loads(base64.b64decode(action["FunctionCall"]["args"]))
                        action_data["args"] = json.dumps(args)
                        if "receiver_id" in args:
                            action_data["receipt_receiver_id"] = args["receiver_id"]
                except Exception as ee:
                    logger.error("handle_latest_actions_receipt FunctionCall error:{}", ee)
                    action_data["args"] = ""
                if "method_name" in action["FunctionCall"]:
                    action_data["method_name"] = action["FunctionCall"]["method_name"]
                if "deposit" in action["FunctionCall"]:
                    action_data["deposit"] = action["FunctionCall"]["deposit"]
            action_list.append(action_data)
    except Exception as e:
        logger.error("handle_latest_actions_receipt error:{}", e)
        action_list = action_list
    return action_list


def handle_limit_order_content(block_id, receipt_id, logs, timestamp, receipt, network_id):
    limit_order_args = handle_limit_order_receipt_content(receipt)
    for args in limit_order_args:
        if "LimitOrderWithSwap" in args:
            try:
                args_data = json.loads(args)
                msg = json.loads(args_data["msg"])
                limit_order_with_swap = msg["LimitOrderWithSwap"]
                handle_limit_order_log_content(logs, block_id, receipt_id, timestamp, network_id, limit_order_with_swap)
            except Exception as e:
                logger.error("handle_limit_order_content error:{}", e)
                continue


def handle_limit_order_receipt_content(content):
    res = []
    try:
        actions = content["Action"]["actions"]
        for action in actions:
            if 'FunctionCall' in action:
                try:
                    args = json.dumps(json.loads(base64.b64decode(action["FunctionCall"]["args"])))
                    res.append(args)
                except Exception as ee:
                    logger.error("handle_limit_order_receipt_content error:{}", ee)
                    continue
    except Exception as e:
        logger.error("handle_limit_order_receipt_content analysis error:{}", e)
    return res


def handle_limit_order_log_content(logs, block_id, receipt_id, timestamp, network, limit_order_with_swap):
    limit_order_data_list = []
    limit_order_swap_data_list = []
    for log in logs:
        if not log.startswith("EVENT_JSON:"):
            continue
        try:
            parsed_log = json.loads(log[len("EVENT_JSON:"):])
        except json.JSONDecodeError:
            logger.error("Error during parsing logs from JSON string to dict")
            continue
        handle_limit_order_log(parsed_log, block_id, receipt_id, timestamp, limit_order_with_swap,
                               limit_order_data_list, limit_order_swap_data_list)
    if len(limit_order_data_list) > 0:
        add_limit_order_log(limit_order_data_list, network)
    else:
        if len(limit_order_swap_data_list) > 0:
            add_limit_order_swap_log(limit_order_swap_data_list, network)


def handle_limit_order_log(parsed_log, block_id, receipt_id, timestamp, limit_order_with_swap, limit_order_data_list,
                           limit_order_swap_data_list):
    event = parsed_log.get("event")
    if "order_added" == event:
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            limit_order_date = {
                "type": "order_added",
                "tx_id": "",
                "block_id": block_id,
                "receipt_id": receipt_id,
                "order_id": data["order_id"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "point": data["point"],
                "sell_token": data["sell_token"],
                "original_amount": data["original_amount"],
                "original_deposit_amount": data["original_deposit_amount"],
                "timestamp": timestamp
            }
            limit_order_data_list.append(limit_order_date)
    if "swap" == event:
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            limit_order_swap_date = {
                "type": "swap",
                "tx_id": "",
                "block_id": block_id,
                "receipt_id": receipt_id,
                "token_in": data["token_in"],
                "token_out": data["token_out"],
                "pool_id": limit_order_with_swap["pool_id"],
                "point": limit_order_with_swap["point"],
                "amount_in": data["amount_in"],
                "amount_out": data["amount_out"],
                "owner_id": data["swapper"],
                "timestamp": timestamp
            }
            limit_order_swap_data_list.append(limit_order_swap_date)


def handle_burrow_args(receipt):
    res = []
    try:
        actions = receipt["Action"]["actions"]
        for action in actions:
            if 'FunctionCall' in action:
                try:
                    args = json.dumps(json.loads(base64.b64decode(action["FunctionCall"]["args"])))
                    res.append(args)
                except Exception as ee:
                    logger.error("handle_burrow_args error:{}", ee)
                    continue
    except Exception as e:
        logger.error("handle_burrow_args analysis error:{}", e)
    return res


def handle_burrow_log(logs, receipt_id, block_id, timestamp, predecessor_id, burrow_date_list, receipt):
    liquidate_number = 0
    for log in logs:
        if not log.startswith("EVENT_JSON:"):
            continue
        try:
            parsed_log = json.loads(log[len("EVENT_JSON:"):])
        except json.JSONDecodeError:
            logger.error("Error during parsing logs from JSON string to dict")
            continue
        handle_burrow_log_content(parsed_log, receipt_id, block_id, timestamp, predecessor_id, burrow_date_list, receipt, liquidate_number)


def handle_burrow_log_content(parsed_log, receipt_id, block_id, timestamp, predecessor_id, burrow_date_list, receipt, liquidate_number):
    event = parsed_log.get("event")
    args_list = handle_burrow_args(receipt)
    args = args_list[0]
    if event == "liquidate":
        args = args_list[liquidate_number]
        liquidate_number += 1
    event_json_data = parsed_log.get("data")
    for data in event_json_data:
        account_id = ""
        amount = 0
        token_id = ""
        liquidation_account_id = ""
        collateral_sum = 0
        repaid_sum = 0
        booster_amount = 0
        duration = 0
        x_booster_amount = 0
        total_booster_amount = 0
        total_x_booster_amount = 0
        position = "REGULAR"
        if "account_id" in data:
            account_id = data["account_id"]
        if "amount" in data:
            amount = data["amount"]
        if "token_id" in data:
            token_id = data["token_id"]
        if "liquidation_account_id" in data:
            liquidation_account_id = data["liquidation_account_id"]
        if "collateral_sum" in data:
            collateral_sum = data["collateral_sum"]
        if "repaid_sum" in data:
            repaid_sum = data["repaid_sum"]
        if "booster_amount" in data:
            booster_amount = data["booster_amount"]
        if "duration" in data:
            duration = data["duration"]
        if "x_booster_amount" in data:
            x_booster_amount = data["x_booster_amount"]
        if "total_booster_amount" in data:
            total_booster_amount = data["total_booster_amount"]
        if "total_x_booster_amount" in data:
            total_x_booster_amount = data["total_x_booster_amount"]
        if "position" in data:
            position = data["position"]
        burrow_date = {
            "event": event,
            "account_id": account_id,
            "amount": amount,
            "token_id": token_id,
            "receipt_id": receipt_id,
            "block_id": block_id,
            "predecessor_id": predecessor_id,
            "liquidation_account_id": liquidation_account_id,
            "collateral_sum": collateral_sum,
            "repaid_sum": repaid_sum,
            "booster_amount": booster_amount,
            "duration": duration,
            "x_booster_amount": x_booster_amount,
            "total_booster_amount": total_booster_amount,
            "total_x_booster_amount": total_x_booster_amount,
            "position": position,
            "timestamp": timestamp,
            "args": args
        }
        burrow_date_list.append(burrow_date)


def handle_receipt_content(content):
    res = []
    try:
        actions = content["Action"]["actions"]
        for action in actions:
            if 'FunctionCall' in action:
                args = json.loads(base64.b64decode(action["FunctionCall"]["args"]))
                if "method_name" in action["FunctionCall"]:
                    args["method_name"] = action["FunctionCall"]["method_name"]
                res.append(args)
    except Exception as e:
        logger.error("handle_receipt_content error:{}", e)
        res = res
    return res


def analysis_log_data(content):
    ret_data = {
        "token_in": "",
        "token_out": "",
        "amount_in": "",
        "amount_out": "",
    }
    try:
        json_obj = json.loads(content)
        if len(json_obj) == 2:
            token_in_data = str(json_obj[0]).split(" ")
            token_out_data = str(json_obj[1]).split(" ")
            ret_data["amount_in"] = token_in_data[0]
            ret_data["token_in"] = token_in_data[1]
            ret_data["amount_out"] = token_out_data[0]
            ret_data["token_out"] = token_out_data[1]
    except Exception as e:
        logger.error("Exception in parsing log data:{}", e)
    return ret_data


def handle_log_content(block_hash, block_id, predecessor_id, receiver_id, logs, timestamp, receipt, xref_data_list,
                       swap_data_list, liquidity_data_list, farm_data_list):
    import re
    stake_value = "assets, get"
    index_number = 0
    for log in logs:
        if stake_value in log:
            amount_out = str(re.findall("assets, get (.*?) token", log)[0])
            xref_args = handle_receipt_content(receipt)
            for xref_arg in xref_args:
                xref_data = {
                    "block_hash": block_hash,
                    "block_id": block_id,
                    "sender_id": xref_arg["sender_id"],
                    "amount_in": xref_arg["amount"],
                    "amount_out": amount_out,
                    "predecessor_id": predecessor_id,
                    "receiver_id": receiver_id,
                    "timestamp": timestamp,
                }
                xref_data_list.append(xref_data)

        un_stake_content = re.findall("Withdraw (.*?) NEAR from ", log)
        if len(un_stake_content) > 0:
            amount_out = str(un_stake_content[0])
            xref_data = {
                "block_hash": block_hash,
                "block_id": block_id,
                "sender_id": predecessor_id,
                "amount_in": amount_out,
                "amount_out": "0",
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
                "timestamp": timestamp,
            }
            xref_data_list.append(xref_data)

        swap_content = re.findall("Swapped (.*?) for ", log)
        if len(swap_content) > 0:
            swap_in = ""
            swap_out = ""
            log_content_list = log.split(" ")
            if len(log_content_list) > 4:
                swap_in = log_content_list[1]
                swap_out = log_content_list[4]
            swap_args = handle_receipt_content(receipt)
            for swap_arg in swap_args:
                if "msg" in swap_arg:
                    msg = swap_arg["msg"]
                    if "" != msg:
                        msg_json = json.loads(msg)
                        force = ""
                        if "force" in msg_json:
                            force = msg_json["force"]
                        actions = []
                        if "actions" in msg_json:
                            actions = msg_json["actions"]
                        if len(actions) > index_number:
                            action = actions[index_number]
                            pool_id = ""
                            token_in = ""
                            token_out = ""
                            amount_in = ""
                            min_amount_out = ""
                            if "pool_id" in action:
                                pool_id = action["pool_id"]
                            if "token_in" in action:
                                token_in = action["token_in"]
                            if "token_out" in action:
                                token_out = action["token_out"]
                            if "amount_in" in action:
                                amount_in = action["amount_in"]
                            if "min_amount_out" in action:
                                min_amount_out = action["min_amount_out"]
                            swap_data = {
                                "block_hash": block_hash,
                                "block_id": block_id,
                                "predecessor_id": predecessor_id,
                                "receiver_id": receiver_id,
                                "sender_id": swap_arg["sender_id"],
                                "amount": swap_arg["amount"],
                                "force": force,
                                "pool_id": pool_id,
                                "token_in": token_in,
                                "token_out": token_out,
                                "amount_in": amount_in,
                                "min_amount_out": min_amount_out,
                                "swap_in": swap_in,
                                "swap_out": swap_out,
                                "timestamp": timestamp,
                            }
                            swap_data_list.append(swap_data)
                else:
                    actions = []
                    sender_id = ""
                    if "actions" in swap_arg:
                        actions = swap_arg["actions"]
                    elif "operation" in swap_arg:
                        operation = swap_arg["operation"]
                        if "Swap" in operation:
                            swap_json = operation["Swap"]
                            actions = swap_json["actions"]
                            sender_id = swap_arg["sender_id"]
                    if len(actions) > index_number:
                        action = actions[index_number]
                        pool_id = ""
                        token_in = ""
                        token_out = ""
                        amount_in = ""
                        min_amount_out = ""
                        if "pool_id" in action:
                            pool_id = action["pool_id"]
                        if "token_in" in action:
                            token_in = action["token_in"]
                        if "token_out" in action:
                            token_out = action["token_out"]
                        if "amount_in" in action:
                            amount_in = action["amount_in"]
                        if "min_amount_out" in action:
                            min_amount_out = action["min_amount_out"]
                        swap_data = {
                            "block_hash": block_hash,
                            "block_id": block_id,
                            "predecessor_id": predecessor_id,
                            "receiver_id": receiver_id,
                            "sender_id": sender_id,
                            "amount": "",
                            "force": "",
                            "pool_id": pool_id,
                            "token_in": token_in,
                            "token_out": token_out,
                            "amount_in": amount_in,
                            "min_amount_out": min_amount_out,
                            "swap_in": swap_in,
                            "swap_out": swap_out,
                            "timestamp": timestamp,
                        }
                        swap_data_list.append(swap_data)
            index_number = index_number + 1

        add_liquidity_value = "Liquidity added "
        if add_liquidity_value in log:
            shares = ""
            shares_content = re.findall(", minted (.*?) shares", log)
            if len(shares_content) > 0:
                shares = str(shares_content[0])
            add_liquidity_token_content = re.findall("Liquidity added (.*?), minted ", log)
            if len(add_liquidity_token_content) > 0:
                add_liquidity_token_data = analysis_log_data(add_liquidity_token_content[0])
            else:
                add_liquidity_token_data = analysis_log_data(log)
            add_liquidity_args = handle_receipt_content(receipt)
            for add_liquidity_arg in add_liquidity_args:
                method_name = ""
                amounts = ""
                pool_id = ""
                if "method_name" in add_liquidity_arg:
                    method_name = add_liquidity_arg["method_name"]
                if "amounts" in add_liquidity_arg:
                    amounts = add_liquidity_arg["amounts"]
                if "pool_id" in add_liquidity_arg:
                    pool_id = add_liquidity_arg["pool_id"]
                add_liquidity_data = {
                    "block_hash": block_hash,
                    "block_id": block_id,
                    "predecessor_id": predecessor_id,
                    "receiver_id": receiver_id,
                    "method_name": method_name,
                    "pool_id": pool_id,
                    "shares": shares,
                    "amounts": str(amounts),
                    "token_in": add_liquidity_token_data["token_in"],
                    "token_out": add_liquidity_token_data["token_out"],
                    "amount_in": add_liquidity_token_data["amount_in"],
                    "amount_out": add_liquidity_token_data["amount_out"],
                    "log": log,
                    "timestamp": timestamp,
                }
                liquidity_data_list.append(add_liquidity_data)

        removed_liquidity_value = "liquidity removed: "
        if removed_liquidity_value in log:
            removed_liquidity_token_content = log[log.rfind('["'):]
            removed_liquidity_token_data = analysis_log_data(removed_liquidity_token_content)
            removed_liquidity_shares = log[0:log.rfind(' shares of liquidity removed')]
            removed_liquidity_args = handle_receipt_content(receipt)
            for removed_liquidity_arg in removed_liquidity_args:
                method_name = ""
                amounts = ""
                pool_id = ""
                if "method_name" in removed_liquidity_arg:
                    method_name = removed_liquidity_arg["method_name"]
                if "min_amounts" in removed_liquidity_arg:
                    amounts = removed_liquidity_arg["min_amounts"]
                if "pool_id" in removed_liquidity_arg:
                    pool_id = removed_liquidity_arg["pool_id"]
                removed_liquidity_data = {
                    "block_hash": block_hash,
                    "block_id": block_id,
                    "predecessor_id": predecessor_id,
                    "receiver_id": receiver_id,
                    "method_name": method_name,
                    "pool_id": pool_id,
                    "shares": removed_liquidity_shares,
                    "amounts": str(amounts),
                    "token_in": removed_liquidity_token_data["token_in"],
                    "token_out": removed_liquidity_token_data["token_out"],
                    "amount_in": removed_liquidity_token_data["amount_in"],
                    "amount_out": removed_liquidity_token_data["amount_out"],
                    "log": log,
                    "timestamp": timestamp,
                }
                liquidity_data_list.append(removed_liquidity_data)

        add_stable_liquidity_content = re.findall("Mint (.*?), fee is ", log)
        if len(add_stable_liquidity_content) > 0:
            add_stable_liquidity_args = handle_receipt_content(receipt)
            for add_stable_liquidity_arg in add_stable_liquidity_args:
                shares = ""
                method_name = ""
                amounts = ""
                pool_id = ""
                if "method_name" in add_stable_liquidity_arg:
                    method_name = add_stable_liquidity_arg["method_name"]
                if "min_shares" in add_stable_liquidity_arg:
                    shares = add_stable_liquidity_arg["min_shares"]
                if "amounts" in add_stable_liquidity_arg:
                    amounts = add_stable_liquidity_arg["amounts"]
                if "pool_id" in add_stable_liquidity_arg:
                    pool_id = add_stable_liquidity_arg["pool_id"]
                add_stable_liquidity_data = {
                    "block_hash": block_hash,
                    "block_id": block_id,
                    "predecessor_id": predecessor_id,
                    "receiver_id": receiver_id,
                    "method_name": method_name,
                    "pool_id": pool_id,
                    "shares": shares,
                    "amounts": str(amounts),
                    "token_in": "",
                    "token_out": "",
                    "amount_in": "",
                    "amount_out": "",
                    "log": log,
                    "timestamp": timestamp,
                }
                liquidity_data_list.append(add_stable_liquidity_data)

        remove_liquidity_by_tokens_content = re.findall("LP (.*?), and fee is ", log)
        if len(remove_liquidity_by_tokens_content) > 0:
            remove_liquidity_by_tokens_args = handle_receipt_content(receipt)
            for remove_liquidity_by_tokens_arg in remove_liquidity_by_tokens_args:
                shares = ""
                method_name = ""
                amounts = ""
                pool_id = ""
                if "method_name" in remove_liquidity_by_tokens_arg:
                    method_name = remove_liquidity_by_tokens_arg["method_name"]
                if "max_burn_shares" in remove_liquidity_by_tokens_arg:
                    shares = remove_liquidity_by_tokens_arg["max_burn_shares"]
                if "amounts" in remove_liquidity_by_tokens_arg:
                    amounts = remove_liquidity_by_tokens_arg["amounts"]
                if "pool_id" in remove_liquidity_by_tokens_arg:
                    pool_id = remove_liquidity_by_tokens_arg["pool_id"]
                remove_liquidity_by_tokens_data = {
                    "block_hash": block_hash,
                    "block_id": block_id,
                    "predecessor_id": predecessor_id,
                    "receiver_id": receiver_id,
                    "method_name": method_name,
                    "pool_id": pool_id,
                    "shares": shares,
                    "amounts": str(amounts),
                    "token_in": "",
                    "token_out": "",
                    "amount_in": "",
                    "amount_out": "",
                    "log": log,
                    "timestamp": timestamp,
                }
                liquidity_data_list.append(remove_liquidity_by_tokens_data)

        remove_liquidity_by_stable_pool_value = re.findall("LP (.*?) shares to gain tokens", log)
        if len(remove_liquidity_by_stable_pool_value) > 0:
            remove_liquidity_by_stable_pool_args = handle_receipt_content(receipt)
            for remove_liquidity_by_stable_pool_arg in remove_liquidity_by_stable_pool_args:
                shares = ""
                method_name = ""
                amounts = ""
                pool_id = ""
                if "method_name" in remove_liquidity_by_stable_pool_arg:
                    method_name = remove_liquidity_by_stable_pool_arg["method_name"]
                if "shares" in remove_liquidity_by_stable_pool_arg:
                    shares = remove_liquidity_by_stable_pool_arg["shares"]
                if "min_amounts" in remove_liquidity_by_stable_pool_arg:
                    amounts = remove_liquidity_by_stable_pool_arg["min_amounts"]
                if "pool_id" in remove_liquidity_by_stable_pool_arg:
                    pool_id = remove_liquidity_by_stable_pool_arg["pool_id"]
                remove_liquidity_by_stable_pool_data = {
                    "block_hash": block_hash,
                    "block_id": block_id,
                    "predecessor_id": predecessor_id,
                    "receiver_id": receiver_id,
                    "method_name": method_name,
                    "pool_id": pool_id,
                    "shares": shares,
                    "amounts": str(amounts),
                    "token_in": "",
                    "token_out": "",
                    "amount_in": "",
                    "amount_out": "",
                    "log": log,
                    "timestamp": timestamp,
                }
                liquidity_data_list.append(remove_liquidity_by_stable_pool_data)

        if log.startswith("EVENT_JSON:"):
            try:
                parsed_log = json.loads(log[len("EVENT_JSON:"):])
            except json.JSONDecodeError:
                logger.error("Error during parsing logs from JSON string to dict")
                continue
            farm_data = {
                "block_hash": block_hash,
                "block_id": block_id,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
                "token_id": "",
                "sender_id": "",
                "msg": "",
                "event": "",
                "farmer_id": "",
                "seed_id": "",
                "amount": "",
                "increased_power": "",
                "duration": "",
                "timestamp": timestamp,
            }
            if "event" in parsed_log:
                if parsed_log.get("event") == "seed_deposit":
                    farm_data["event"] = parsed_log.get("event")
                    farm_args = handle_receipt_content(receipt)
                    log_data = parsed_log.get("data")
                    if len(farm_args) > 0:
                        for farm_arg in farm_args:
                            farm_data["token_id"] = ""
                            if "token_id" in farm_arg:
                                farm_data["token_id"] = farm_arg["token_id"]
                            farm_data["sender_id"] = ""
                            if "sender_id" in farm_arg:
                                farm_data["sender_id"] = farm_arg["sender_id"]
                            farm_data["msg"] = ""
                            if "msg" in farm_arg:
                                farm_data["msg"] = farm_arg["msg"]
                            for data in log_data:
                                farm_data["farmer_id"] = data["farmer_id"]
                                farm_data["seed_id"] = data["seed_id"]
                                farm_data["amount"] = data["deposit_amount"]
                                farm_data["increased_power"] = data["increased_power"]
                                farm_data["duration"] = data["duration"]
                                farm_data_list.append(farm_data)
                    else:
                        for data in log_data:
                            farm_data["farmer_id"] = data["farmer_id"]
                            farm_data["seed_id"] = data["seed_id"]
                            farm_data["amount"] = data["deposit_amount"]
                            farm_data["increased_power"] = data["increased_power"]
                            farm_data["duration"] = data["duration"]
                            farm_data_list.append(farm_data)

                if parsed_log.get("event") == "seed_withdraw":
                    farm_data["event"] = parsed_log.get("event")
                    farm_args = handle_receipt_content(receipt)
                    log_data = parsed_log.get("data")
                    if len(farm_args) > 0:
                        for farm_arg in farm_args:
                            farm_data["sender_id"] = farm_arg["sender_id"]
                            for data in log_data:
                                farm_data["farmer_id"] = data["farmer_id"]
                                farm_data["seed_id"] = data["seed_id"]
                                farm_data["amount"] = data["withdraw_amount"]
                                farm_data_list.append(farm_data)
                    else:
                        for data in log_data:
                            farm_data["farmer_id"] = data["farmer_id"]
                            farm_data["seed_id"] = data["seed_id"]
                            farm_data["amount"] = data["withdraw_amount"]
                            farm_data_list.append(farm_data)


def handle_dcl_log(logs, tx_id, block_id, timestamp, network, receipt, predecessor_id, receiver_id):
    for log in logs:
        # logger.info("log:{}", log)
        if not log.startswith("EVENT_JSON:"):
            continue
        try:
            parsed_log = json.loads(log[len("EVENT_JSON:"):])
        except json.JSONDecodeError:
            logger.error("Error during parsing logs from JSON string to dict")
            continue
        handle_dcl_log_content(parsed_log, tx_id, block_id, timestamp, network, receipt, predecessor_id, receiver_id)


def handle_dcl_receipt_args_content(content):
    res = []
    try:
        actions = content["Action"]["actions"]
        for action in actions:
            if 'FunctionCall' in action:
                args = json.loads(base64.b64decode(action["FunctionCall"]["args"]))
                res.append(args)
    except Exception as e:
        logger.error("handle_limit_order_receipt_content analysis error:{}", e)
        res = res
    return res


def handle_dcl_log_content(parsed_log, tx_id, block_id, timestamp, network, receipt, predecessor_id, receiver_id):
    args = json.dumps(handle_dcl_receipt_args_content(receipt))
    event = parsed_log.get("event")
    if "swap" == event:
        swap_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            pool_id = ""
            protocol_fee_amounts = ""
            total_fee_amounts = ""
            if "pool_ids" in data:
                pool_id = str(data["pool_ids"])
            if "pool_id" in data:
                pool_id = data["pool_id"]
            if "protocol_fee" in data:
                protocol_fee_amounts = str(data["protocol_fee"])
            if "total_fee" in data:
                total_fee_amounts = str(data["total_fee"])
            swap_date = {
                "swapper": data["swapper"],
                "token_in": data["token_in"],
                "token_out": data["token_out"],
                "amount_in": data["amount_in"],
                "amount_out": data["amount_out"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
                "pool_id": pool_id,
                "protocol_fee_amounts": protocol_fee_amounts,
                "total_fee_amounts": total_fee_amounts,
            }
            swap_date_list.append(swap_date)
        add_swap(swap_date_list, event, network)

    elif "swap_desire" == event:
        swap_desire_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            swap_desire_date = {
                "swapper": data["swapper"],
                "token_in": data["token_in"],
                "token_out": data["token_out"],
                "amount_in": data["amount_in"],
                "amount_out": data["amount_out"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
            }
            swap_desire_date_list.append(swap_desire_date)
        add_swap_desire(swap_desire_date_list, event, network)

    elif "liquidity_added" == event:
        liquidity_added_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            liquidity_added_date = {
                "event_method": event,
                "lpt_id": data["lpt_id"],
                "merge_lpt_ids": "",
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "left_point": data["left_point"],
                "right_point": data["right_point"],
                "added_amount": data["added_amount"],
                "cur_amount": data["cur_amount"],
                "paid_token_x": data["paid_token_x"],
                "paid_token_y": data["paid_token_y"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
                "claim_fee_token_x": "",
                "claim_fee_token_y": "",
                "merge_token_x": "",
                "merge_token_y": "",
                "remove_token_x": "",
                "remove_token_y": "",
            }
            liquidity_added_date_list.append(liquidity_added_date)
        add_liquidity_added(liquidity_added_date_list, event, network)

    elif "liquidity_removed" == event or "emergency_liquidity_removed" == event:
        liquidity_removed_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            claim_fee_token_x = ""
            claim_fee_token_y = ""
            if "claim_fee_token_x" in data:
                claim_fee_token_x = data["claim_fee_token_x"]
            if "claim_fee_token_y" in data:
                claim_fee_token_y = data["claim_fee_token_y"]
            liquidity_removed_date = {
                "event_method": event,
                "lpt_id": data["lpt_id"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "left_point": data["left_point"],
                "right_point": data["right_point"],
                "removed_amount": data["removed_amount"],
                "cur_amount": data["cur_amount"],
                "refund_token_x": data["refund_token_x"],
                "refund_token_y": data["refund_token_y"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
                "claim_fee_token_x": claim_fee_token_x,
                "claim_fee_token_y": claim_fee_token_y,
            }
            liquidity_removed_date_list.append(liquidity_removed_date)
        add_liquidity_removed(liquidity_removed_date_list, "liquidity_removed", network)

    elif "lostfound" == event:
        lostfound_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            lostfound_date = {
                "user": data["user"],
                "token": data["token"],
                "amount": data["amount"],
                "locked": data["locked"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
            }
            lostfound_date_list.append(lostfound_date)
        add_lostfound(lostfound_date_list, event, network)

    elif "order_added" == event:
        order_added_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            order_added_date = {
                "order_id": data["order_id"],
                "created_at": data["created_at"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "point": data["point"],
                "sell_token": data["sell_token"],
                "buy_token": data["buy_token"],
                "original_amount": data["original_amount"],
                "original_deposit_amount": data["original_deposit_amount"],
                "swap_earn_amount": data["swap_earn_amount"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
            }
            order_added_date_list.append(order_added_date)
        add_order_added(order_added_date_list, event, network)

    elif "order_cancelled" == event:
        order_cancelled_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            order_cancelled_date = {
                "order_id": data["order_id"],
                "created_at": data["created_at"],
                "cancel_at": data["cancel_at"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "point": data["point"],
                "sell_token": data["sell_token"],
                "buy_token": data["buy_token"],
                "request_cancel_amount": data["request_cancel_amount"],
                "actual_cancel_amount": data["actual_cancel_amount"],
                "original_amount": data["original_amount"],
                "cancel_amount": data["cancel_amount"],
                "remain_amount": data["remain_amount"],
                "bought_amount": data["bought_amount"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
            }
            order_cancelled_date_list.append(order_cancelled_date)
        add_order_cancelled(order_cancelled_date_list, event, network)

    elif "order_completed" == event or "emergency_order_cancel" == event:
        order_completed_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            cancel_amount_this_time = ""
            bought_amount_this_time = ""
            if "emergency_order_cancel" == event:
                cancel_amount_this_time = data["cancel_amount_this_time"]
                bought_amount_this_time = data["bought_amount_this_time"]
            order_completed_date = {
                "event_method": event,
                "order_id": data["order_id"],
                "created_at": data["created_at"],
                "completed_at": data["completed_at"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "point": data["point"],
                "sell_token": data["sell_token"],
                "buy_token": data["buy_token"],
                "original_amount": data["original_amount"],
                "original_deposit_amount": data["original_deposit_amount"],
                "swap_earn_amount": data["swap_earn_amount"],
                "cancel_amount": data["cancel_amount"],
                "bought_amount": data["bought_amount"],
                "cancel_amount_this_time": cancel_amount_this_time,
                "bought_amount_this_time": bought_amount_this_time,
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
            }
            order_completed_date_list.append(order_completed_date)
        add_order_completed(order_completed_date_list, "order_completed", network)

    elif "liquidity_append" == event:
        liquidity_append_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            claim_fee_token_x = ""
            claim_fee_token_y = ""
            if "claim_fee_token_x" in data:
                claim_fee_token_x = data["claim_fee_token_x"]
            if "claim_fee_token_y" in data:
                claim_fee_token_y = data["claim_fee_token_y"]
            liquidity_append_date = {
                "event_method": event,
                "lpt_id": data["lpt_id"],
                "merge_lpt_ids": "",
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "left_point": data["left_point"],
                "right_point": data["right_point"],
                "added_amount": data["added_amount"],
                "cur_amount": data["cur_amount"],
                "paid_token_x": data["paid_token_x"],
                "paid_token_y": data["paid_token_y"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
                "claim_fee_token_x": claim_fee_token_x,
                "claim_fee_token_y": claim_fee_token_y,
                "merge_token_x": "",
                "merge_token_y": "",
                "remove_token_x": "",
                "remove_token_y": "",
            }
            liquidity_append_date_list.append(liquidity_append_date)
        add_liquidity_added(liquidity_append_date_list, "liquidity_added", network)

    elif "liquidity_merge" == event:
        liquidity_merge_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            claim_fee_token_x = ""
            claim_fee_token_y = ""
            merge_token_x = ""
            merge_token_y = ""
            remove_token_x = ""
            remove_token_y = ""
            paid_token_x = ""
            paid_token_y = ""
            if "claim_fee_token_x" in data:
                claim_fee_token_x = data["claim_fee_token_x"]
            if "claim_fee_token_y" in data:
                claim_fee_token_y = data["claim_fee_token_y"]
            if "merge_token_x" in data:
                merge_token_x = data["merge_token_x"]
            if "merge_token_y" in data:
                merge_token_y = data["merge_token_y"]
            if "remove_token_x" in data:
                remove_token_x = data["remove_token_x"]
            if "remove_token_y" in data:
                remove_token_y = data["remove_token_y"]
            if "paid_token_x" in data:
                paid_token_x = data["paid_token_x"]
            if "paid_token_y" in data:
                paid_token_y = data["paid_token_y"]
            liquidity_merge_date = {
                "event_method": event,
                "lpt_id": data["lpt_id"],
                "merge_lpt_ids": data["merge_lpt_ids"],
                "owner_id": data["owner_id"],
                "pool_id": data["pool_id"],
                "left_point": data["left_point"],
                "right_point": data["right_point"],
                "added_amount": data["added_amount"],
                "cur_amount": data["cur_amount"],
                "paid_token_x": paid_token_x,
                "paid_token_y": paid_token_y,
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
                "claim_fee_token_x": claim_fee_token_x,
                "claim_fee_token_y": claim_fee_token_y,
                "merge_token_x": merge_token_x,
                "merge_token_y": merge_token_y,
                "remove_token_x": remove_token_x,
                "remove_token_y": remove_token_y,
            }
            liquidity_merge_date_list.append(liquidity_merge_date)
        add_liquidity_added(liquidity_merge_date_list, "liquidity_added", network)

    elif "claim_charged_fee" == event:
        claim_charged_fee_date_list = []
        event_json_data = parsed_log.get("data")
        for data in event_json_data:
            claim_charged_fee_date = {
                "user": data["user"],
                "pool_id": data["pool_id"],
                "amount_x": data["amount_x"],
                "amount_y": data["amount_y"],
                "tx_id": tx_id,
                "block_id": block_id,
                "timestamp": timestamp,
                "args": args,
                "predecessor_id": predecessor_id,
                "receiver_id": receiver_id,
            }
            claim_charged_fee_date_list.append(claim_charged_fee_date)
        add_claim_charged_fee(claim_charged_fee_date_list, event, network)


def handle_not_registered_logs_content(logs, receipt, block_hash, tx_id, timestamp, block, receiver_id, not_registered_data_list):
    value = 'Depositing to owner'
    try:
        for log in logs:
            if value in log:
                args = handle_not_registered_receipt_content(receipt)
                if "is not registered. Depositing to owner" in value:
                    data_type = "not_registered"
                else:
                    data_type = "not_enough_storage"
                for arg in args:
                    data = {
                        "block_id": block_hash,
                        "tx_id": tx_id,
                        "token_id": arg["token_id"],
                        "sender_id": arg["sender_id"],
                        "amount": arg["amount"],
                        "timestamp": timestamp,
                        "block": block,
                        "receiver_id": receiver_id,
                        "type": data_type,
                        "log": log
                    }
                    not_registered_data_list.append(data)
    except Exception as e:
        logger.error("handle_not_registered_logs_content error:{}", e)


def handle_not_registered_receipt_content(content):
    res = []
    try:
        actions = content["Action"]["actions"]
        for action in actions:
            if 'FunctionCall' in action:
                args = json.loads(base64.b64decode(action["FunctionCall"]["args"]))
                res.append(args)
    except Exception as e:
        logger.error("handle_not_registered_receipt_content error:{}", e)
        res = res
    return res


def handle_withdraw_reward_content(receipt_id, block_id, logs, timestamp, withdraw_reward_insert_data):
    for log in logs:
        try:
            if " withdraw reward " in log and log.endswith("Succeed."):
                log_data = log.split(" ")
                account_id = log_data[0]
                token = log_data[3]
                amount = log_data[5].rstrip(",")
                withdraw_reward_data = {
                    "account_id": account_id,
                    "amount": amount,
                    "token": token,
                    "receipt_id": receipt_id,
                    "block_id": block_id,
                    "timestamp": timestamp
                }
                withdraw_reward_insert_data.append(withdraw_reward_data)

            if log.startswith("EVENT_JSON:"):
                parsed_log = json.loads(log[len("EVENT_JSON:"):])
                event = parsed_log.get("event")
                if "reward_withdraw" == event:
                    event_json_data = parsed_log.get("data")
                    for data in event_json_data:
                        if data["success"] is True:
                            withdraw_reward_data = {
                                "account_id": data["farmer_id"],
                                "amount": data["withdraw_amount"],
                                "token": data["token_id"],
                                "receipt_id": receipt_id,
                                "block_id": block_id,
                                "timestamp": timestamp
                            }
                            withdraw_reward_insert_data.append(withdraw_reward_data)
        except Exception as e:
            logger.error("analysis withdraw reward log error:{}", e)
            continue


def to_under_line(x):
    import re
    return re.sub('(?<=[a-z])[A-Z]|(?<!^)[A-Z](?=[a-z])', '_\g<0>', x).upper()


if __name__ == "__main__":
    print("-----------------------------")
    network_id = "MAINNET"
    start_id = Cfg.DB3_START_ID
    while True:
        start_id = get_near_transaction_data(network_id.lower(), start_id)
        logger.info("start_id:{}", start_id)
        time.sleep(1)
