import sys
sys.path.append('../')
import requests
from config import Cfg
import json
import time
from db_provider import add_near_lake_latest_actions, add_liquidate_log


def get_near_transaction_data(start_time):
    args = "?chain_id=90000000&start_time=%s&limit=100&direction=asc" % start_time
    try:
        headers = {
            'AccessKey': Cfg.DB3_ACCESS_KEY
        }
        response = requests.get(Cfg.DB3_URL + args, headers=headers)
        ret_data = response.text
        near_transaction_data = json.loads(ret_data)
        if near_transaction_data["code"] != 0:
            print("db3 transaction list error:", near_transaction_data)
        else:
            transaction_data_list = near_transaction_data["data"]["list"]
            start_time = handel_transaction_data(transaction_data_list, start_time)
    except Exception as e:
        print("get_near_transaction_data error: ", e)
    return start_time


def handel_transaction_data(transaction_data_list, start_time):
    latest_actions_list = []
    liquidata_log_list = []
    if transaction_data_list is not None:
        for transaction_data in transaction_data_list:
            if transaction_data["sender"] not in Cfg.CONTRACT_LIST and transaction_data["receive"] not in Cfg.CONTRACT_LIST:
                continue
            handel_latest_actions(transaction_data, latest_actions_list)
            if transaction_data["method"] == "liquidate":
                liquidata_log_list.append(handel_liquidata_log(transaction_data))
            if int(transaction_data["tx_time"]) > start_time:
                start_time = int(transaction_data["tx_time"]) + 1
    if len(latest_actions_list) > 0:
        add_near_lake_latest_actions(latest_actions_list, Cfg.NETWORK_ID)
    if len(liquidata_log_list) > 0:
        add_liquidate_log(liquidata_log_list, Cfg.NETWORK_ID)
    return start_time


def handel_latest_actions(transaction_data, latest_actions_list):
    receive_contract_address = ["v2.ref-finance.near", "v2.ref-farming.near", "xtoken.ref-finance.near", "wrap.near", "boostfarm.ref-labs.near", "usn", "dclv2.ref-labs.near"]
    sender_contract_address = ["v2.ref-finance.near", "xtoken.ref-finance.near", "dclv2.ref-labs.near"]
    manager_method_name = ["user_request_withdraw", "storage_deposit", "ft_transfer_call", "user_deposit_native_token", "user_request_settlement"]
    burrow_method_name = ["ft_transfer_call", "ft_on_transfer", "ft_resolve_transfer", "execute", "oracle_call", "oracle_on_call", "ft_transfer", "after_ft_transfer", "account_farm_claim_all"]
    sender = transaction_data["sender"]
    receive = transaction_data["receive"]
    method = transaction_data["method"]
    if receive in receive_contract_address or \
            (receive == "aurora" and (method == "ft_transfer_call" or method == "call")) or \
            sender in sender_contract_address or \
            ((receive == "asset-manager.orderly-network.near" or sender == "asset-manager.orderly-network.near") and
             method in manager_method_name) or \
            ((receive == "contract.main.burrow.near" or sender == "contract.main.burrow.near") and
             method in burrow_method_name):
        args = {}
        amounts = []
        if transaction_data["token_in"] is not None:
            amount_1 = transaction_data["token_in"]["volume"]
            amounts.append(amount_1)
            args["amounts"] = amounts
        if transaction_data["token_out"] is not None:
            amount_2 = transaction_data["token_out"]["volume"]
            amounts.append(amount_2)
            args["amounts"] = amounts
        if transaction_data["token_out_1"] is not None:
            pool_id = transaction_data["token_out_1"]["address"]
            args["pool_id"] = pool_id
        latest_actions_data = {
            "timestamp": transaction_data["tx_time"],
            "tx_id": transaction_data["tx_hash"],
            "receiver_account_id": transaction_data["receive"],
            "method_name": transaction_data["method"],
            "args": json.dumps(args),
            "deposit": "0",
            "status": "SUCCESS_VALUE",
            "predecessor_account_id": transaction_data["sender"],
            "receiver_id": transaction_data["sender"],
            "receipt_id": transaction_data["tx_hash"],
        }
        latest_actions_list.append(latest_actions_data)


def handel_liquidata_log(transaction_data):
    liquidata_log = {
        "block_number": transaction_data["block_number"],
        "tx_hash": transaction_data["tx_hash"],
        "tx_time": transaction_data["tx_time"],
        "sender": transaction_data["sender"],
        "receive": transaction_data["receive"],
        "type": transaction_data["type"],
        "sub_type": transaction_data["sub_type"],
        "dapp": transaction_data["dapp"],
        "gas": transaction_data["gas"],
        "trading_usd": transaction_data["trading_usd"],
        "extra_data": transaction_data["extra_data"],
        "token_in": json.dumps(transaction_data["token_in"]),
        "token_in_1": json.dumps(transaction_data["token_in_1"]),
        "token_out": json.dumps(transaction_data["token_out"]),
        "token_out_1": json.dumps(transaction_data["token_out_1"]),
        "tokens_in": json.dumps(transaction_data["tokens_in"]),
        "tokens_out": json.dumps(transaction_data["tokens_out"]),
    }
    return liquidata_log


if __name__ == "__main__":
    print("-----------------------------")
    # now_time = int(time.time_ns())
    now_time = 1719978745616365652
    while True:
        new_time = get_near_transaction_data(now_time)
        now_time = new_time
        print("start_time:", now_time)
        time.sleep(1)

