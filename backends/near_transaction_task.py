import sys
sys.path.append('../')
import requests
from config import Cfg
import json
import time
from db_provider import add_near_lake_latest_actions


def get_near_transaction_data(start_time):
    args = "?chain_id=90000000&start_time=%s&limit=10&direction=asc" % start_time
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
    if transaction_data_list is not None:
        for transaction_data in transaction_data_list:
            latest_actions_data = {
                "timestamp": transaction_data["tx_time"],
                "tx_id": transaction_data["tx_hash"],
                "receiver_account_id": transaction_data["receive"],
                "method_name": transaction_data["method"],
                "args": "",
                "deposit": "0",
                "status": "SUCCESS_VALUE",
                "predecessor_account_id": transaction_data["sender"],
                "receiver_id": transaction_data["sender"],
                "receipt_id": transaction_data["tx_hash"],
            }
            latest_actions_list.append(latest_actions_data)
            if int(transaction_data["tx_time"]) > start_time:
                start_time = int(transaction_data["tx_time"]) + 1
                print("insert data:", transaction_data["id"])
    if len(latest_actions_list) > 0:
        add_near_lake_latest_actions(latest_actions_list, Cfg.NETWORK_ID)
    return start_time


if __name__ == "__main__":
    print("-----------------------------")
    # now_time = int(time.time_ns())
    now_time = 1720032637027005567
    while True:
        new_time = get_near_transaction_data(now_time)
        now_time = new_time
        print("start_time:", now_time)
        time.sleep(5)

