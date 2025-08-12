import sys
import requests
import json
import time
sys.path.append('../')
from db_provider import get_token_day_data_index_number, add_token_day_data
from config import Cfg


def handel_rhea_data(network_id):
    timestamp = int(time.time())
    token_list = ["token.v2.ref-finance.near", "token.burrow.near", "xtoken.ref-finance.near", "token.rhealab.near", "xtoken.rhealab.near"]
    index_number = get_token_day_data_index_number(network_id)
    for token_id in token_list:
        token_holders_list = []
        request_url = "https://api.fastnear.com/v1/ft/" + token_id + "/top"
        holders_list_ret = requests.get(request_url).text
        holders_data = json.loads(holders_list_ret)
        holders_accounts = holders_data["accounts"]
        rank = 1
        for holders in holders_accounts:
            if holders["account_id"] not in Cfg.TOKEN_HOLDERS_WHITELIST:
                holders_account_data = {"account_id": holders["account_id"], "balance": holders["balance"], "rank": rank}
                token_holders_list.append(holders_account_data)
                rank = rank + 1
        add_token_day_data(network_id, token_holders_list, index_number + 1, token_id, timestamp)


if __name__ == '__main__':
    print("start rhea data task")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            handel_rhea_data(network_id)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("end rhea data task")
