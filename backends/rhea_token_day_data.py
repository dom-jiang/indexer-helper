import sys
import json
import time
import requests
sys.path.append('../')
from db_provider import get_claim_airdrop_account_list, get_rhea_token_day_data_index_number, add_rhea_token_day_data
from near_multinode_rpc_provider import MultiNodeJsonProvider


def handel_rhea_token_data(network_id):
    rhea_token_data_list = []
    timestamp = int(time.time())
    request_url = "https://api.ref.finance/get-pool?pool_id=6458"
    pool_ret = requests.get(request_url).text
    pool_data = json.loads(pool_ret)
    shares_total_supply = int(pool_data["shares_total_supply"])
    rhea_pool_amount = int(pool_data["amounts"][0])
    conn = MultiNodeJsonProvider(network_id)
    conn._rpc_addr = "http://45.78.208.140:3030"
    index_number = get_rhea_token_day_data_index_number(network_id)
    airdrop_account_list = get_claim_airdrop_account_list(network_id)
    for airdrop_account in airdrop_account_list:
        account_id = airdrop_account["addr"]
        print("account_id:", account_id)
        if account_id != "":
            airdrop_balance = int(airdrop_account["total_reward_amount"].replace(',', ''))
            query_args = {"account_id": account_id}
            rhea_ret = conn.view_call(account_id="token.rhealab.near", method_name="ft_balance_of", args=json.dumps(query_args).encode('utf8'))
            if rhea_ret is None or "result" not in rhea_ret or rhea_ret['result'] is None:
                rhea_balance = "0"
            else:
                rhea_balance = json.loads(''.join([chr(x) for x in rhea_ret['result']]))

            xrhea_ret = conn.view_call(account_id="xtoken.rhealab.near", method_name="ft_balance_of", args=json.dumps(query_args).encode('utf8'))
            if xrhea_ret is None or "result" not in xrhea_ret or xrhea_ret['result'] is None:
                stake_rhea_balance = "0"
            else:
                stake_rhea_balance = json.loads(''.join([chr(x) for x in xrhea_ret['result']]))

            query_args = {"pool_id": 6458, "account_id": account_id}
            pool_share_ret = conn.view_call(account_id="v2.ref-finance.near", method_name="get_pool_shares", args=json.dumps(query_args).encode('utf8'))
            if pool_share_ret is None or "result" not in pool_share_ret or pool_share_ret['result'] is None:
                lp_balance = "0"
            else:
                pool_share_data = json.loads(''.join([chr(x) for x in pool_share_ret['result']]))
                lp_balance = int(int(pool_share_data) / shares_total_supply * rhea_pool_amount)
            account_positions_ret = conn.view_call(account_id="contract.main.burrow.near", method_name="get_account_all_positions", args=json.dumps(query_args).encode('utf8'))
            if account_positions_ret is None or "result" not in account_positions_ret or account_positions_ret['result'] is None:
                lock_boost_balance = "0"
                lending_balance = "0"
            else:
                account_positions_data = json.loads(''.join([chr(x) for x in account_positions_ret['result']]))
                lock_boost_balance = "0"
                lending_balance = "0"
                if account_positions_data is not None:
                    if "supplied" in account_positions_data:
                        supplied_list = account_positions_data["supplied"]
                        for supplied in supplied_list:
                            if supplied["token_id"] == "xtoken.rhealab.near":
                                lock_boost_balance = supplied["balance"]
                    if "booster_stakings" in account_positions_data:
                        booster_stakings = account_positions_data["booster_stakings"]
                        for token_id, staked_value in booster_stakings.items():
                            if token_id == "xtoken.rhealab.near":
                                lending_balance = staked_value["staked_booster_amount"]
            rhea_token_data = {"account_id": account_id, "airdrop_balance": airdrop_balance,
                               "rhea_balance": rhea_balance, "stake_rhea_balance": stake_rhea_balance,
                               "lp_balance": str(lp_balance), "lock_boost_balance": lock_boost_balance,
                               "lending_balance": lending_balance}
            rhea_token_data_list.append(rhea_token_data)
    if len(rhea_token_data_list) > 0:
        add_rhea_token_day_data(network_id, rhea_token_data_list, index_number + 1, timestamp)


if __name__ == '__main__':
    print("start rhea data task")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            handel_rhea_token_data(network_id)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("end rhea data task")
