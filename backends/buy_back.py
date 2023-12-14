import sys

sys.path.append('../')
from near_multinode_rpc_provider import MultiNodeJsonProviderError, MultiNodeJsonProvider
from config import Cfg
import json
import time
import sys
from redis_provider import list_top_pools, list_token_price, list_token_metadata
from utils import combine_pools_info
from decimal import *
import requests
from contract_handler import RpcHandler
import globals
from buyback_config import GlobalConfig
global_config = GlobalConfig()
import random


def handle_buy_buck(network_id):
    try:
        conn = MultiNodeJsonProvider(network_id)
        ret = conn.view_call(Cfg.NETWORK[network_id]["BUYBACK_CONTRACT"], "get_available_fund_amount", b'')
        b = "".join([chr(x) for x in ret["result"]])
        amount_in = int(json.loads(b))
        print("fund_amount:", amount_in)
        if amount_in > 0:
            handle_flow(network_id, amount_in)
        else:
            print("not fund_amount")
    except MultiNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)


def handle_flow(network_id, amount_in):
    # query_list_pools_url = "https://dev-indexer.ref-finance.com/list-top-pools"
    # requests.packages.urllib3.disable_warnings()
    # list_pools_data_ret = requests.get(url=query_list_pools_url, verify=False)
    # pools = json.loads(list_pools_data_ret.text)

    pools = list_top_pools(Cfg.NETWORK_ID)
    prices = list_token_price(Cfg.NETWORK_ID)
    metadata = list_token_metadata(Cfg.NETWORK_ID)
    combine_pools_info(pools, prices, metadata)

    actions = []
    buyback_pool_one = {}
    buyback_pool_two = {}
    for pool in pools:
        if pool["id"] == Cfg.NETWORK[network_id]["BUYBACK_POOL_ONE"]:
            buyback_pool_one = pool
        if pool["id"] == Cfg.NETWORK[network_id]["BUYBACK_POOL_TWO"]:
            buyback_pool_two = pool

    if buyback_pool_one != {} and buyback_pool_two != {}:
        one_account_ids = buyback_pool_one["token_account_ids"]
        one_amounts = buyback_pool_one["amounts"]
        if one_account_ids[0] == Cfg.NETWORK[network_id]["BUYBACK_TOKEN_IN_CONTRACT"]:
            one_in_balance = one_amounts[0]
            one_out_balance = one_amounts[1]
            one_token_in = one_account_ids[0]
            one_token_out = one_account_ids[1]
        else:
            one_in_balance = one_amounts[1]
            one_out_balance = one_amounts[0]
            one_token_in = one_account_ids[1]
            one_token_out = one_account_ids[0]
        one_amount_out = get_token_flow_ratio(amount_in, one_in_balance, one_out_balance, buyback_pool_one["total_fee"])
        print("one_amount_out:", one_amount_out)
        one_min_amount_out = int(decimal_mult(one_amount_out, 0.997))
        action_one = {
            "pool_id": int(buyback_pool_one["id"]),
            "token_in": one_token_in,
            "amount_in": str(amount_in),
            "token_out": one_token_out,
            "min_amount_out": str(one_min_amount_out)
        }
        actions.append(action_one)
        two_account_ids = buyback_pool_two["token_account_ids"]
        two_amounts = buyback_pool_two["amounts"]
        if two_account_ids[1] == Cfg.NETWORK[network_id]["BUYBACK_TOKEN_OUT_CONTRACT"]:
            two_in_balance = two_amounts[0]
            two_out_balance = two_amounts[1]
            two_token_in = two_account_ids[0]
            two_token_out = two_account_ids[1]
        else:
            two_in_balance = two_amounts[1]
            two_out_balance = two_amounts[0]
            two_token_in = two_account_ids[1]
            two_token_out = two_account_ids[0]
        two_amount_out = get_token_flow_ratio(one_amount_out, two_in_balance, two_out_balance, buyback_pool_two["total_fee"])
        print("two_amount_out:", two_amount_out)
        two_min_amount_out = int(decimal_mult(two_amount_out, 0.997))
        action_two = {
            "pool_id": int(buyback_pool_two["id"]),
            "token_in": two_token_in,
            "amount_in": None,
            "token_out": two_token_out,
            "min_amount_out": str(two_min_amount_out)
        }
        actions.append(action_two)
    num = random.randint(1, 100)
    print("random num:", num)
    time.sleep(num)
    print("actions:", actions)
    signer = globals.get_signer_account(global_config.signer_account_id)
    burrow_handler = RpcHandler(signer, Cfg.NETWORK[network_id]["BUYBACK_CONTRACT"])
    ret = burrow_handler.do_buyback(actions)
    # print("buyback:", ret)
    return ret


def get_token_flow_ratio(token_in_amount, token_in_balance, token_out_balance, fee):
    try:
        token_in_amount = int(token_in_amount)
        token_in_balance = int(token_in_balance)
        token_out_balance = int(token_out_balance)
        fee = int(fee)
        ratio = token_in_amount * (10000 - fee) * token_out_balance / (
                10000 * token_in_balance + token_in_amount * (10000 - fee))
        return int(ratio)
    except Exception as e:
        print("get ratio error:", e)
        return 0


def format_decimal_float(number):
    format_number = "{0:.16f}".format(Decimal(number))
    if '.' in format_number:
        return float(format_number[:format_number.index('.') + 1 + 8])
    return float(format_number)


def decimal_mult(number_one, number_two):
    return Decimal(str(number_one)) * Decimal(str(number_two))


if __name__ == "__main__":

    if len(sys.argv) == 2:
        start_time = int(time.time())
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            print("Staring buy back ...")
            handle_buy_buck(network_id)
            end_time = int(time.time())
            print("buy back end")
            print("buy back consuming time:{}", start_time - end_time)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)

    # handle_buy_buck("DEVNET")

    # actions = []
    # action_two = {
    #     "pool_id": "id",
    #     "token_in": "two_token_in",
    #     "amount_in": None,
    #     "token_out": "two_token_out",
    #     "min_amount_out": str("two_min_amount_out")
    # }
    # actions.append(action_two)
    # msg = {
    #     "actions": actions
    # }
    # a = {
    #     "swap_msg": json.dumps(msg)
    # }
    # print(json.dumps(a))

    # ret = get_token_flow_ratio(1000000, 265173061933, 161409282403047959654493194, 30)
    # print(ret)


