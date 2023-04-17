import sys

sys.path.append('../')
import requests
import json
from config import Cfg
import time
from db_provider import add_token_flow
from token_flow_utils import get_stable_and_rated_pool, get_swapped_amount, get_token_flow_ratio
from redis_provider import list_top_pools, list_token_price, list_token_metadata
from utils import combine_pools_info


tvl_balance = 10


def get_list_top_pools(network_id):
    # query_list_pools_url = "https://indexer.ref.finance/list-top-pools"
    # requests.packages.urllib3.disable_warnings()
    # list_pools_data_ret = requests.get(url=query_list_pools_url, verify=False)
    # list_pools_data_list = json.loads(list_pools_data_ret.text)
    # return list_pools_data_list
    pools = list_top_pools(network_id)
    prices = list_token_price(network_id)
    metadata = list_token_metadata(network_id)
    combine_pools_info(pools, prices, metadata)
    return pools


def handle_list_pool_data(network_id, list_pools_data_list):
    insert_pools_list = []
    pool_ids = {}
    rated_pool_ids = []
    stable_pool_ids = []
    for list_pools_data in list_pools_data_list:
        if list_pools_data["id"] in Cfg.TOKEN_FLOW_BLACK_LIST:
            continue
        if list_pools_data["pool_kind"] == "RATED_SWAP":
            rated_pool_ids.append(list_pools_data["id"])
        elif list_pools_data["pool_kind"] == "STABLE_SWAP":
            stable_pool_ids.append(list_pools_data["id"])
    pool_ids["rated_pool"] = rated_pool_ids
    pool_ids["stable_pool"] = stable_pool_ids
    stable_and_rated_pool_data = get_stable_and_rated_pool(network_id, pool_ids)
    for list_pools_data in list_pools_data_list:
        if list_pools_data["id"] in Cfg.TOKEN_FLOW_BLACK_LIST:
            continue
        pool_data = {"pool_id": list_pools_data["id"], "token_one": list_pools_data["token_account_ids"][0],
                     "token_two": list_pools_data["token_account_ids"][1], "token_three": "",
                     "token_one_symbol": list_pools_data["token_symbols"][0],
                     "token_two_symbol": list_pools_data["token_symbols"][1], "token_three_symbol": "",
                     "token_one_amount": list_pools_data["amounts"][0], "total_fee": list_pools_data["total_fee"],
                     "token_two_amount": list_pools_data["amounts"][1], "token_three_amount": "",
                     "tvl": list_pools_data["tvl"], "pool_kind": list_pools_data["pool_kind"], "amp": 0,
                     "rates": [0, 0], "token_account_ids": list_pools_data["token_account_ids"],
                     "stable_pool_decimal": 24, "three_c_amount": ""}

        if list_pools_data["pool_kind"] == "RATED_SWAP" or list_pools_data["pool_kind"] == "STABLE_SWAP":
            pool_date_detail = stable_and_rated_pool_data[list_pools_data["id"]]
            pool_data["token_one_amount"] = pool_date_detail["c_amounts"][0]
            pool_data["token_two_amount"] = pool_date_detail["c_amounts"][1]
            pool_data["amp"] = pool_date_detail["amp"]
            pool_data["rates"] = pool_date_detail["rates"]
            if len(list_pools_data["amounts"]) > 2:
                pool_data["token_three_amount"] = pool_date_detail["c_amounts"][2]
                pool_data["three_c_amount"] = pool_date_detail["c_amounts"]
            if list_pools_data["pool_kind"] == "STABLE_SWAP":
                pool_data["stable_pool_decimal"] = 18
        if len(list_pools_data["token_account_ids"]) > 2:
            pool_data["token_three"] = list_pools_data["token_account_ids"][2]
        if len(list_pools_data["token_symbols"]) > 2:
            pool_data["token_three_symbol"] = list_pools_data["token_symbols"][2]
        if len(list_pools_data["amounts"]) > 2:
            pool_data["token_three_amount"] = list_pools_data["amounts"][2]
        if int(pool_data["token_one_amount"]) > 0 and int(pool_data["token_two_amount"]) > 0 and float(
                pool_data["tvl"]) > tvl_balance:
            if len(list_pools_data["amounts"]) > 2 and int(pool_data["token_three_amount"]) <= 0:
                continue
            insert_pools_list.append(pool_data)

    return insert_pools_list


def handle_token_pair():
    token_pair_list = []
    whitelist_token = ["wrap.near", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near", "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near", "6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near", "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.factory.bridge.near", "111111111117dc0aa78b770fa6a738034120c302.factory.bridge.near", "c944e90c64b2c07662a292be6244bdf05cda44a7.factory.bridge.near", "usdt.tether-token.near", "berryclub.ek.near", "farm.berryclub.ek.near", "6f259637dcd74c767781e37bc6133cd6a68aa161.factory.bridge.near", "de30da39c46104798bb5aa3fe8b9e0e1f348163f.factory.bridge.near", "1f9840a85d5af5bf1d1762f925bdaddc4201f984.factory.bridge.near", "2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near", "514910771af9ca656af840dff83e8264ecf986ca.factory.bridge.near", "f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near", "token.v2.ref-finance.near", "d9c2d319cd7e6177336b0a9c93c21cb48d84fb54.factory.bridge.near", "token.paras.near", "a4ef4b0b23c1fc81d3f9ecf93510e64f58a4a016.factory.bridge.near", "marmaj.tkn.near", "meta-pool.near", "token.cheddar.near", "52a047ee205701895ee06a375492490ec9c597ce.factory.bridge.near", "aurora", "pixeltoken.near", "dbio.near", "aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near", "meta-token.near", "v1.dacha-finance.near", "3ea8ea4237344c9931214796d9417af1a1180770.factory.bridge.near", "e99de844ef3ef72806cf006224ef3b813e82662f.factory.bridge.near", "v3.oin_finance.near", "9aeb50f542050172359a0e1a25a9933bc8c01259.factory.bridge.near", "myriadcore.near", "xtoken.ref-finance.near", "sol.token.a11bd.near", "ust.token.a11bd.near", "luna.token.a11bd.near", "celo.token.a11bd.near", "cusd.token.a11bd.near", "abr.a11bd.near", "utopia.secretskelliessociety.near", "deip-token.near", "4691937a7508860f876c9c0a2a617e7d9e945d4b.factory.bridge.near", "linear-protocol.near", "usn", "0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near", "token.pembrock.near", "atocha-token.near", "token.stlb.near", "far.tokens.fewandfar.near", "059a1f1dea1020297588c316ffc30a58a1a0d4a2.factory.bridge.near", "token.burrow.near", "fusotao-token.near", "v2-nearx.stader-labs.near", "discovol-token.near", "30d20208d987713f46dfd34ef128bb16c404d10f.factory.bridge.near", "token.sweat", "apys.token.a11bd.near", "ftv2.nekotoken.near", "phoenix-bonds.near"]
    # whitelist_token = ["wrap.near", "usdt.tether-token.near"]
    for token_one in whitelist_token:
        for token_two in whitelist_token:
            if token_one != token_two:
                token_pair_list.append(token_one + "->" + token_two)
    return token_pair_list


def query_three_pools(token_one, token_two, token_three, list_pool_data):
    ret_data_list = []
    for pool_data in list_pool_data:
        if (pool_data["token_one"] == token_one or pool_data["token_two"] == token_one or pool_data[
            "token_three"] == token_one) and pool_data["token_one"] != token_two \
                and pool_data["token_two"] != token_two and pool_data["token_three"] != token_two \
                and pool_data["token_one"] != token_three and pool_data["token_two"] != token_three \
                and pool_data["token_three"] != token_three:
            ret_data_list.append(pool_data)
    return ret_data_list


def query_two_pools(token_one, token_two, list_pool_data):
    ret_data_list = []
    for pool_data in list_pool_data:
        if (pool_data["token_one"] == token_one or pool_data["token_two"] == token_one or pool_data["token_three"] == token_one) and pool_data["token_one"] != token_two and pool_data["token_two"] != token_two and pool_data["token_three"] != token_two:
            ret_data_list.append(pool_data)
    return ret_data_list


def query_one_pools(token_one, token_two, list_pool_data):
    ret_data_list = []
    for pool_data in list_pool_data:
        if (pool_data["token_one"] == token_one or pool_data["token_two"] == token_one or pool_data["token_three"] == token_one) and (pool_data["token_one"] == token_two or pool_data["token_two"] == token_two or pool_data["token_three"] == token_two):
            ret_data_list.append(pool_data)
    return ret_data_list


def handle_flow_grade(list_pool_data):
    decimals_data = get_token_decimal()
    token_flow_insert_all_data_list = []
    token_pair_list = handle_token_pair()
    for token_pair in token_pair_list:
        token_pair_one = token_pair.split("->")[0]
        token_pair_two = token_pair.split("->")[1]
        token_pair_one_data_list = query_one_pools(token_pair_one, token_pair_two, list_pool_data)
        for token_pair_one_data in token_pair_one_data_list:
            token_flow_insert_data = {
                "token_pair": token_pair,
                "grade": "1",
                "pool_ids": json.dumps([token_pair_one_data["pool_id"]]),
                "token_in": "",
                "token_in_amount": "0",
                "revolve_token_one": "",
                "revolve_token_two": "",
                "token_out": "",
                "token_out_amount": "0",
                "token_in_symbol": "",
                "revolve_token_one_symbol": "",
                "revolve_token_two_symbol": "",
                "revolve_one_out_amount": "0",
                "revolve_one_in_amount": "0",
                "revolve_two_out_amount": "0",
                "revolve_two_in_amount": "0",
                "token_out_symbol": "",
                "token_pair_ratio": 0.00,
                "revolve_token_one_ratio": 0.00,
                "revolve_token_two_ratio": 0.00,
                "final_ratio": 0.00,
                "pool_fee": token_pair_one_data["total_fee"],
                "revolve_one_pool_fee": 0,
                "revolve_two_pool_fee": 0,
                "pool_kind": token_pair_one_data["pool_kind"],
                "revolve_one_pool_kind": "",
                "revolve_two_pool_kind": "",
                "three_c_amount": "[]",
                "three_pool_ids": "[]",
                "amp": 0,
                "revolve_one_pool_amp": 0,
                "revolve_two_pool_amp": 0,
                "rates": "[]",
                "revolve_one_pool_rates": "[]",
                "revolve_two_pool_rates": "[]",
                "pool_token_number": "2",
                "revolve_one_pool_token_number": "2",
                "revolve_two_pool_token_number": "2",
            }
            if token_pair_one_data["token_one"] == token_pair_one and token_pair_one_data["token_two"] == token_pair_two:
                token_flow_insert_data["token_in"] = token_pair_one_data["token_one"]
                token_flow_insert_data["token_out"] = token_pair_one_data["token_two"]
                token_flow_insert_data["token_in_symbol"] = token_pair_one_data["token_one_symbol"]
                token_flow_insert_data["token_out_symbol"] = token_pair_one_data["token_two_symbol"]
                token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_one_amount"]
                token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_two_amount"]
                token_in_symbol = token_pair_one_data["token_one_symbol"]
                token_out_symbol = token_pair_one_data["token_two_symbol"]
            elif token_pair_one_data["token_one"] == token_pair_one and token_pair_one_data["token_three"] == token_pair_two:
                token_flow_insert_data["token_in"] = token_pair_one_data["token_one"]
                token_flow_insert_data["token_out"] = token_pair_one_data["token_three"]
                token_flow_insert_data["token_in_symbol"] = token_pair_one_data["token_one_symbol"]
                token_flow_insert_data["token_out_symbol"] = token_pair_one_data["token_three_symbol"]
                token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_one_amount"]
                token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_three_amount"]
                token_in_symbol = token_pair_one_data["token_one_symbol"]
                token_out_symbol = token_pair_one_data["token_three_symbol"]
            elif token_pair_one_data["token_two"] == token_pair_one and token_pair_one_data["token_one"] == token_pair_two:
                token_flow_insert_data["token_in"] = token_pair_one_data["token_two"]
                token_flow_insert_data["token_out"] = token_pair_one_data["token_one"]
                token_flow_insert_data["token_in_symbol"] = token_pair_one_data["token_two_symbol"]
                token_flow_insert_data["token_out_symbol"] = token_pair_one_data["token_one_symbol"]
                token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_two_amount"]
                token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_one_amount"]
                token_in_symbol = token_pair_one_data["token_two_symbol"]
                token_out_symbol = token_pair_one_data["token_one_symbol"]
            elif token_pair_one_data["token_two"] == token_pair_one and token_pair_one_data["token_three"] == token_pair_two:
                token_flow_insert_data["token_in"] = token_pair_one_data["token_two"]
                token_flow_insert_data["token_out"] = token_pair_one_data["token_three"]
                token_flow_insert_data["token_in_symbol"] = token_pair_one_data["token_two_symbol"]
                token_flow_insert_data["token_out_symbol"] = token_pair_one_data["token_three_symbol"]
                token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_two_amount"]
                token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_three_amount"]
                token_in_symbol = token_pair_one_data["token_two_symbol"]
                token_out_symbol = token_pair_one_data["token_three_symbol"]
            elif token_pair_one_data["token_three"] == token_pair_one and token_pair_one_data["token_one"] == token_pair_two:
                token_flow_insert_data["token_in"] = token_pair_one_data["token_three"]
                token_flow_insert_data["token_out"] = token_pair_one_data["token_one"]
                token_flow_insert_data["token_in_symbol"] = token_pair_one_data["token_three_symbol"]
                token_flow_insert_data["token_out_symbol"] = token_pair_one_data["token_one_symbol"]
                token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_three_amount"]
                token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_one_amount"]
                token_in_symbol = token_pair_one_data["token_three_symbol"]
                token_out_symbol = token_pair_one_data["token_one_symbol"]
            elif token_pair_one_data["token_three"] == token_pair_one and token_pair_one_data["token_two"] == token_pair_two:
                token_flow_insert_data["token_in"] = token_pair_one_data["token_three"]
                token_flow_insert_data["token_out"] = token_pair_one_data["token_two"]
                token_flow_insert_data["token_in_symbol"] = token_pair_one_data["token_three_symbol"]
                token_flow_insert_data["token_out_symbol"] = token_pair_one_data["token_two_symbol"]
                token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_three_amount"]
                token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_two_amount"]
                token_in_symbol = token_pair_one_data["token_three_symbol"]
                token_out_symbol = token_pair_one_data["token_two_symbol"]
            if token_flow_insert_data["token_in"] in decimals_data and token_flow_insert_data["token_out"] in decimals_data:
                if token_pair_one_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_insert_data["token_in_amount"]) / int("1" + "0" * decimals_data[token_flow_insert_data["token_in"]])
                    token_out_balance = int(token_flow_insert_data["token_out_amount"]) / int("1" + "0" * decimals_data[token_flow_insert_data["token_out"]])
                    token_pair_ratio = get_token_flow_ratio(1, token_in_balance, token_out_balance, token_pair_one_data["total_fee"])
                    token_flow_insert_data["token_pair_ratio"] = token_pair_ratio
                    token_flow_insert_data["final_ratio"] = token_pair_ratio
                    token_flow_insert_data["token_in_amount"] = token_in_balance
                    token_flow_insert_data["token_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_insert_data["token_in_amount"])
                    token_out_balance = int(token_flow_insert_data["token_out_amount"])
                    # print("token_pair:", token_pair)
                    c_amounts = [token_in_balance, token_out_balance]
                    if len(token_pair_one_data["token_account_ids"]) > 2:
                        c_amounts = token_pair_one_data["three_c_amount"]
                        token_flow_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_insert_data["three_pool_ids"] = json.dumps(token_pair_one_data["token_account_ids"])
                        token_flow_insert_data["pool_token_number"] = "3"
                    stable_pool = {"amp": token_pair_one_data["amp"], "total_fee": token_pair_one_data["total_fee"],
                                   "token_account_ids": token_pair_one_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_pair_one_data["rates"]}
                    token_pair_ratio = get_swapped_amount(token_pair_one, token_pair_two, 1, stable_pool, token_pair_one_data["stable_pool_decimal"])
                    token_flow_insert_data["token_pair_ratio"] = token_pair_ratio
                    token_flow_insert_data["final_ratio"] = token_pair_ratio
                    token_flow_insert_data["token_in_amount"] = token_in_balance
                    token_flow_insert_data["token_out_amount"] = token_out_balance
                    token_flow_insert_data["amp"] = token_pair_one_data["amp"]
                    token_flow_insert_data["rates"] = json.dumps(token_pair_one_data["rates"])
            else:
                continue
            token_flow_insert_all_data_list.append(token_flow_insert_data)
            handle_grade_two(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol, list_pool_data, token_flow_insert_all_data_list)
        if len(token_pair_one_data_list) < 1:
            handle_grade_two(token_pair, token_pair_one, token_pair_two, "", "",
                             list_pool_data, token_flow_insert_all_data_list)
    return token_flow_insert_all_data_list


def handle_grade_two(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol, list_pool_data, token_flow_insert_all_data_list):
    decimals_data = get_token_decimal()
    token_two_data_list = query_two_pools(token_pair_one, token_pair_two, list_pool_data)
    for token_two_data in token_two_data_list:
        if token_two_data["token_one"] == token_pair_one:
            revolve_token_one = token_two_data["token_two"]
            token_in_amount = token_two_data["token_one_amount"]
            revolve_one_out_amount = token_two_data["token_two_amount"]
        else:
            revolve_token_one = token_two_data["token_one"]
            token_in_amount = token_two_data["token_two_amount"]
            revolve_one_out_amount = token_two_data["token_one_amount"]
        token_one_data_list = query_one_pools(revolve_token_one, token_pair_two, list_pool_data)
        total_fee = token_two_data["total_fee"]
        for token_one_data in token_one_data_list:
            pool_ids = [token_two_data["pool_id"], token_one_data["pool_id"]]
            token_flow_two_insert_data = {
                "token_pair": token_pair,
                "grade": "2",
                "pool_ids": json.dumps(pool_ids),
                "token_in": token_pair_one,
                "token_in_amount": token_in_amount,
                "revolve_token_one": revolve_token_one,
                "revolve_token_two": "",
                "token_out": token_pair_two,
                "token_out_amount": "0",
                "token_in_symbol": token_in_symbol,
                "revolve_token_one_symbol": "",
                "revolve_token_two_symbol": "",
                "revolve_one_out_amount": revolve_one_out_amount,
                "revolve_one_in_amount": "0",
                "revolve_two_out_amount": "0",
                "revolve_two_in_amount": "0",
                "token_out_symbol": token_out_symbol,
                "token_pair_ratio": 0.00,
                "revolve_token_one_ratio": 0.00,
                "revolve_token_two_ratio": 0.00,
                "final_ratio": 0.00,
                "pool_fee": token_two_data["total_fee"],
                "revolve_one_pool_fee": token_one_data["total_fee"],
                "revolve_two_pool_fee": 0,
                "pool_kind": token_two_data["pool_kind"],
                "revolve_one_pool_kind": token_one_data["pool_kind"],
                "revolve_two_pool_kind": "",
                "three_c_amount": "[]",
                "three_pool_ids": "[]",
                "amp": 0,
                "revolve_one_pool_amp": 0,
                "revolve_two_pool_amp": 0,
                "rates": "[]",
                "revolve_one_pool_rates": "[]",
                "revolve_two_pool_rates": "[]",
                "pool_token_number": "2",
                "revolve_one_pool_token_number": "2",
                "revolve_two_pool_token_number": "2",
            }
            if token_one_data["token_one"] == revolve_token_one and token_one_data["token_two"] == token_pair_two:
                token_flow_two_insert_data["revolve_token_one_symbol"] = token_one_data["token_one_symbol"]
                token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_one_amount"]
                token_flow_two_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
                revolve_token_one_symbol = token_one_data["token_one_symbol"]
            elif token_one_data["token_one"] == revolve_token_one and token_one_data["token_three"] == token_pair_two:
                token_flow_two_insert_data["revolve_token_one_symbol"] = token_one_data["token_one_symbol"]
                token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_one_amount"]
                token_flow_two_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
                revolve_token_one_symbol = token_one_data["token_one_symbol"]
            elif token_one_data["token_two"] == revolve_token_one and token_one_data["token_one"] == token_pair_two:
                token_flow_two_insert_data["revolve_token_one_symbol"] = token_one_data["token_two_symbol"]
                token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_two_amount"]
                token_flow_two_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
                revolve_token_one_symbol = token_one_data["token_two_symbol"]
            elif token_one_data["token_two"] == revolve_token_one and token_one_data["token_three"] == token_pair_two:
                token_flow_two_insert_data["revolve_token_one_symbol"] = token_one_data["token_two_symbol"]
                token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_two_amount"]
                token_flow_two_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
                revolve_token_one_symbol = token_one_data["token_two_symbol"]
            elif token_one_data["token_three"] == revolve_token_one and token_one_data["token_one"] == token_pair_two:
                token_flow_two_insert_data["revolve_token_one_symbol"] = token_one_data["token_three_symbol"]
                token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_three_amount"]
                token_flow_two_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
                revolve_token_one_symbol = token_one_data["token_three_symbol"]
            elif token_one_data["token_three"] == revolve_token_one and token_one_data["token_two"] == token_pair_two:
                token_flow_two_insert_data["revolve_token_one_symbol"] = token_one_data["token_three_symbol"]
                token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_three_amount"]
                token_flow_two_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
                revolve_token_one_symbol = token_one_data["token_three_symbol"]
            if token_flow_two_insert_data["token_in"] in decimals_data and token_flow_two_insert_data["revolve_token_one"] in decimals_data:
                if token_two_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_two_insert_data["token_in_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["token_in"]])
                    token_out_balance = int(token_flow_two_insert_data["revolve_one_out_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["revolve_token_one"]])
                    token_flow_two_insert_data["token_pair_ratio"] = get_token_flow_ratio(1, token_in_balance, token_out_balance, total_fee)
                    token_flow_two_insert_data["token_in_amount"] = token_in_balance
                    token_flow_two_insert_data["revolve_one_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_two_insert_data["token_in_amount"])
                    token_out_balance = int(token_flow_two_insert_data["revolve_one_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    if len(token_two_data["token_account_ids"]) > 2:
                        c_amounts = token_two_data["three_c_amount"]
                        token_flow_two_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_two_insert_data["three_pool_ids"] = json.dumps(token_two_data["token_account_ids"])
                        token_flow_two_insert_data["pool_token_number"] = "3"
                    stable_pool = {"amp": token_two_data["amp"], "total_fee": token_two_data["total_fee"],
                                   "token_account_ids": token_two_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_two_data["rates"]}
                    token_pair_ratio = get_swapped_amount(token_flow_two_insert_data["token_in"],
                                                          token_flow_two_insert_data["revolve_token_one"],
                                                          1, stable_pool, token_two_data["stable_pool_decimal"])
                    token_flow_two_insert_data["token_pair_ratio"] = token_pair_ratio
                    token_flow_two_insert_data["token_in_amount"] = token_in_balance
                    token_flow_two_insert_data["revolve_one_out_amount"] = token_out_balance
                    token_flow_two_insert_data["amp"] = token_two_data["amp"]
                    token_flow_two_insert_data["rates"] = json.dumps(token_two_data["rates"])
            else:
                continue
            if token_flow_two_insert_data["revolve_token_one"] in decimals_data and token_flow_two_insert_data["token_out"] in decimals_data:
                if token_one_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_two_insert_data["revolve_one_in_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["revolve_token_one"]])
                    token_out_balance = int(token_flow_two_insert_data["token_out_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["token_out"]])
                    token_flow_two_insert_data["revolve_token_one_ratio"] = get_token_flow_ratio(1, token_in_balance, token_out_balance, token_one_data["total_fee"])
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_two_insert_data["token_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_two_insert_data["revolve_one_in_amount"])
                    token_out_balance = int(token_flow_two_insert_data["token_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    if len(token_one_data["token_account_ids"]) > 2:
                        c_amounts = token_one_data["three_c_amount"]
                        token_flow_two_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_two_insert_data["three_pool_ids"] = json.dumps(token_one_data["token_account_ids"])
                        token_flow_two_insert_data["revolve_one_pool_token_number"] = "3"
                    stable_pool = {"amp": token_one_data["amp"], "total_fee": token_one_data["total_fee"],
                                   "token_account_ids": token_one_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_one_data["rates"]}
                    token_pair_ratio = get_swapped_amount(token_one_data["token_account_ids"][0],
                                                          token_one_data["token_account_ids"][1], 1, stable_pool,
                                                          token_one_data["stable_pool_decimal"])
                    token_flow_two_insert_data["revolve_token_one_ratio"] = token_pair_ratio
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_two_insert_data["token_out_amount"] = token_out_balance
                    token_flow_two_insert_data["revolve_one_pool_amp"] = token_one_data["amp"]
                    token_flow_two_insert_data["revolve_one_pool_rates"] = json.dumps(token_one_data["rates"])
            else:
                continue
            final_ratio = token_flow_two_insert_data["revolve_token_one_ratio"] * token_flow_two_insert_data["token_pair_ratio"]
            token_flow_two_insert_data["final_ratio"] = '%.6f' % final_ratio
            token_flow_insert_all_data_list.append(token_flow_two_insert_data)
            handle_grade_three(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol,
                               token_two_data["pool_id"], revolve_token_one, revolve_token_one_symbol,
                               token_in_amount, revolve_one_out_amount, total_fee, list_pool_data,
                               token_flow_insert_all_data_list, token_two_data["total_fee"], token_two_data["pool_kind"], token_two_data)


def handle_grade_three(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol, pool_id,
                       revolve_token_one, revolve_token_one_symbol, token_in_amount, revolve_one_out_amount, total_fee,
                       list_pool_data, token_flow_insert_all_data_list, pool_fee, pool_kind, one_pool_data):
    decimals_data = get_token_decimal()
    token_three_data_list = query_three_pools(revolve_token_one, token_pair_two, token_pair_one, list_pool_data)
    for token_three_data in token_three_data_list:
        if token_three_data["token_one"] == revolve_token_one:
            revolve_token_two = token_three_data["token_two"]
            revolve_one_in_amount = token_three_data["token_one_amount"]
            revolve_two_out_amount = token_three_data["token_two_amount"]
        elif token_three_data["token_two"] == revolve_token_one:
            revolve_token_two = token_three_data["token_one"]
            revolve_one_in_amount = token_three_data["token_two_amount"]
            revolve_two_out_amount = token_three_data["token_one_amount"]
        elif token_three_data["token_three"] == revolve_token_one:
            revolve_token_two = token_three_data["token_one"]
            revolve_one_in_amount = token_three_data["token_three_amount"]
            revolve_two_out_amount = token_three_data["token_two_amount"]
        token_one_data_list = query_one_pools(revolve_token_two, token_pair_two, list_pool_data)
        for token_one_data in token_one_data_list:
            insert_three_pool_ids = [pool_id, token_three_data["pool_id"], token_one_data["pool_id"]]
            token_flow_three_insert_data = {
                "token_pair": token_pair,
                "grade": "3",
                "pool_ids": json.dumps(insert_three_pool_ids),
                "token_in": token_pair_one,
                "token_in_amount": token_in_amount,
                "revolve_token_one": revolve_token_one,
                "revolve_token_two": revolve_token_two,
                "token_out": token_pair_two,
                "token_out_amount": "0",
                "token_in_symbol": token_in_symbol,
                "revolve_token_one_symbol": revolve_token_one_symbol,
                "revolve_token_two_symbol": "",
                "revolve_one_out_amount": revolve_one_out_amount,
                "revolve_one_in_amount": revolve_one_in_amount,
                "revolve_two_out_amount": revolve_two_out_amount,
                "revolve_two_in_amount": "0",
                "token_out_symbol": token_out_symbol,
                "token_pair_ratio": 0.00,
                "revolve_token_one_ratio": 0.00,
                "revolve_token_two_ratio": 0.00,
                "final_ratio": 0.00,
                "pool_fee": pool_fee,
                "revolve_one_pool_fee": token_three_data["total_fee"],
                "revolve_two_pool_fee": token_one_data["total_fee"],
                "pool_kind": pool_kind,
                "revolve_one_pool_kind": token_three_data["pool_kind"],
                "revolve_two_pool_kind": token_one_data["pool_kind"],
                "three_c_amount": "[]",
                "three_pool_ids": "[]",
                "amp": 0,
                "revolve_one_pool_amp": 0,
                "revolve_two_pool_amp": 0,
                "rates": "[]",
                "revolve_one_pool_rates": "[]",
                "revolve_two_pool_rates": "[]",
                "pool_token_number": "2",
                "revolve_one_pool_token_number": "2",
                "revolve_two_pool_token_number": "2",
            }
            if token_one_data["token_one"] == revolve_token_two and token_one_data["token_two"] == token_pair_two:
                token_flow_three_insert_data["revolve_token_two_symbol"] = token_one_data["token_one_symbol"]
                token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_one_amount"]
                token_flow_three_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
            elif token_one_data["token_one"] == revolve_token_two and token_one_data["token_three"] == token_pair_two:
                token_flow_three_insert_data["revolve_token_two_symbol"] = token_one_data["token_one_symbol"]
                token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_one_amount"]
                token_flow_three_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
            elif token_one_data["token_two"] == revolve_token_two and token_one_data["token_one"] == token_pair_two:
                token_flow_three_insert_data["revolve_token_two_symbol"] = token_one_data["token_two_symbol"]
                token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_two_amount"]
                token_flow_three_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
            elif token_one_data["token_two"] == revolve_token_two and token_one_data["token_three"] == token_pair_two:
                token_flow_three_insert_data["revolve_token_two_symbol"] = token_one_data["token_two_symbol"]
                token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_two_amount"]
                token_flow_three_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
            elif token_one_data["token_three"] == revolve_token_two and token_one_data["token_one"] == token_pair_two:
                token_flow_three_insert_data["revolve_token_two_symbol"] = token_one_data["token_three_symbol"]
                token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_three_amount"]
                token_flow_three_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
            elif token_one_data["token_three"] == revolve_token_two and token_one_data["token_two"] == token_pair_two:
                token_flow_three_insert_data["revolve_token_two_symbol"] = token_one_data["token_three_symbol"]
                token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_three_amount"]
                token_flow_three_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
            if token_flow_three_insert_data["token_in"] in decimals_data and token_flow_three_insert_data["revolve_token_one"] in decimals_data:
                if one_pool_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_three_insert_data["token_in_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["token_in"]])
                    token_out_balance = int(token_flow_three_insert_data["revolve_one_out_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_one"]])
                    token_flow_three_insert_data["token_pair_ratio"] = get_token_flow_ratio(1, token_in_balance, token_out_balance, total_fee)
                    token_flow_three_insert_data["token_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_one_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_three_insert_data["token_in_amount"])
                    token_out_balance = int(token_flow_three_insert_data["revolve_one_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    if len(one_pool_data["token_account_ids"]) > 2:
                        c_amounts = one_pool_data["three_c_amount"]
                        token_flow_three_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_three_insert_data["three_pool_ids"] = json.dumps(one_pool_data["token_account_ids"])
                        token_flow_three_insert_data["pool_token_number"] = "3"
                    stable_pool = {"amp": one_pool_data["amp"], "total_fee": one_pool_data["total_fee"],
                                   "token_account_ids": one_pool_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": one_pool_data["rates"]}
                    token_pair_ratio = get_swapped_amount(one_pool_data["token_account_ids"][0],
                                                          one_pool_data["token_account_ids"][1],
                                                          1, stable_pool, one_pool_data["stable_pool_decimal"])
                    token_flow_three_insert_data["token_pair_ratio"] = token_pair_ratio
                    token_flow_three_insert_data["token_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_one_out_amount"] = token_out_balance
                    token_flow_three_insert_data["amp"] = one_pool_data["amp"]
                    token_flow_three_insert_data["rates"] = json.dumps(one_pool_data["rates"])
            else:
                continue
            if token_flow_three_insert_data["revolve_token_one"] in decimals_data and token_flow_three_insert_data["revolve_token_two"] in decimals_data:
                if token_three_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_three_insert_data["revolve_one_in_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_one"]])
                    token_out_balance = int(token_flow_three_insert_data["revolve_two_out_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_two"]])
                    token_flow_three_insert_data["revolve_token_one_ratio"] = get_token_flow_ratio(1, token_in_balance, token_out_balance, token_three_data["total_fee"])
                    token_flow_three_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_two_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_three_insert_data["revolve_one_in_amount"])
                    token_out_balance = int(token_flow_three_insert_data["revolve_two_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    if len(token_three_data["token_account_ids"]) > 2:
                        c_amounts = token_three_data["three_c_amount"]
                        token_flow_three_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_three_insert_data["three_pool_ids"] = json.dumps(token_three_data["token_account_ids"])
                        token_flow_three_insert_data["revolve_one_pool_token_number"] = "3"
                    stable_pool = {"amp": token_three_data["amp"], "total_fee": token_three_data["total_fee"],
                                   "token_account_ids": token_three_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_three_data["rates"]}
                    revolve_token_one_ratio = get_swapped_amount(token_three_data["token_account_ids"][0],
                                                          token_three_data["token_account_ids"][1],
                                                          1, stable_pool, token_three_data["stable_pool_decimal"])
                    token_flow_three_insert_data["revolve_token_one_ratio"] = revolve_token_one_ratio
                    token_flow_three_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_two_out_amount"] = token_out_balance
                    token_flow_three_insert_data["revolve_one_pool_amp"] = token_three_data["amp"]
                    token_flow_three_insert_data["revolve_one_pool_rates"] = json.dumps(token_three_data["rates"])
            else:
                continue
            if token_flow_three_insert_data["revolve_token_two"] in decimals_data and token_flow_three_insert_data["token_out"] in decimals_data:
                if token_one_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_three_insert_data["revolve_two_in_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_two"]])
                    token_out_balance = int(token_flow_three_insert_data["token_out_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["token_out"]])
                    token_flow_three_insert_data["revolve_token_two_ratio"] = get_token_flow_ratio(1, token_in_balance, token_out_balance, token_one_data["total_fee"])
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_in_balance
                    token_flow_three_insert_data["token_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_three_insert_data["revolve_two_in_amount"])
                    token_out_balance = int(token_flow_three_insert_data["token_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    if len(token_one_data["token_account_ids"]) > 2:
                        c_amounts = token_one_data["three_c_amount"]
                        token_flow_three_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_three_insert_data["three_pool_ids"] = json.dumps(token_one_data["token_account_ids"])
                        token_flow_three_insert_data["revolve_two_pool_token_number"] = "3"
                    stable_pool = {"amp": token_one_data["amp"], "total_fee": token_one_data["total_fee"],
                                   "token_account_ids": token_one_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_one_data["rates"]}
                    revolve_token_two_ratio = get_swapped_amount(token_one_data["token_account_ids"][0],
                                                                 token_one_data["token_account_ids"][1],
                                                                 1, stable_pool,
                                                                 token_one_data["stable_pool_decimal"])
                    token_flow_three_insert_data["revolve_token_two_ratio"] = revolve_token_two_ratio
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_in_balance
                    token_flow_three_insert_data["token_out_amount"] = token_out_balance
                    token_flow_three_insert_data["revolve_two_pool_amp"] = token_one_data["amp"]
                    token_flow_three_insert_data["revolve_two_pool_rates"] = json.dumps(token_one_data["rates"])
            else:
                continue
            final_ratio = token_flow_three_insert_data["token_pair_ratio"] * token_flow_three_insert_data["revolve_token_one_ratio"] * token_flow_three_insert_data["revolve_token_two_ratio"]
            token_flow_three_insert_data["final_ratio"] = '%.6f' % final_ratio
            token_flow_insert_all_data_list.append(token_flow_three_insert_data)


def get_token_decimal():
    decimals = {}
    for token in Cfg.TOKENS["MAINNET"]:
        decimals[token["NEAR_ID"]] = token["DECIMAL"]
    decimals["usn"] = 18
    decimals["usdt.tether-token.near"] = 6
    decimals["rftt.tkn.near"] = 8
    return decimals


if __name__ == "__main__":
    print("#########TOKEN FLOW START###########")

    if len(sys.argv) == 2:
        start_time = int(time.time())
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            start_time = int(time.time())
            list_top_pools_data = get_list_top_pools(network_id)
            end_time1 = int(time.time())
            print("get_list_top_pools consuming:", end_time1 - start_time)
            pools_data_list = handle_list_pool_data(network_id, list_top_pools_data)
            end_time2 = int(time.time())
            print("handle_list_pool_data consuming:", end_time2 - end_time1)
            token_flow_insert_data_list = handle_flow_grade(pools_data_list)
            end_time3 = int(time.time())
            print("handle_flow_grade consuming:", end_time3 - end_time2)
            add_token_flow(network_id, token_flow_insert_data_list)
            end_time = int(time.time())
            print("add_token_flow consuming:", end_time - end_time3)
            print("total consuming:", end_time - start_time)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("#########TOKEN FLOW START###########")

    # start_time = int(time.time())
    # list_top_pools_data = get_list_top_pools("MAINNET")
    # end_time1 = int(time.time())
    # print("get_list_top_pools consuming:", end_time1 - start_time)
    # pools_data_list = handle_list_pool_data("MAINNET", list_top_pools_data)
    # end_time2 = int(time.time())
    # print("handle_list_pool_data consuming:", end_time2 - end_time1)
    # token_flow_insert_data_list = handle_flow_grade(pools_data_list)
    # end_time3 = int(time.time())
    # print("handle_flow_grade consuming:", end_time3 - end_time2)
    # add_token_flow("MAINNET", token_flow_insert_data_list)
    # end_time = int(time.time())
    # print("add_token_flow consuming:", end_time - end_time3)
    # print("total consuming:", end_time - start_time)

    # print("")
    # token_in_amount = 100
    # token_in_balance = float(64.585966)
    # token_out_balance = float(64.541357)
    # fee = 5
    # ratio_ret = get_token_flow_ratio(token_in_amount, token_in_balance, token_out_balance, fee)
    # print("ratio:", ratio_ret)

    # start_time = int(time.time())
    # # pool_data = get_stable_pool("MAINNET", )
    # pool_ids = {"rated_pool": ["3514", "3689", "3515", "3688", "3612"], "stable_pool": ["3020", "3433", "3364", "1910"]}
    # pool_data = get_stable_and_rated_pool("MAINNET", pool_ids)
    # print(pool_data)
    # end_time1 = int(time.time())
    # print("get_stable_pool consuming:", end_time1 - start_time)
