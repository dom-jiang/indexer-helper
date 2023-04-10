import decimal
import sys

sys.path.append('../')
import requests
import json
import pymysql
from config import Cfg
import time


tvl_balance = 10


def get_db_connect():
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        passwd="root",
        db="ref")
    return conn


def get_list_top_pools():
    query_list_pools_url = "https://indexer.ref.finance/list-top-pools"
    requests.packages.urllib3.disable_warnings()
    list_pools_data_ret = requests.get(url=query_list_pools_url, verify=False)
    list_pools_data_list = json.loads(list_pools_data_ret.text)
    return list_pools_data_list


def handle_list_pool_data(list_pools_data_list):
    insert_pools_list = []
    for list_pools_data in list_pools_data_list:
        pool_data = {"pool_id": list_pools_data["id"], "token_one": list_pools_data["token_account_ids"][0],
                     "token_two": list_pools_data["token_account_ids"][1], "token_three": "",
                     "token_one_symbol": list_pools_data["token_symbols"][0],
                     "token_two_symbol": list_pools_data["token_symbols"][1], "token_three_symbol": "",
                     "token_one_amount": list_pools_data["amounts"][0], "total_fee": list_pools_data["total_fee"],
                     "token_two_amount": list_pools_data["amounts"][1], "token_three_amount": "",
                     "tvl": list_pools_data["tvl"]}
        if len(list_pools_data["token_account_ids"]) > 2:
            pool_data["token_three"] = list_pools_data["token_account_ids"][2]
        if len(list_pools_data["token_symbols"]) > 2:
            pool_data["token_three_symbol"] = list_pools_data["token_symbols"][2]
        if len(list_pools_data["amounts"]) > 2:
            pool_data["token_three_amount"] = list_pools_data["amounts"][2]
        if int(pool_data["token_one_amount"]) > 0 and int(pool_data["token_two_amount"]) > 0 and float(pool_data["tvl"]) > tvl_balance:
            if len(list_pools_data["amounts"]) > 2 and int(pool_data["token_three_amount"]) <= 0:
                continue
            insert_pools_list.append(pool_data)
    return insert_pools_list


def add_token_flow(data_list):
    db_conn = get_db_connect()
    sql = "update t_token_flow set states = '2' where states = '1'"

    sql1 = "insert into t_token_flow(token_pair, grade, pool_ids, token_in, revolve_token_one, revolve_token_two, " \
          "token_out, token_in_symbol, revolve_token_one_symbol, revolve_token_two_symbol, token_out_symbol, " \
          "token_in_amount, token_out_amount, revolve_one_out_amount, revolve_one_in_amount, revolve_two_out_amount, " \
          "revolve_two_in_amount, token_pair_ratio, revolve_token_one_ratio, revolve_token_two_ratio, final_ratio, " \
          "create_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["token_pair"], data["grade"], data["pool_ids"], data["token_in"],
                                data["revolve_token_one"], data["revolve_token_two"], data["token_out"],
                                data["token_in_symbol"], data["revolve_token_one_symbol"],
                                data["revolve_token_two_symbol"], data["token_out_symbol"],
                                data["token_in_amount"], data["token_out_amount"],
                                data["revolve_one_out_amount"], data["revolve_one_in_amount"],
                                data["revolve_two_out_amount"], data["revolve_two_in_amount"],
                                data["token_pair_ratio"], data["revolve_token_one_ratio"],
                                data["revolve_token_two_ratio"], data["final_ratio"]))

        cursor.execute(sql)
        db_conn.commit()
        cursor.executemany(sql1, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print("insert list pools order log to db error:", e)
        # logger.error("insert limit order log to db insert_data:{}", insert_data)
    finally:
        cursor.close()


def update_old_token_flow_data():
    db_conn = get_db_connect()
    sql = "update t_token_flow set states = '2' where states = '1'"
    cursor = db_conn.cursor()
    try:
        cursor.execute(sql)
        db_conn.commit()
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print("update_old_token_flow_data error:", e)
    finally:
        cursor.close()


def handle_token_pair():
    token_pair_list = []
    # ["token.v2.ref-finance.near", "wrap.near"]
    whitelist_token = ["wrap.near", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near", "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near", "6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near", "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.factory.bridge.near", "111111111117dc0aa78b770fa6a738034120c302.factory.bridge.near", "c944e90c64b2c07662a292be6244bdf05cda44a7.factory.bridge.near", "usdt.tether-token.near", "berryclub.ek.near", "farm.berryclub.ek.near", "6f259637dcd74c767781e37bc6133cd6a68aa161.factory.bridge.near", "de30da39c46104798bb5aa3fe8b9e0e1f348163f.factory.bridge.near", "1f9840a85d5af5bf1d1762f925bdaddc4201f984.factory.bridge.near", "2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near", "514910771af9ca656af840dff83e8264ecf986ca.factory.bridge.near", "f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near", "token.v2.ref-finance.near", "d9c2d319cd7e6177336b0a9c93c21cb48d84fb54.factory.bridge.near", "token.paras.near", "a4ef4b0b23c1fc81d3f9ecf93510e64f58a4a016.factory.bridge.near", "marmaj.tkn.near", "meta-pool.near", "token.cheddar.near", "52a047ee205701895ee06a375492490ec9c597ce.factory.bridge.near", "aurora", "pixeltoken.near", "dbio.near", "aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near", "meta-token.near", "v1.dacha-finance.near", "3ea8ea4237344c9931214796d9417af1a1180770.factory.bridge.near", "e99de844ef3ef72806cf006224ef3b813e82662f.factory.bridge.near", "v3.oin_finance.near", "9aeb50f542050172359a0e1a25a9933bc8c01259.factory.bridge.near", "myriadcore.near", "xtoken.ref-finance.near", "sol.token.a11bd.near", "ust.token.a11bd.near", "luna.token.a11bd.near", "celo.token.a11bd.near", "cusd.token.a11bd.near", "abr.a11bd.near", "utopia.secretskelliessociety.near", "deip-token.near", "4691937a7508860f876c9c0a2a617e7d9e945d4b.factory.bridge.near", "linear-protocol.near", "usn", "0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near", "token.pembrock.near", "atocha-token.near", "token.stlb.near", "far.tokens.fewandfar.near", "059a1f1dea1020297588c316ffc30a58a1a0d4a2.factory.bridge.near", "token.burrow.near", "fusotao-token.near", "v2-nearx.stader-labs.near", "discovol-token.near", "30d20208d987713f46dfd34ef128bb16c404d10f.factory.bridge.near", "token.sweat", "apys.token.a11bd.near", "ftv2.nekotoken.near", "phoenix-bonds.near"]
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
                "pool_ids": token_pair_one_data["pool_id"],
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
                token_in_balance = int(token_flow_insert_data["token_in_amount"]) / int("1" + "0" * decimals_data[token_flow_insert_data["token_in"]])
                token_out_balance = int(token_flow_insert_data["token_out_amount"]) / int("1" + "0" * decimals_data[token_flow_insert_data["token_out"]])
                token_pair_ratio = get_ratio(1, token_in_balance, token_out_balance, token_pair_one_data["total_fee"])
                token_flow_insert_data["token_pair_ratio"] = token_pair_ratio
                token_flow_insert_data["final_ratio"] = token_pair_ratio
                token_flow_insert_data["token_in_amount"] = token_in_balance
                token_flow_insert_data["token_out_amount"] = token_out_balance
            else:
                continue
            token_flow_insert_all_data_list.append(token_flow_insert_data)
            handle_grade_two(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol, list_pool_data, token_flow_insert_all_data_list)
        # print("token_pair:", token_pair)
    # add_token_flow(token_flow_one_insert_data_list)
    return token_flow_insert_all_data_list


def handle_grade_two(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol, list_pool_data, token_flow_insert_all_data_list):
    decimals_data = get_token_decimal()
    # token_flow_two_insert_data_list = []
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
                "pool_ids": str(pool_ids),
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
                token_in_balance = int(token_flow_two_insert_data["token_in_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["token_in"]])
                token_out_balance = int(token_flow_two_insert_data["revolve_one_out_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["revolve_token_one"]])
                token_flow_two_insert_data["token_pair_ratio"] = get_ratio(1, token_in_balance, token_out_balance, total_fee)
                token_flow_two_insert_data["token_in_amount"] = token_in_balance
                token_flow_two_insert_data["revolve_one_out_amount"] = token_out_balance
            else:
                continue
            if token_flow_two_insert_data["revolve_token_one"] in decimals_data and token_flow_two_insert_data["token_out"] in decimals_data:
                token_in_balance = int(token_flow_two_insert_data["revolve_one_in_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["revolve_token_one"]])
                token_out_balance = int(token_flow_two_insert_data["token_out_amount"]) / int("1" + "0" * decimals_data[token_flow_two_insert_data["token_out"]])
                token_flow_two_insert_data["revolve_token_one_ratio"] = get_ratio(1, token_in_balance, token_out_balance, token_one_data["total_fee"])
                token_flow_two_insert_data["revolve_one_in_amount"] = token_in_balance
                token_flow_two_insert_data["token_out_amount"] = token_out_balance
            else:
                continue
            final_ratio = token_flow_two_insert_data["revolve_token_one_ratio"] * token_flow_two_insert_data["token_pair_ratio"]
            token_flow_two_insert_data["final_ratio"] = '%.6f' % final_ratio
            token_flow_insert_all_data_list.append(token_flow_two_insert_data)
            handle_grade_three(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol,
                               token_two_data["pool_id"], revolve_token_one, revolve_token_one_symbol,
                               token_in_amount, revolve_one_out_amount, total_fee, list_pool_data, token_flow_insert_all_data_list)
    # add_token_flow(token_flow_two_insert_data_list)


def handle_grade_three(token_pair, token_pair_one, token_pair_two, token_in_symbol, token_out_symbol, pool_id,
                       revolve_token_one, revolve_token_one_symbol, token_in_amount, revolve_one_out_amount, total_fee, list_pool_data, token_flow_insert_all_data_list):
    decimals_data = get_token_decimal()
    # token_flow_three_insert_data_list = []
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
            three_pool_ids = [pool_id, token_three_data["pool_id"], token_one_data["pool_id"]]
            token_flow_three_insert_data = {
                "token_pair": token_pair,
                "grade": "3",
                "pool_ids": str(three_pool_ids),
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
                token_in_balance = int(token_flow_three_insert_data["token_in_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["token_in"]])
                token_out_balance = int(token_flow_three_insert_data["revolve_one_out_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_one"]])
                token_flow_three_insert_data["token_pair_ratio"] = get_ratio(1, token_in_balance, token_out_balance, total_fee)
                token_flow_three_insert_data["token_in_amount"] = token_in_balance
                token_flow_three_insert_data["revolve_one_out_amount"] = token_out_balance
            else:
                continue
            if token_flow_three_insert_data["revolve_token_one"] in decimals_data and token_flow_three_insert_data["revolve_token_two"] in decimals_data:
                token_in_balance = int(token_flow_three_insert_data["revolve_one_in_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_one"]])
                token_out_balance = int(token_flow_three_insert_data["revolve_two_out_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_two"]])
                token_flow_three_insert_data["revolve_token_one_ratio"] = get_ratio(1, token_in_balance, token_out_balance, token_three_data["total_fee"])
                token_flow_three_insert_data["revolve_one_in_amount"] = token_in_balance
                token_flow_three_insert_data["revolve_two_out_amount"] = token_out_balance
            else:
                continue
            if token_flow_three_insert_data["revolve_token_two"] in decimals_data and token_flow_three_insert_data["token_out"] in decimals_data:
                token_in_balance = int(token_flow_three_insert_data["revolve_two_in_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_two"]])
                token_out_balance = int(token_flow_three_insert_data["token_out_amount"]) / int("1" + "0" * decimals_data[token_flow_three_insert_data["token_out"]])
                token_flow_three_insert_data["revolve_token_two_ratio"] = get_ratio(1, token_in_balance, token_out_balance, token_one_data["total_fee"])
                token_flow_three_insert_data["revolve_two_in_amount"] = token_in_balance
                token_flow_three_insert_data["token_out_amount"] = token_out_balance
            else:
                continue
            final_ratio = token_flow_three_insert_data["token_pair_ratio"] * token_flow_three_insert_data["revolve_token_one_ratio"] * token_flow_three_insert_data["revolve_token_two_ratio"]
            token_flow_three_insert_data["final_ratio"] = '%.6f' % final_ratio
            token_flow_insert_all_data_list.append(token_flow_three_insert_data)
    # add_token_flow(token_flow_three_insert_data_list)


def get_token_decimal():
    decimals = {}
    for token in Cfg.TOKENS["MAINNET"]:
        decimals[token["NEAR_ID"]] = token["DECIMAL"]
    return decimals


def get_ratio(token_in_amount, token_in_balance, token_out_balance, fee):
    try:
        token_in_amount = decimal.Decimal(token_in_amount)
        token_in_balance = decimal.Decimal(token_in_balance)
        token_out_balance = decimal.Decimal(token_out_balance)
        fee = decimal.Decimal(fee)
        ratio = token_in_amount * (10000 - fee) * token_out_balance / (10000 * token_in_balance + token_in_amount * (10000 - fee))
    except Exception as e:
        print("get ratio error:", e)
        return 0
    a, b = str(ratio).split('.')
    return float(a + '.' + b[0:6])
    # return '%.6f' % ratio


if __name__ == "__main__":
    print("#########TOKEN FLOW START###########")
    start_time = int(time.time())
    # update_old_token_flow_data()
    # end_time1 = int(time.time())
    # print("update_old_token_flow_data consuming:", end_time1 - start_time)
    list_top_pools_data = get_list_top_pools()
    end_time1 = int(time.time())
    print("get_list_top_pools consuming:", end_time1 - start_time)
    pools_data_list = handle_list_pool_data(list_top_pools_data)
    end_time2 = int(time.time())
    print("handle_list_pool_data consuming:", end_time2 - end_time1)
    token_flow_insert_data_list = handle_flow_grade(pools_data_list)
    end_time3 = int(time.time())
    print("handle_flow_grade consuming:", end_time3 - end_time2)
    add_token_flow(token_flow_insert_data_list)
    end_time = int(time.time())
    print("add_token_flow consuming:", end_time - end_time3)
    print("total consuming:", end_time - start_time)
    print("#########TOKEN FLOW END###########")

    # token_in_amount = 1
    # token_in_balance = decimal.Decimal(40300.068074627033347709)
    # # # token_in_balance = decimal.Decimal(2305203.936049)
    # token_out_balance = decimal.Decimal(20367.711115568328953233)
    # fee = 30
    # ratio_ret = get_ratio(token_in_amount, token_in_balance, token_out_balance, fee)
    # print(ratio_ret)
