import sys
sys.path.append('../')
from redis_provider import RedisProvider
import requests
from config import Cfg
import json
import time
from db_provider import batch_add_history_token_price
from near_multinode_rpc_provider import MultiNodeJsonProvider

usd_token = "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1"
amount_in = 100000000
usd_token_decimal = 6


def get_now_millisecond():
    millisecond = int(time.time_ns()) // 1000000
    return millisecond


def pool_price(tokens):
    rhea_price = 0
    virtual_price = 0
    pool_tokens_price = []
    decimal_data = get_decimals()
    conn = MultiNodeJsonProvider(network_id)
    for token in tokens:
        if token["NEAR_ID"] == "xtoken.rhealab.near":
            ret = conn.view_call("xtoken.rhealab.near", "get_virtual_price", "NA".encode(encoding='utf-8'))
            json_str = "".join([chr(x) for x in ret["result"]])
            virtual_price = json.loads(json_str)
            print("xtoken get_virtual_price:", virtual_price)
            virtual_price = int(virtual_price) / 100000000
            continue
        else:
            price = get_price_by_smart_router(token["NEAR_ID"], decimal_data[token["NEAR_ID"]])
        if price is not None:
            pool_tokens_price.append({"NEAR_ID": token["NEAR_ID"], "price": price})
            if token["NEAR_ID"] == "token.rhealab.near":
                rhea_price = price
                print("r_price111:", rhea_price)
    pool_tokens_price.append({"NEAR_ID": "xtoken.rhealab.near", "price": float(rhea_price) * float(virtual_price)})
    return pool_tokens_price


def market_price(tokens):
    market_tokens_price = []
    obj = None
    try:
        response = requests.get(Cfg.MARKET_URL)
        data = response.text
        obj = json.loads(data)
    except Exception as e:
        print("Error: ", e)

    if obj and len(obj) > 0:
        for token in tokens:
            md_id = token["MD_ID"]
            if md_id in obj and "usd" in obj[md_id]:
                market_tokens_price.append({
                    "NEAR_ID": token["NEAR_ID"],
                    "BASE_ID": "",
                    "price": str(obj[md_id]["usd"])
                })
    return market_tokens_price


def update_price(network_id):
    start_time1 = get_now_millisecond()
    pool_tokens = []
    market_tokens = []
    decimals = {}
    price_ref = {}
    for token in Cfg.TOKENS[network_id]:
        decimals[token["NEAR_ID"]] = token["DECIMAL"]
        if len(token["MD_ID"].split("|")) == 3:
            pool_tokens.append(token)
        else:
            market_tokens.append(token)

    tokens_price = market_price(market_tokens)
    token_list = []
    for token in tokens_price:
        price_ref[token["NEAR_ID"]] = token["price"]
        token_list.append(token["NEAR_ID"])
    pool_price_data_list = pool_price(pool_tokens)
    for pool_price_data in pool_price_data_list:
        if pool_price_data["NEAR_ID"] not in token_list:
            tokens_price.append(pool_price_data)

    try:
        if len(tokens_price) > 0:
            conn = RedisProvider()
            conn.begin_pipe()
            for token in tokens_price:
                conn.add_token_price(network_id, token["NEAR_ID"], token["price"])
            conn.end_pipe()
            conn.close()
    except Exception as e:
        print("Error occurred when update to Redis, cancel pipe. Error is: ", e)
    end_time1 = get_now_millisecond()
    if end_time1 - start_time1 > 10:
        print("update_price time:", end_time1 - start_time1)
    try:
        if len(tokens_price) > 0:
            insert_data_list = []
            for token in tokens_price:
                insert_data_list.append({"contract_address": token["NEAR_ID"], "symbol": get_symbol(token["NEAR_ID"]), "price": token["price"], "decimal": decimals[token["NEAR_ID"]]})
                if len(insert_data_list) >= 500:
                    batch_add_history_token_price(insert_data_list, network_id)
                    insert_data_list.clear()
            if len(insert_data_list) > 0:
                batch_add_history_token_price(insert_data_list, network_id)
    except Exception as e:
        print("Error occurred when update to db, Error is: ", e)
    end_time2 = get_now_millisecond()
    if end_time2 - end_time1 > 10:
        print("insert data time:", end_time2 - end_time1)


def get_symbol(contract_address):
    symbol = ""
    for token in Cfg.TOKENS[Cfg.NETWORK_ID]:
        if token["NEAR_ID"] in contract_address:
            symbol = token["SYMBOL"]
            return symbol
    return symbol


def get_decimals():
    decimals = {}
    for token in Cfg.TOKENS[Cfg.NETWORK_ID]:
        decimals[token["NEAR_ID"]] = token["DECIMAL"]
    return decimals


def get_price_by_smart_router(token_id, token_decimal):
    price_data = None
    in_price_data = 0
    try:
        smart_router_url = Cfg.REF_SDK_URL + "?amountIn=%s&tokenIn=%s&tokenOut=%s&pathDeep=3&slippage=0" % (amount_in, usd_token, token_id)
        smart_router_ret = requests.get(smart_router_url, timeout=10)
        if smart_router_ret.status_code == 200:
            smart_router_data = json.loads(smart_router_ret.content)
            if 0 == smart_router_data["result_code"]:
                result_data = smart_router_data["result_data"]
                in_price_data = int(result_data["amount_out"])
                print("in_price_data:", in_price_data)
            else:
                print("smart_router_data:", smart_router_data)
        else:
            print("smart_router_ret:", smart_router_ret)
        if in_price_data > 0:
            in_price_amount = in_price_data / int("1" + "0" * token_decimal)
            in_price_usd = 100 / in_price_amount
            smart_router_url = Cfg.REF_SDK_URL + "?amountIn=%s&tokenIn=%s&tokenOut=%s&pathDeep=3&slippage=0" % (in_price_data, token_id, usd_token)
            smart_router_ret = requests.get(smart_router_url, timeout=5)
            if smart_router_ret.status_code == 200:
                smart_router_data = json.loads(smart_router_ret.content)
                if 0 == smart_router_data["result_code"]:
                    result_data = smart_router_data["result_data"]
                    out_price_data = int(result_data["amount_out"])
                    out_price_usd = (out_price_data / int("1" + "0" * usd_token_decimal)) / in_price_amount
                    price_data = "%.12f" % ((out_price_usd + in_price_usd) / 2)
                    print("token:", token_id)
                    print("price:", price_data)
                else:
                    print("smart_router_data:", smart_router_data)
            else:
                print("smart_router_ret:", smart_router_ret)
    except Exception as e:
        print("handel token price error:", e)
        print("token_id:", token_id)
    return price_data


if __name__ == '__main__':
    print("----------------start_token_price-------------------")
    start_time = get_now_millisecond()
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            update_price(network_id)
            end_time = get_now_millisecond()
            if end_time - start_time > 20:
                print("all time:", end_time - start_time)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)

    # ret = get_price_by_smart_router("phoenix-bonds.near", 24)
    # print(ret)
