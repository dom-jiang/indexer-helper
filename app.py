#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'Marco'
# 导入Flask类
from http.client import responses
from flask import Flask
from flask import request
from flask import jsonify
import flask_cors 
import json
import logging
from indexer_provider import get_actions, get_liquidity_pools
from redis_provider import list_farms, list_top_pools, list_pools, list_token_price, list_whitelist 
from redis_provider import list_pools_by_id_list, list_token_metadata, list_pools_by_tokens, get_pool
from config import Cfg

Welcome = 'Welcome to ref datacenter API server, version 20210629.01'
# 实例化，可视为固定格式
app = Flask(__name__)


# route()方法用于设定路由；类似spring路由配置
@app.route('/')
def hello_world():
    return Welcome


@app.route('/latest-actions/<account_id>', methods=['GET'])
@flask_cors.cross_origin()
def handle_latest_actions(account_id):
    """
    get user's latest actions
    """
    ret = get_actions(Cfg.NETWORK_ID, account_id)
    json_obj = json.loads(ret)
    return jsonify(json_obj)


@app.route('/liquidity-pools/<account_id>', methods=['GET'])
@flask_cors.cross_origin()
def handle_liquidity_pools(account_id):
    """
    get user's liqudity pools
    """
    ret = []
    id_list = get_liquidity_pools(Cfg.NETWORK_ID, account_id)
    if len(id_list) > 0:
        ret = list_pools_by_id_list(Cfg.NETWORK_ID, [int(x) for x in id_list])
    return jsonify(ret)

@app.route('/list-farms', methods=['GET'])
@flask_cors.cross_origin()
def handle_list_farms():
    """
    list_farms
    """
    ret = list_farms(Cfg.NETWORK_ID)
    return jsonify(ret)

@app.route('/list-top-pools', methods=['GET'])
@flask_cors.cross_origin()
def handle_list_top_pools():
    """
    list_top_pools
    """
    # precisions = {}
    # for token in Cfg.TOKENS[Cfg.NETWORK_ID]:
    #     precisions[token["NEAR_ID"]] = token["DECIMAL"]
    pools = list_top_pools(Cfg.NETWORK_ID)
    prices = list_token_price(Cfg.NETWORK_ID)
    metadata = list_token_metadata(Cfg.NETWORK_ID)
    for pool in pools:
        token0, token1 = pool['token_account_ids'][0], pool['token_account_ids'][1]
        (balance0, balance1) = (
            float(pool['amounts'][0]) / (10 ** metadata[token0]["decimals"]), 
            float(pool['amounts'][1]) / (10 ** metadata[token1]["decimals"])
        )
        # add TVL
        tvl0, tvl1 = 0, 0
        if token0 in prices and token0 in metadata:
            tvl0 = float(prices[token0]) * balance0
        if token1 in prices and token1 in metadata:
            tvl1 = float(prices[token1]) * balance1
        if tvl0 > 0 and tvl1 > 0:
            pool["tvl"] = str(tvl0 + tvl1)
        elif tvl0 > 0:
            pool["tvl"] = str(tvl0 * 2)
        elif tvl1 > 0:
            pool["tvl"] = str(tvl1 * 2)
        else:
            pool["tvl"] = "0"
        # add token0_ref_price = token1_price * token1_balance / token0_balance 
        if balance0 > 0 and balance1 > 0 and token1 in prices:
            pool["token0_ref_price"] = str(float(prices[token1]) * balance1 / balance0)
        else:
            pool["token0_ref_price"] = "N/A"

    return jsonify(pools)

@app.route('/list-token-price', methods=['GET'])
@flask_cors.cross_origin()
def handle_list_token_price():
    """
    list_token_price
    """
    ret = {}
    prices = list_token_price(Cfg.NETWORK_ID)
    for token in Cfg.TOKENS[Cfg.NETWORK_ID]:
        ret[token["NEAR_ID"]] = {
            "price": prices[token["NEAR_ID"]], 
            "decimal": token["DECIMAL"],
            "symbol": token["SYMBOL"],
        }
    # if token.v2.ref-finance.near exists, mirror its info to rftt.tkn.near
    if "token.v2.ref-finance.near" in ret:
        ret["rftt.tkn.near"] = {
            "price": prices["token.v2.ref-finance.near"], 
            "decimal": 8,
            "symbol": "RFTT",
        }
    return jsonify(ret)
    
@app.route('/list-token', methods=['GET'])
@flask_cors.cross_origin()
def handle_list_token():
    """
    list_token
    """
    ret = list_token_metadata(Cfg.NETWORK_ID)
    return jsonify(ret)

@app.route('/get-pool', methods=['GET'])
@flask_cors.cross_origin()
def handle_get_pool():
    """
    get_pool
    """
    pool_id = request.args.get("pool_id", "N/A") 
    pool = get_pool(Cfg.NETWORK_ID, pool_id)
    # print(pool)
    if pool:
        prices = list_token_price(Cfg.NETWORK_ID)
        metadata = list_token_metadata(Cfg.NETWORK_ID)
        token0, token1 = pool['token_account_ids'][0], pool['token_account_ids'][1]
        (balance0, balance1) = (
            float(pool['amounts'][0]) / (10 ** metadata[token0]["decimals"]), 
            float(pool['amounts'][1]) / (10 ** metadata[token1]["decimals"])
        )
        # add TVL
        tvl0, tvl1 = 0, 0
        if token0 in prices:
            tvl0 = float(prices[token0]) * balance0
        if token1 in prices:
            tvl1 = float(prices[token1]) * balance1
        if tvl0 > 0 and tvl1 > 0:
            pool["tvl"] = str(tvl0 + tvl1)
        elif tvl0 > 0:
            pool["tvl"] = str(tvl0 * 2)
        elif tvl1 > 0:
            pool["tvl"] = str(tvl1 * 2)
        else:
            pool["tvl"] = "0"
        # add token0_ref_price = token1_price * token1_balance / token0_balance 
        if balance0 > 0 and balance1 > 0 and token1 in prices:
            pool["token0_ref_price"] = str(float(prices[token1]) * balance1 / balance0)
        else:
            pool["token0_ref_price"] = "N/A"

    return jsonify(pool)


@app.route('/list-pools', methods=['GET'])
@flask_cors.cross_origin()
def handle_list_pools():
    """
    list_pools
    """
    # precisions = {}
    # for token in Cfg.TOKENS[Cfg.NETWORK_ID]:
    #     precisions[token["NEAR_ID"]] = token["DECIMAL"]
    pools = list_pools(Cfg.NETWORK_ID)
    prices = list_token_price(Cfg.NETWORK_ID)
    metadata = list_token_metadata(Cfg.NETWORK_ID)
    for pool in pools:
        token0, token1 = pool['token_account_ids'][0], pool['token_account_ids'][1]
        (balance0, balance1) = (
            float(pool['amounts'][0]) / (10 ** metadata[token0]["decimals"]), 
            float(pool['amounts'][1]) / (10 ** metadata[token1]["decimals"])
        )
        # add TVL
        tvl0, tvl1 = 0, 0
        if token0 in prices:
            tvl0 = float(prices[token0]) * balance0
        if token1 in prices:
            tvl1 = float(prices[token1]) * balance1
        if tvl0 > 0 and tvl1 > 0:
            pool["tvl"] = str(tvl0 + tvl1)
        elif tvl0 > 0:
            pool["tvl"] = str(tvl0 * 2)
        elif tvl1 > 0:
            pool["tvl"] = str(tvl1 * 2)
        else:
            pool["tvl"] = "0"
        # add token0_ref_price = token1_price * token1_balance / token0_balance 
        if balance0 > 0 and balance1 > 0 and token1 in prices:
            pool["token0_ref_price"] = str(float(prices[token1]) * balance1 / balance0)
        else:
            pool["token0_ref_price"] = "N/A"

    return jsonify(pools)


@app.route('/list-pools-by-tokens', methods=['GET'])
@flask_cors.cross_origin()
def handle_list_pools_by_tokens():
    """
    list_pools_by_tokens
    """
    token0 = request.args.get("token0", "N/A") 
    token1 = request.args.get("token1", "N/A") 
    pools = list_pools_by_tokens(Cfg.NETWORK_ID, token0, token1)
    prices = list_token_price(Cfg.NETWORK_ID)
    metadata = list_token_metadata(Cfg.NETWORK_ID)
    for pool in pools:
        token0, token1 = pool['token_account_ids'][0], pool['token_account_ids'][1]
        (balance0, balance1) = (
            float(pool['amounts'][0]) / (10 ** metadata[token0]["decimals"]), 
            float(pool['amounts'][1]) / (10 ** metadata[token1]["decimals"])
        )
        # add TVL
        tvl0, tvl1 = 0, 0
        if token0 in prices:
            tvl0 = float(prices[token0]) * balance0
        if token1 in prices:
            tvl1 = float(prices[token1]) * balance1
        if tvl0 > 0 and tvl1 > 0:
            pool["tvl"] = str(tvl0 + tvl1)
        elif tvl0 > 0:
            pool["tvl"] = str(tvl0 * 2)
        elif tvl1 > 0:
            pool["tvl"] = str(tvl1 * 2)
        else:
            pool["tvl"] = "0"
        # add token0_ref_price = token1_price * token1_balance / token0_balance 
        if balance0 > 0 and balance1 > 0 and token1 in prices:
            pool["token0_ref_price"] = str(float(prices[token1]) * balance1 / balance0)
        else:
            pool["token0_ref_price"] = "N/A"

    return jsonify(pools)


@app.route('/list-pools-by-ids', methods=['GET'])
@flask_cors.cross_origin()
def handle_list_pools_by_ids():
    """
    list_pools_by_ids
    """
    ids = request.args.get("ids", "") 
    id_str_list = ids.split("|")

    pools = list_pools_by_id_list(Cfg.NETWORK_ID, [int(x) for x in id_str_list])
    prices = list_token_price(Cfg.NETWORK_ID)
    metadata = list_token_metadata(Cfg.NETWORK_ID)
    for pool in pools:
        token0, token1 = pool['token_account_ids'][0], pool['token_account_ids'][1]
        (balance0, balance1) = (
            float(pool['amounts'][0]) / (10 ** metadata[token0]["decimals"]), 
            float(pool['amounts'][1]) / (10 ** metadata[token1]["decimals"])
        )
        # add TVL
        tvl0, tvl1 = 0, 0
        if token0 in prices:
            tvl0 = float(prices[token0]) * balance0
        if token1 in prices:
            tvl1 = float(prices[token1]) * balance1
        if tvl0 > 0 and tvl1 > 0:
            pool["tvl"] = str(tvl0 + tvl1)
        elif tvl0 > 0:
            pool["tvl"] = str(tvl0 * 2)
        elif tvl1 > 0:
            pool["tvl"] = str(tvl1 * 2)
        else:
            pool["tvl"] = "0"
        # add token0_ref_price = token1_price * token1_balance / token0_balance 
        if balance0 > 0 and balance1 > 0 and token1 in prices:
            pool["token0_ref_price"] = str(float(prices[token1]) * balance1 / balance0)
        else:
            pool["token0_ref_price"] = "N/A"

    return jsonify(pools)


@app.route('/whitelisted-active-pools', methods=['GET'])
@flask_cors.cross_origin()
def handle_whitelisted_active_pools():
    """
    handle_whitelisted_active_pools
    """
    ret = []
    pools = list_top_pools(Cfg.NETWORK_ID)
    whitelist = list_whitelist(Cfg.NETWORK_ID)
    for pool in pools:
        token0, token1 = pool['token_account_ids'][0], pool['token_account_ids'][1]
        if pool["amounts"][0] == "0":
            continue
        if token0 in whitelist and token1 in whitelist:
            ret.append({
                "pool_id": pool["id"], 
                "token_symbols": pool["token_symbols"],
                "token_decimals": [whitelist[token0]["decimals"], whitelist[token1]["decimals"]],
                "token_names": [whitelist[token0]["name"], whitelist[token1]["name"]],
                "liquidity_amounts": pool["amounts"],
                "vol_token0_token1": pool["vol01"],
                "vol_token1_token0": pool["vol10"],
                })

    return jsonify(ret)
    
@app.route('/to-coingecko', methods=['GET'])
@flask_cors.cross_origin()
def handle_to_coingecko():
    """
    handle_price_to_coingecko
    """
    ret = {}
    # pools = list_pools_by_id_list(Cfg.NETWORK_ID, ['1346', '1429'])
    pools = list_top_pools(Cfg.NETWORK_ID)
    prices = list_token_price(Cfg.NETWORK_ID)
    metadata = list_token_metadata(Cfg.NETWORK_ID)
    whitelist = list_whitelist(Cfg.NETWORK_ID)
    for pool in pools:

        token0, token1 = pool['token_account_ids'][0], pool['token_account_ids'][1]
        if pool["amounts"][0] == "0":
            continue
        if token0 in whitelist and token1 in whitelist:
            (balance0, balance1) = (
                float(pool['amounts'][0]) / (10 ** metadata[token0]["decimals"]), 
                float(pool['amounts'][1]) / (10 ** metadata[token1]["decimals"])
            )
            key = "%s-%s" % (pool["token_symbols"][0], pool["token_symbols"][1])
            # add token0_ref_price = token1_price * token1_balance / token0_balance 
            if balance0 > 0 and balance1 > 0:
                ret[key] = {
                    "pool_id": pool["id"], 
                    "token_symbol": pool["token_symbols"][0],
                    "other_token": pool["token_symbols"][1],
                    "token_decimals": [whitelist[token0]["decimals"], whitelist[token1]["decimals"]],
                    "liquidity_amounts": pool["amounts"],
                    "price_in_usd": str(float(prices[token1]) * balance1 / balance0) if token1 in prices else "N/A",
                    "price_in_other_token": str(balance1 / balance0),
                    "vol_to_other_token": pool["vol01"],
                    "vol_from_other_token": pool["vol10"],
                    }

    return jsonify(ret)

@app.route('/price-skyward-near', methods=['GET'])
@flask_cors.cross_origin()
def handle_price_skyward_near():
    """
    handle_price_skyward_near
    """
    token_price = {"price": "N/A", "decimal_skyward": 18, "decimal_near": 24, 
    "volume_skyward2near": {}, "volume_near2skyward": {}, "block_height": 0}
    from near_multinode_rpc_provider import MultiNodeJsonProviderError,  MultiNodeJsonProvider
    contract = Cfg.NETWORK[Cfg.NETWORK_ID]["REF_CONTRACT"]
    try:
        conn = MultiNodeJsonProvider(Cfg.NETWORK_ID)
        ret = conn.view_call(contract, "get_return", b'{"pool_id": 1346, "token_in": "token.skyward.near", "amount_in": "1000000000000000000", "token_out": "wrap.near"}')
        b = "".join([chr(x) for x in ret["result"]])
        obj = json.loads(b)
        price = int(obj[:-16]) / 100000000
        token_price["price"] = "%s" % price
        if 'block_height' in ret:
            token_price['block_height'] = ret['block_height']
        
        ret = conn.view_call("ref-finance.near", "get_pool_volumes", b'{"pool_id": 1346}')
        b = "".join([chr(x) for x in ret["result"]])
        obj = json.loads(b)
        if len(obj) == 2:
            token_price["volume_skyward2near"] = obj[0]
            token_price["volume_near2skyward"] = obj[1]

    except MultiNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)
    return jsonify(token_price)


if __name__ == '__main__':
    app.logger.setLevel(logging.INFO)
    app.logger.info(Welcome)
    app.run(host='0.0.0.0', port=28080, debug=False)
