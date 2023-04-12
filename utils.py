import gzip
from flask import make_response
import json
from flask import request
import decimal
import time


def combine_pools_info(pools, prices, metadata):
    ret_pools = []
    for pool in pools:
        tokens = pool['token_account_ids']
        token_balances = []
        token_prices = []
        token_tvls = []
        valid_token_tvl = 0
        valid_token_price = 0
        token_metadata_flag = True
        for i in range(len(tokens)):
            if metadata[tokens[i]] != "":
                token_decimals = metadata[tokens[i]]["decimals"]
                token_symbol = metadata[tokens[i]]["symbol"]
                if token_decimals is None or token_symbol is None or token_decimals == "" or token_symbol == "":
                    token_metadata_flag = False
                balance = float(pool['amounts'][i]) / (10 ** token_decimals)
            else:
                token_metadata_flag = False
                balance = 0
            # balance = float(pool['amounts'][i]) / (10 ** metadata[tokens[i]]["decimals"])
            token_balances.append(balance)
            if tokens[i] in prices:
                # record latest valid token_price
                valid_token_price = prices[tokens[i]]
                token_prices.append(valid_token_price)
                token_tvl = float(valid_token_price) * balance
                token_tvls.append(token_tvl)
                if token_tvl > 0:
                    # record latest valid token_tvl
                    valid_token_tvl = token_tvl
            else:
                token_prices.append(0)
                token_tvls.append(0)
        # sum to TVL
        tvl = 0
        for i in range(len(token_tvls)):
            token_tvl = token_tvls[i]
            if token_tvl > 0:
                tvl += token_tvl
            else:
                if pool["pool_kind"] == "SIMPLE_POOL":
                    tvl += valid_token_tvl
                elif pool["pool_kind"] == "STABLE_SWAP":
                    tvl += float(valid_token_price) * token_balances[i]
                else:
                    pass
        pool["tvl"] = str(tvl)

        if pool["pool_kind"] == "SIMPLE_POOL":
            # add token0_ref_price = token1_price * token1_balance / token0_balance 
            if token_balances[0] > 0 and token_balances[1] > 0 and tokens[1] in prices:
                pool["token0_ref_price"] = str(float(token_prices[1]) * token_balances[1] / token_balances[0])
            else:
                pool["token0_ref_price"] = "N/A"
        if token_metadata_flag:
            ret_pools.append(pool)
    pools.clear()
    for ret_pool in ret_pools:
        pools.append(ret_pool)
    pass


def compress_response_content(ret):
    content = gzip.compress(json.dumps(ret).encode('utf8'), 5)
    response = make_response(content)
    response.headers['Content-length'] = len(content)
    response.headers['Content-Encoding'] = 'gzip'
    return response


def get_ip_address():
    if request.headers.getlist("X-Forwarded-For"):
        ip_address = request.headers.getlist("X-Forwarded-For")[0]
    else:
        ip_address = request.remote_addr
    ip_address = ip_address.split(", ")
    return ip_address[0]


def pools_filter(pools, tvl, amounts):
    ret_pools = []
    for pool in pools:
        try:
            if not tvl is None and "" != tvl:
                if float(pool["tvl"]) <= float(tvl):
                    continue
            if not amounts is None and "" != amounts:
                amount_count = float(0)
                for amount in pool["amounts"]:
                    amount_count = amount_count + float(amount)
                if float(amount_count) <= float(amounts):
                    continue
            ret_pools.append(pool)
        except Exception as e:
            print("pools filter error:", e)
            print("error content:", pool)
            ret_pools.append(pool)
            continue

    return ret_pools


def combine_token_flow(token_flow_data_list, swap_amount):
    max_ratio = 0.00
    max_token_pair_data = {}
    for token_pair_data in token_flow_data_list:
        if "'1910'" in token_pair_data["pool_ids"]:
            continue
        grade_ratio = 0.00
        if token_pair_data["grade"] == "1":
            grade_1_ratio = get_token_flow_ratio(swap_amount, token_pair_data["token_in_amount"], token_pair_data["token_out_amount"], token_pair_data["pool_fee"])
            # print("grade_1_ratio", grade_1_ratio)
            token_pair_data["token_pair_ratio"] = '%.6f' % grade_1_ratio
            token_pair_data["final_ratio"] = '%.6f' % (float(grade_1_ratio) / float(swap_amount))
            grade_ratio = float(grade_1_ratio)
        if token_pair_data["grade"] == "2":
            grade_2_ratio_one = get_token_flow_ratio(swap_amount, token_pair_data["token_in_amount"], token_pair_data["revolve_one_out_amount"], token_pair_data["pool_fee"])
            grade_2_ratio_two = (get_token_flow_ratio(swap_amount, token_pair_data["revolve_one_in_amount"], token_pair_data["token_out_amount"], token_pair_data["revolve_one_pool_fee"])) / float(swap_amount)
            grade_2_ratio = '%.6f' % (grade_2_ratio_one * grade_2_ratio_two)
            # print("grade_2_ratio", grade_2_ratio)
            token_pair_data["token_pair_ratio"] = '%.6f' % grade_2_ratio_one
            token_pair_data["revolve_token_one_ratio"] = '%.6f' % grade_2_ratio_two
            token_pair_data["final_ratio"] = '%.6f' % (float(grade_2_ratio) / float(swap_amount))
            grade_ratio = float(grade_2_ratio)
        if token_pair_data["grade"] == "3":
            grade_3_ratio_one = get_token_flow_ratio(swap_amount, token_pair_data["token_in_amount"], token_pair_data["revolve_one_out_amount"], token_pair_data["pool_fee"])
            grade_3_ratio_two = (get_token_flow_ratio(swap_amount, token_pair_data["revolve_one_in_amount"], token_pair_data["revolve_two_out_amount"], token_pair_data["revolve_one_pool_fee"])) / float(swap_amount)
            grade_3_ratio_three = (get_token_flow_ratio(swap_amount, token_pair_data["revolve_two_in_amount"], token_pair_data["token_out_amount"], token_pair_data["revolve_two_pool_fee"])) / float(swap_amount)
            grade_3_ratio = '%.6f' % (grade_3_ratio_one * grade_3_ratio_two * grade_3_ratio_three)
            # print("grade_3_ratio", grade_3_ratio)
            token_pair_data["token_pair_ratio"] = '%.6f' % grade_3_ratio_one
            token_pair_data["revolve_token_one_ratio"] = '%.6f' % grade_3_ratio_two
            token_pair_data["revolve_token_two_ratio"] = '%.6f' % grade_3_ratio_three
            token_pair_data["final_ratio"] = '%.6f' % (float(grade_3_ratio) / float(swap_amount))
            grade_ratio = float(grade_3_ratio)
        if grade_ratio > max_ratio:
            max_ratio = grade_ratio
            max_token_pair_data = token_pair_data
            max_token_pair_data["amount"] = max_ratio
            max_token_pair_data["swap_amount"] = swap_amount
            max_token_pair_data["timestamp"] = int(time.time())
    return max_token_pair_data


def get_token_flow_ratio(token_in_amount, token_in_balance, token_out_balance, fee):
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


if __name__ == '__main__':
    from config import Cfg
    from redis_provider import list_token_price, list_pools_by_id_list, list_token_metadata
    pools = list_pools_by_id_list(Cfg.NETWORK_ID, [10, 11, 14, 79])
    prices = list_token_price(Cfg.NETWORK_ID)
    metadata = list_token_metadata(Cfg.NETWORK_ID)
    combine_pools_info(pools, prices, metadata)
    for pool in pools:
        print(pool)
    pass