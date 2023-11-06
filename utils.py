import gzip

from flask import make_response
import json
from flask import request
import requests
from db_provider import add_tx_receipt, query_tx_by_receipt
from config import Cfg

LEFT_MOST_POINT = -800000
RIGHT_MOST_POINT = 800000


def get_tx_id(receipt_id, network_id):
    tx_id = query_tx_by_receipt(receipt_id, network_id)
    if tx_id == "":
        try:
            tx_id = near_explorer_tx(receipt_id, network_id)
        except Exception as e:
            print("explorer error:", e)
            tx_id = near_block_tx(receipt_id, network_id)
    return tx_id


def near_explorer_tx(receipt_id, network_id):
    import re
    tx_receipt_data_list = []
    tx_id = ""
    tx_receipt_data = {
        "tx_id": "",
        "receipt_id": receipt_id
    }
    explorer_query_tx_id_url = "https://explorer.near.org/?query=" + receipt_id
    requests.packages.urllib3.disable_warnings()
    explorer_tx_ret = requests.get(url=explorer_query_tx_id_url, verify=False)
    explorer_tx_data = str(explorer_tx_ret.text)
    tx_ret_list = re.findall("<a class=\"(.*?)</a>", explorer_tx_data)
    if len(tx_ret_list) > 0:
        for tx_ret in tx_ret_list:
            tx_list = re.findall("href=\"/transactions/(.*?)#" + receipt_id, tx_ret)
            if len(tx_list) > 0:
                tx_id = str(tx_list[0])
        tx_receipt_data["tx_id"] = tx_id
        if tx_receipt_data["tx_id"] != "":
            tx_receipt_data_list.append(tx_receipt_data)
            add_tx_receipt(tx_receipt_data_list, network_id)
    return tx_id


def near_block_tx(receipt_id, network_id):
    tx_receipt_data_list = []
    tx_id = ""
    tx_receipt_data = {
        "tx_id": "",
        "receipt_id": receipt_id
    }
    blocks_query_tx_id_url = "https://api.nearblocks.io/v1/search/?keyword=" + receipt_id
    requests.packages.urllib3.disable_warnings()
    blocks_tx_ret = requests.get(url=blocks_query_tx_id_url, verify=False)
    blocks_tx_data = json.loads(blocks_tx_ret.text)
    for receipt in blocks_tx_data["receipts"]:
        if receipt["receipt_id"] == receipt_id:
            tx_id = receipt["originated_from_transaction_hash"]
            tx_receipt_data["tx_id"] = tx_id
            if tx_receipt_data["tx_id"] != "":
                tx_receipt_data_list.append(tx_receipt_data)
                add_tx_receipt(tx_receipt_data_list, network_id)
            else:
                print("blocks_tx_data:", blocks_tx_data)
    return tx_id


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


def combine_dcl_pool_log(ret):
    ret_data_list = []
    for data in ret:
        args_data = json.loads(data["args"])
        args = args_data[0]
        flag = False
        if "msg" in data["args"]:
            amount = args["amount"]
            msg = args["msg"]
            ret_msg_data = {
                "event_method": data["event_method"],
                "tx": data["tx_id"],
                # "index_in_chunk": index_data_list[mysql_data["tx_id"]]["index_in_chunk"],
                "block_no": data["block_id"],
                "operator": data["owner_id"],
                "token_contract": data["predecessor_id"],
                "receiver_id": data["receiver_id"],
                "amount": amount,
                "msg": msg
            }
            ret_data_list.append(ret_msg_data)
        else:
            ret_msg_data = {
                "event_method": data["event_method"],
                "tx": data["tx_id"],
                # "index_in_chunk": index_data_list[mysql_data["tx_id"]]["index_in_chunk"],
                "block_no": data["block_id"],
                "operator": data["owner_id"],
            }
            args_detail = args
            if data["event_method"] == "liquidity_added" and "add_liquidity_infos" in args:
                add_liquidity_infos = args["add_liquidity_infos"]
                for add_liquidity_info in add_liquidity_infos:
                    if data["left_point"] == str(add_liquidity_info["left_point"]) and data["right_point"] == str(add_liquidity_info["right_point"]):
                        args_detail = add_liquidity_info
            if data["event_method"] == "liquidity_removed" and "remove_liquidity_infos" in args:
                remove_liquidity_infos = args["remove_liquidity_infos"]
                for remove_liquidity_info in remove_liquidity_infos:
                    if data["lpt_id"] == remove_liquidity_info["lpt_id"]:
                        args_detail = remove_liquidity_info
            if "pool_id" in args_detail:
                ret_msg_data["pool_id"] = args_detail["pool_id"]
                flag = True
            if "lpt_id" in args_detail:
                ret_msg_data["lpt_id"] = args_detail["lpt_id"]
                flag = True
            if "order_id" in args_detail:
                ret_msg_data["order_id"] = args_detail["order_id"]
            if "amount" in args_detail:
                ret_msg_data["amount"] = args_detail["amount"]
                flag = True
            if "left_point" in args_detail:
                ret_msg_data["left_point"] = args_detail["left_point"]
                flag = True
            if "right_point" in args_detail:
                ret_msg_data["right_point"] = args_detail["right_point"]
                flag = True
            if "amount_x" in args_detail:
                ret_msg_data["amount_x"] = args_detail["amount_x"]
                flag = True
            if "amount_y" in args_detail:
                ret_msg_data["amount_y"] = args_detail["amount_y"]
                flag = True
            if "min_amount_x" in args_detail:
                ret_msg_data["min_amount_x"] = args_detail["min_amount_x"]
                flag = True
            if "min_amount_y" in args_detail:
                ret_msg_data["min_amount_y"] = args_detail["min_amount_y"]
                flag = True
            if flag is False:
                ret_msg_data["amount"] = "None"
            ret_data_list.append(ret_msg_data)
    return ret_data_list


def handle_point_data(all_point_data, start_point, end_point):
    point_data_list = []
    for point_data in all_point_data:
        if start_point <= point_data["point"] <= end_point:
            point_data_list.append(point_data)
    return point_data_list


def handle_dcl_point_bin(pool_id, point_data, slot_number, start_point, end_point, point_data_24h, token_price):
    token_decimal_data = get_token_decimal()
    ret_point_list = []
    if len(point_data) < 1:
        return ret_point_list
    point_data_object = {}
    for point in point_data:
        current_point = point["cp"]
        point_number = point["point"]
        point_object = {
            "tvl_x_l": float(point["tvl_x_l"]),
            "tvl_y_l": float(point["tvl_y_l"]),
            "tvl_x_o": float(point["tvl_x_o"]),
            "tvl_y_o": float(point["tvl_y_o"]),
            "current_point": current_point
        }
        point_data_object[point_number] = point_object
    point_data_24h_object = {}
    for point_24h in point_data_24h:
        point_number = point_24h["point"]
        fee_x = float(point_24h["fee_x"]) * token_price[0]
        fee_y = float(point_24h["fee_y"]) * token_price[1]
        tvl_x_l_24h = float(point_24h["tvl_x_l"]) * token_price[0] / 24
        tvl_y_l_24h = float(point_24h["tvl_y_l"]) * token_price[1] / 24
        point_24h_object = {
            "fee": fee_x + fee_y,
            "total_liquidity": tvl_x_l_24h + tvl_y_l_24h,
        }
        point_data_24h_object[point_number] = point_24h_object
    total_point = end_point - start_point
    pool_id_s = pool_id.split("|")
    fee_tier = pool_id_s[-1]
    token_x = pool_id_s[0]
    token_y = pool_id_s[1]
    point_delta_number = 40
    if fee_tier == "100":
        point_delta_number = 1
    elif fee_tier == "400":
        point_delta_number = 8
    elif fee_tier == "2000":
        point_delta_number = 40
    elif fee_tier == "10000":
        point_delta_number = 200
    bin_point_number = point_delta_number * slot_number
    total_bin = int(total_point / bin_point_number)
    for i in range(1, total_bin + 2):
        slot_point_number = bin_point_number * i
        start_point_number = int(start_point / bin_point_number) * bin_point_number
        ret_point_data = {
            "pool_id": pool_id,
            "point": start_point_number + slot_point_number - bin_point_number,
            "liquidity": 0,
            "token_x": 0,
            "token_y": 0,
            "order_x": 0,
            "order_y": 0,
            "order_liquidity": 0,
            "fee": 0,
            "total_liquidity": 0,
            "sort_number": i,
        }
        end_slot_point_number = start_point_number + slot_point_number
        start_slot_point_number = end_slot_point_number - bin_point_number
        current_point = 0
        # for point in point_data:
        #     current_point = point["cp"]
        #     point_number = point["point"]
        #     if start_slot_point_number <= point_number < end_slot_point_number:
        #         ret_point_data["token_x"] = ret_point_data["token_x"] + float(point["tvl_x_l"])
        #         ret_point_data["token_y"] = ret_point_data["token_y"] + float(point["tvl_y_l"])
        #         ret_point_data["order_x"] = ret_point_data["order_x"] + float(point["tvl_x_o"])
        #         ret_point_data["order_y"] = ret_point_data["order_y"] + float(point["tvl_y_o"])
        number = int((end_slot_point_number - start_slot_point_number) / point_delta_number) - 1
        for o in range(0, number):
            point_data_number = point_delta_number * o + start_slot_point_number
            for e in range(0, int(bin_point_number / point_delta_number)):
                point_data_number_e = point_data_number + point_delta_number * e
                if point_data_number_e in point_data_object:
                    point_object = point_data_object.pop(point_data_number_e)
                    ret_point_data["token_x"] = ret_point_data["token_x"] + point_object["tvl_x_l"]
                    ret_point_data["token_y"] = ret_point_data["token_y"] + point_object["tvl_y_l"]
                    ret_point_data["order_x"] = ret_point_data["order_x"] + point_object["tvl_x_o"]
                    ret_point_data["order_y"] = ret_point_data["order_y"] + point_object["tvl_y_o"]
                    current_point = point_object["current_point"]
            for f in range(0, int(bin_point_number / point_delta_number)):
                point_data_number_e = point_data_number + point_delta_number * f
                if point_data_number_e in point_data_24h_object:
                    point_24h_object = point_data_24h_object.pop(point_data_number_e)
                    ret_point_data["fee"] = ret_point_data["fee"] + point_24h_object["fee"]
                    ret_point_data["total_liquidity"] = ret_point_data["total_liquidity"] + point_24h_object["total_liquidity"]
        if end_slot_point_number >= RIGHT_MOST_POINT:
            end_slot_point_number = RIGHT_MOST_POINT - 1
        liquidity_amount_x = ret_point_data["token_x"] * int("1" + "0" * token_decimal_data[token_x])
        liquidity_amount_y = ret_point_data["token_y"] * int("1" + "0" * token_decimal_data[token_y])
        if liquidity_amount_x > 0 and liquidity_amount_y == 0:
            ret_point_data["liquidity"] = compute_liquidity(start_slot_point_number, end_slot_point_number, liquidity_amount_x, liquidity_amount_y, current_point - bin_point_number)
        if liquidity_amount_x == 0 and liquidity_amount_y > 0:
            ret_point_data["liquidity"] = compute_liquidity(start_slot_point_number, end_slot_point_number, liquidity_amount_x, liquidity_amount_y, current_point + bin_point_number)
        if liquidity_amount_x > 0 and liquidity_amount_y > 0:
            ret_point_data["liquidity"] = compute_liquidity(start_slot_point_number, end_slot_point_number, liquidity_amount_x, liquidity_amount_y, current_point)
        order_amount_x = ret_point_data["order_x"] * int("1" + "0" * token_decimal_data[token_x])
        order_amount_y = ret_point_data["order_y"] * int("1" + "0" * token_decimal_data[token_y])
        if order_amount_x > 0 and order_amount_y == 0:
            ret_point_data["order_liquidity"] = compute_liquidity(start_slot_point_number, end_slot_point_number, order_amount_x, order_amount_y, current_point - bin_point_number)
        if order_amount_x == 0 and order_amount_y > 0:
            ret_point_data["order_liquidity"] = compute_liquidity(start_slot_point_number, end_slot_point_number, order_amount_x, order_amount_y, current_point + bin_point_number)
        if order_amount_x > 0 and order_amount_y > 0:
            ret_point_data["order_liquidity"] = compute_liquidity(start_slot_point_number, end_slot_point_number, order_amount_x, order_amount_y, current_point)
        if ret_point_data["order_liquidity"] < 0:
            ret_point_data["order_liquidity"] = 0
        # for point_24h in point_data_24h:
        #     point_number = point_24h["point"]
        #     if start_slot_point_number <= point_number < end_slot_point_number:
        #         fee_x = float(point_24h["fee_x"]) * token_price[0]
        #         fee_y = float(point_24h["fee_y"]) * token_price[1]
        #         ret_point_data["fee"] = ret_point_data["fee"] + fee_x + fee_y
        #         tvl_x_l_24h = float(point_24h["tvl_x_l"]) * token_price[0] / 24
        #         tvl_y_l_24h = float(point_24h["tvl_y_l"]) * token_price[1] / 24
        #         ret_point_data["total_liquidity"] = ret_point_data["total_liquidity"] + tvl_x_l_24h + tvl_y_l_24h
        if ret_point_data["liquidity"] > 0 or ret_point_data["order_liquidity"] > 0:
            ret_point_list.append(ret_point_data)
    return ret_point_list


def handle_top_bin_fee(point_data):
    ret_point_data = {
        "total_fee": 0,
        "total_liquidity": 0,
    }
    max_fee_apr = 0
    for point in point_data:
        total_fee = point["fee"]
        total_liquidity = point["total_liquidity"]
        if total_liquidity > 0 and total_fee > 0:
            bin_fee_apr = total_fee / total_liquidity
            if bin_fee_apr > max_fee_apr:
                max_fee_apr = bin_fee_apr
                ret_point_data["total_fee"] = total_fee
                ret_point_data["total_liquidity"] = total_liquidity
    return ret_point_data


# def handle_top_bin_fee(pool_id, point_data, slot_number, start_point, end_point):
#     total_point = end_point - start_point
#     fee_tier = pool_id.split("|")[-1]
#     point_delta_number = 40
#     if fee_tier == "100":
#         point_delta_number = 1
#     elif fee_tier == "400":
#         point_delta_number = 8
#     elif fee_tier == "2000":
#         point_delta_number = 40
#     elif fee_tier == "10000":
#         point_delta_number = 200
#     bin_point_number = point_delta_number * slot_number
#     total_bin = int(total_point / bin_point_number)
#     ret_point_data = {
#         "total_fee": 0,
#         "total_liquidity": 0,
#     }
#     max_fee_apr = 0
#     max_total_liquidity = 0
#     for i in range(1, total_bin + 2):
#         slot_point_number = bin_point_number * i
#         start_point_number = int(start_point / bin_point_number) * bin_point_number
#         end_slot_point_number = start_point_number + slot_point_number
#         start_slot_point_number = end_slot_point_number - bin_point_number
#         total_fee = 0
#         total_liquidity = 0
#         for point in point_data:
#             point_number = point["point"]
#             if start_slot_point_number <= point_number < end_slot_point_number:
#                 total_fee = total_fee + (float(point["fee_x"]) + float(point["fee_y"])) * float(point["p"])
#                 total_liquidity = total_liquidity + (float(point["tvl_x_l"]) + float(point["tvl_y_l"])) * float(point["p"])
#         if total_liquidity > 0:
#             if total_liquidity > max_total_liquidity:
#                 max_total_liquidity = total_liquidity
#                 ret_point_data["total_liquidity"] = total_liquidity
#             bin_fee_apr = total_fee / total_liquidity
#             if bin_fee_apr > max_fee_apr:
#                 max_fee_apr = bin_fee_apr
#                 ret_point_data["total_fee"] = total_fee
#                 ret_point_data["total_liquidity"] = total_liquidity
#     return ret_point_data


def handle_dcl_point_bin_by_account(pool_id, point_data, slot_number, account_id, start_point, end_point):
    ret_point_list = []
    total_point = end_point - start_point
    fee_tier = pool_id.split("|")[-1]
    point_delta_number = 40
    if fee_tier == "100":
        point_delta_number = 1
    elif fee_tier == "400":
        point_delta_number = 8
    elif fee_tier == "2000":
        point_delta_number = 40
    elif fee_tier == "10000":
        point_delta_number = 200
    bin_point_number = point_delta_number * slot_number
    total_bin = int(total_point / bin_point_number)
    for i in range(1, total_bin + 2):
        slot_point_number = bin_point_number * i
        start_point_number = int(start_point / bin_point_number) * bin_point_number
        ret_point_data = {
            "pool_id": "",
            "account_id": account_id,
            "point": start_point_number + slot_point_number - bin_point_number,
            "liquidity": 0,
            "token_x": 0,
            "token_y": 0,
            "fee": 0,
            "total_liquidity": 0,
            "sort_number": i,
        }
        end_slot_point_number = start_point_number + slot_point_number
        start_slot_point_number = end_slot_point_number - bin_point_number
        for point in point_data:
            point_number = point["point"]
            if start_slot_point_number <= point_number < end_slot_point_number:
                if ret_point_data["pool_id"] == "":
                    ret_point_data["pool_id"] = point["pool_id"]
                ret_point_data["liquidity"] = ret_point_data["liquidity"] + int(point["l"])
                ret_point_data["token_x"] = ret_point_data["token_x"] + float(point["tvl_x_l"])
                ret_point_data["token_y"] = ret_point_data["token_y"] + float(point["tvl_y_l"])
                ret_point_data["fee"] = (ret_point_data["fee"] + (float(point["tvl_x_l"]) + float(point["tvl_y_l"])) * float(point["p"]))
                ret_point_data["total_liquidity"] = ret_point_data["total_liquidity"] + (float(point["tvl_x_l"]) + float(point["tvl_y_l"])) * float(point["p"])
        if ret_point_data["liquidity"] > 0:
            ret_point_list.append(ret_point_data)
    return ret_point_list


def pow_128():
    return 1 << 128


def pow_96():
    return 1 << 96


def sqrt_rate_96():
    return get_sqrt_price(1)


def get_sqrt_price(point: int):
    if point > RIGHT_MOST_POINT or point < LEFT_MOST_POINT:
        print("E202_ILLEGAL_POINT")
        return None

    abs_point = point
    if point < 0:
        abs_point = -point

    value = 0x100000000000000000000000000000000
    if point & 1 != 0:
        value = 0xfffcb933bd6fad37aa2d162d1a594001

    value = update_value(abs_point, value, 0x2, 0xfff97272373d413259a46990580e213a)
    value = update_value(abs_point, value, 0x4, 0xfff2e50f5f656932ef12357cf3c7fdcc)
    value = update_value(abs_point, value, 0x8, 0xffe5caca7e10e4e61c3624eaa0941cd0)
    value = update_value(abs_point, value, 0x10, 0xffcb9843d60f6159c9db58835c926644)
    value = update_value(abs_point, value, 0x20, 0xff973b41fa98c081472e6896dfb254c0)
    value = update_value(abs_point, value, 0x40, 0xff2ea16466c96a3843ec78b326b52861)
    value = update_value(abs_point, value, 0x80, 0xfe5dee046a99a2a811c461f1969c3053)
    value = update_value(abs_point, value, 0x100, 0xfcbe86c7900a88aedcffc83b479aa3a4)
    value = update_value(abs_point, value, 0x200, 0xf987a7253ac413176f2b074cf7815e54)
    value = update_value(abs_point, value, 0x400, 0xf3392b0822b70005940c7a398e4b70f3)
    value = update_value(abs_point, value, 0x800, 0xe7159475a2c29b7443b29c7fa6e889d9)
    value = update_value(abs_point, value, 0x1000, 0xd097f3bdfd2022b8845ad8f792aa5825)
    value = update_value(abs_point, value, 0x2000, 0xa9f746462d870fdf8a65dc1f90e061e5)
    value = update_value(abs_point, value, 0x4000, 0x70d869a156d2a1b890bb3df62baf32f7)
    value = update_value(abs_point, value, 0x8000, 0x31be135f97d08fd981231505542fcfa6)
    value = update_value(abs_point, value, 0x10000, 0x9aa508b5b7a84e1c677de54f3e99bc9)
    value = update_value(abs_point, value, 0x20000, 0x5d6af8dedb81196699c329225ee604)
    value = update_value(abs_point, value, 0x40000, 0x2216e584f5fa1ea926041bedfe98)
    value = update_value(abs_point, value, 0x80000, 0x48a170391f7dc42444e8fa2)

    if point > 0:
        value = ((1 << 256) - 1) // value

    remainder = 0
    if value % (1 << 32):
        remainder = 1
    return (value >> 32) + remainder


def update_value(point, value, hex1, hex2):
    if point & hex1 != 0:
        value = value * hex2
        value = (value >> 128)
    return value


def mul_fraction_floor(number, _numerator, _denominator):
    return number * _numerator // _denominator


def get_amount_y_unit_liquidity_96(sqrt_price_l_96: int, sqrt_price_r_96: int, sqrt_rate_96: int):
    numerator = sqrt_price_r_96 - sqrt_price_l_96
    denominator = sqrt_rate_96 - pow_96()
    return mul_fraction_ceil(pow_96(), numerator, denominator)


def mul_fraction_ceil(number, _numerator, _denominator):
    res = number * _numerator // _denominator
    if number * _numerator % _denominator == 0:
        return res
    else:
        return res + 1


def get_amount_x_unit_liquidity_96(left_pt: int, right_pt: int, sqrt_price_r_96: int, sqrt_rate_96: int):
    sqrt_price_pr_pc_96 = get_sqrt_price(right_pt - left_pt + 1)
    sqrt_price_pr_pd_96 = get_sqrt_price(right_pt + 1)
    numerator = sqrt_price_pr_pc_96 - sqrt_rate_96
    denominator = sqrt_price_pr_pd_96 - sqrt_price_r_96
    return mul_fraction_ceil(pow_96(), numerator, denominator)


def compute_deposit_xy_per_unit(left_point: int, right_point: int, current_point: int):
    sqrt_price_96 = get_sqrt_price(current_point)
    sqrt_price_r_96 = get_sqrt_price(right_point)
    y = 0
    if left_point < current_point:
        sqrt_price_l_96 = get_sqrt_price(left_point)
        if right_point < current_point:
            y = get_amount_y_unit_liquidity_96(sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96())
        else:
            y = get_amount_y_unit_liquidity_96(sqrt_price_l_96, sqrt_price_96, sqrt_rate_96())
    x = 0
    if right_point > current_point:
        xr_left = current_point + 1
        if left_point > current_point:
            xr_left = left_point
        x = get_amount_x_unit_liquidity_96(xr_left, right_point, sqrt_price_r_96, sqrt_rate_96())
    if left_point <= current_point < right_point:
        y += sqrt_price_96
    return x, y


def compute_liquidity(left_point: int, right_point: int, amount_x: int, amount_y: int, current_point: int):
    liquidity = ((1 << 128) - 1) // 2
    (x, y) = compute_deposit_xy_per_unit(left_point, right_point, current_point)
    if x > 0:
        xl = mul_fraction_floor(amount_x, pow_96(), x)
        if liquidity > xl:
            liquidity = xl
    if y > 0:
        yl = mul_fraction_floor(amount_y - 1, pow_96(), y)
        if liquidity > yl:
            liquidity = yl
    return liquidity


def get_token_decimal():
    token_decimal_data = {}
    for token in Cfg.TOKENS[Cfg.NETWORK_ID]:
        token_decimal_data[token["NEAR_ID"]] = token["DECIMAL"]
    return token_decimal_data


def compute_deposit_x_y(left_point: int, right_point: int, liquidity: int, current_point):
    sqrt_price_r_96 = get_sqrt_price(right_point)
    sqrt_price_96 = get_sqrt_price(current_point)
    amount_y = 0
    if left_point < current_point:
        sqrt_price_l_96 = get_sqrt_price(left_point)
        if right_point < current_point:
            amount_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), True)
        else:
            amount_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_96, sqrt_rate_96(), True)

    amount_x = 0
    if right_point > current_point:
        xr_left = current_point + 1
        if left_point > current_point:
            xr_left = left_point
        amount_x = get_amount_x(liquidity, xr_left, right_point, sqrt_price_r_96, sqrt_rate_96(), True)

    if left_point <= current_point < right_point:
        amount_y += mul_fraction_ceil(liquidity, sqrt_price_96, pow_96())
        liquidity += liquidity

    return amount_x, amount_y


def get_amount_x(liquidity: int, left_pt: int, right_pt: int, sqrt_price_r_96: int, sqrt_rate_96: int, upper: bool):
    # d = 1.0001,  ∵ L = X * sqrt(P)   ∴ X(i) = L / sqrt(d ^ i)
    # sqrt(d) ^ (r - l) - 1
    # --------------------------------- = amount_x_of_unit_liquidity: the amount of token X equivalent to a unit of  c in the range
    # sqrt(d) ^ r - sqrt(d) ^ (r - 1)
    #
    # (sqrt(d) - 1) * (sqrt(d) ^ (r - l - 1) + sqrt(d) ^ (r - l - 2) + ...... + 1)
    # ----------------------------------------------------------------------------
    # (sqrt(d) - 1) * sqrt(d) ^ (r - 1))
    #
    #      1                1                             1
    # ------------ + ----------------- + ...... + -----------------
    # sqrt(d) ^ l    sqrt(d) ^ (l + 1)            sqrt(d) ^ (r - 1)
    #
    # X(l) + X(l + 1) + ...... + X(r - 1)

    # amount_x = amount_x_of_unit_liquidity * liquidity

    sqrt_price_pr_pl_96 = get_sqrt_price(right_pt - left_pt)
    sqrt_price_pr_m1_96 = mul_fraction_floor(sqrt_price_r_96, pow_96(), sqrt_rate_96)

    # using sum equation of geomitric series to compute range numbers
    numerator = sqrt_price_pr_pl_96 - pow_96()
    denominator = sqrt_price_r_96 - sqrt_price_pr_m1_96
    if not upper:
        return mul_fraction_floor(liquidity, numerator, denominator)
    else:
        return mul_fraction_ceil(liquidity, numerator, denominator)


def get_amount_y(liquidity: int, sqrt_price_l_96: int, sqrt_price_r_96: int, sqrt_rate_96: int, upper: bool):
    # d = 1.0001, ∵ L = Y / sqrt(P)   ∴ Y(i) = L * sqrt(d ^ i)
    # sqrt(d) ^ r - sqrt(d) ^ l
    # ------------------------- = amount_y_of_unit_liquidity: the amount of token Y equivalent to a unit of liquidity in the range
    # sqrt(d) - 1
    #
    # sqrt(d) ^ l * sqrt(d) ^ (r - l) - sqrt(d) ^ l
    # ----------------------------------------------
    # sqrt(d) - 1
    #
    # sqrt(d) ^ l * (sqrt(d) ^ (r - l) - 1)
    # ----------------------------------------------
    # sqrt(d) - 1
    #
    # sqrt(d) ^ l * (sqrt(d) - 1) * (sqrt(d) ^ (r - l - 1) + sqrt(d) ^ (r - l - 2) + ...... + sqrt(d) + 1)
    # ----------------------------------------------------------------------------------------------------
    # sqrt(d) - 1
    #
    # sqrt(d) ^ l + sqrt(d) ^ (l + 1) + ...... + sqrt(d) ^ (r - 1)
    #
    # Y(l) + Y(l + 1) + ...... + Y(r - 1)

    # amount_y = amount_y_of_unit_liquidity * liquidity

    # using sum equation of geomitric series to compute range numbers
    numerator = sqrt_price_r_96 - sqrt_price_l_96
    denominator = sqrt_rate_96 - pow_96()
    if not upper:
        return mul_fraction_floor(liquidity, numerator, denominator)
    else:
        return mul_fraction_ceil(liquidity, numerator, denominator)


def compute_deposit_x_y_buckup(liquidity, left_point, right_point, current_point):
    user_liquidity_y = 0
    user_liquidity_x = 0
    sqrt_price_96 = get_sqrt_price(current_point)
    sqrt_price_r_96 = get_sqrt_price(right_point)
    if left_point < current_point:
        sqrt_price_l_96 = get_sqrt_price(left_point)
        if right_point < current_point:
            user_liquidity_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), True)
        else:
            user_liquidity_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_96, sqrt_rate_96(), True)

    if right_point > current_point:
        xr_left = 0
        if left_point > current_point:
            xr_left = left_point
        else:
            xr_left = current_point + 1

        user_liquidity_x = get_amount_x(liquidity, xr_left, right_point, sqrt_price_r_96, sqrt_rate_96(), True)

    if left_point <= current_point < right_point:
        user_liquidity_y += mul_fraction_ceil(liquidity, sqrt_price_96, pow_96())

    return user_liquidity_x, user_liquidity_y


def pagination(page, size, data_list):
    data_list_size = len(data_list)
    if data_list_size % size == 0:
        pages = int(data_list_size / size)
    else:
        pages = int(data_list_size / size) + 1
    start = (page - 1) * size
    end = start + size
    if start >= data_list_size:
        return []
    if end > data_list_size:
        end = data_list_size
    res_data = {"total": data_list_size,
                "page": page,
                "size": size,
                "pages": pages,
                "items": [data_list[i] for i in range(start, end)]
                }
    return res_data


if __name__ == '__main__':
    # from config import Cfg
    # from redis_provider import list_token_price, list_pools_by_id_list, list_token_metadata
    # pools = list_pools_by_id_list(Cfg.NETWORK_ID, [10, 11, 14, 79])
    # prices = list_token_price(Cfg.NETWORK_ID)
    # metadata = list_token_metadata(Cfg.NETWORK_ID)
    # combine_pools_info(pools, prices, metadata)
    # for pool in pools:
    #     print(pool)
    # pass
    # liquidity_ = compute_liquidity(5160, 5240, 7404115124903830000000000000, 10555983177592727000000000000, 5214)
    # print("liquidity_:", liquidity_)
    # a_x, a_y = compute_deposit_x_y_buckup(182847144196469251612398703, 5000, 5040, 5035)
    # print("x:", a_x)
    # print("y", a_y)
    pools = [{"pool_kind": "SIMPLE_POOL", "token_account_ids": ["know.tkn.near", "metacoin.tkn.near"], "amounts": ["101949192161316401239", "19619876484994480145145"], "total_fee": 60, "shares_total_supply": "1000011527432435354067871", "amp": 0, "farming": False, "token_symbols": ["KNOW", "METACOIN"], "decimals": [18, 18], "id": "3868", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["ntrash.tkn.near", "lfg.tkn.near"], "amounts": ["50000000000000000000000", "2500000000000000000000000"], "total_fee": 20, "shares_total_supply": "1000000000000000000000000", "amp": 0, "farming": False, "token_symbols": ["nTRASH", "LFG"], "decimals": [18, 18], "id": "64", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["xtoken.ref-finance.near", "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near"], "amounts": ["0", "0"], "total_fee": 30, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["xREF", "USDT.e"], "decimals": [18, 6], "id": "3062", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["111111111117dc0aa78b770fa6a738034120c302.factory.bridge.near", "059a1f1dea1020297588c316ffc30a58a1a0d4a2.factory.bridge.near"], "amounts": ["0", "0"], "total_fee": 30, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["1INCH", "BSTN"], "decimals": [18, 18], "id": "3756", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["linear-protocol.near", "xtoken.ref-finance.near"], "amounts": ["0", "0"], "total_fee": 20, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["LINEAR", "xREF"], "decimals": [24, 18], "id": "3652", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["slush.tkn.near", "berryclub.ek.near"], "amounts": ["30000000000000000000000", "250000000000000040000"], "total_fee": 40, "shares_total_supply": "1000000000000000000000000", "amp": 0, "farming": False, "token_symbols": ["SLUSH", "BANANA"], "decimals": [18, 18], "id": "33", "tvl": "0.45216500000000004", "token0_ref_price": "7.536083333333334e-06"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["meritocracy.tkn.near", "hak.tkn.near"], "amounts": ["3008364318650434574256", "34033697012679256155864"], "total_fee": 30, "shares_total_supply": "1001836667790314600200028", "amp": 0, "farming": False, "token_symbols": ["MERITOCRACY", "HAK"], "decimals": [18, 18], "id": "1302", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["wrap.near", "nearkat.tkn.near"], "amounts": ["304108729259707", "2826533968474729401706496668895"], "total_fee": 30, "shares_total_supply": "909673427742419665197", "amp": 0, "farming": False, "token_symbols": ["wNEAR", "NEARKAT"], "decimals": [24, 18], "id": "1226", "tvl": "9.731479336310623e-10", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["meta-token.near", "t.tkn.near"], "amounts": ["73541884206208111170542449", "1906722337069607325775"], "total_fee": 20, "shares_total_supply": "1001201749968576889309716", "amp": 0, "farming": False, "token_symbols": ["$META", "T"], "decimals": [24, 18], "id": "3907", "tvl": "0.30396919915183584", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["4691937a7508860f876c9c0a2a617e7d9e945d4b.factory.bridge.near", "token.v2.ref-finance.near"], "amounts": ["514161085634048742", "1502220156923865697"], "total_fee": 30, "shares_total_supply": "72774505497381443010035", "amp": 0, "farming": False, "token_symbols": ["WOO", "REF"], "decimals": [18, 18], "id": "3163", "tvl": "0.2348395165920258", "token0_ref_price": "0.22852107751709455"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["aurora", "dbio.near"], "amounts": ["0", "0"], "total_fee": 30, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["ETH", "DBIO"], "decimals": [18, 18], "id": "1349", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["de30da39c46104798bb5aa3fe8b9e0e1f348163f.factory.bridge.near", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near"], "amounts": ["7473059687440765976", "5869956"], "total_fee": 19, "shares_total_supply": "2046760207639997342085795", "amp": 0, "farming": False, "token_symbols": ["GTC", "USDC.e"], "decimals": [18, 6], "id": "14", "tvl": "13.507423000564463", "token0_ref_price": "0.7854822851027212"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["avb.tkn.near", "wrap.near"], "amounts": ["33210059407503675287698", "5574993183449504293992454"], "total_fee": 40, "shares_total_supply": "13576728059769856079067647", "amp": 0, "farming": False, "token_symbols": ["AVB", "wNEAR"], "decimals": [18, 24], "id": "20", "tvl": "17.839978187038415", "token0_ref_price": "0.0002685929881686322"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["firerune.tkn.near", "wrap.near"], "amounts": ["0", "0"], "total_fee": 30, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["FIRERUNE", "wNEAR"], "decimals": [9, 24], "id": "1481", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["ynp.tkn.near", "wrap.near"], "amounts": ["48848623594411260041", "2146740426489187265331"], "total_fee": 30, "shares_total_supply": "1002347315148488676627686", "amp": 0, "farming": False, "token_symbols": ["YNP", "wNEAR"], "decimals": [18, 24], "id": "2913", "tvl": "0.006869569364765399", "token0_ref_price": "7.031487132373717e-05"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["ctrl.tkn.near", "6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near"], "amounts": ["223884210718664", "1309302828596896"], "total_fee": 30, "shares_total_supply": "170887956065803199112", "amp": 0, "farming": False, "token_symbols": ["CTRL", "DAI"], "decimals": [18, 18], "id": "3527", "tvl": "0.002618605657193792", "token0_ref_price": "5.848124905253743"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["token.cheddar.near", "token.shrm.near"], "amounts": ["0", "0"], "total_fee": 100, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["Cheddar", "SHRM"], "decimals": [24, 18], "id": "2030", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near", "token.skyward.near"], "amounts": ["517056296866595", "296614881918563"], "total_fee": 30, "shares_total_supply": "1003323936853108325676604", "amp": 0, "farming": False, "token_symbols": ["AURORA", "SKYWARD"], "decimals": [18, 18], "id": "3151", "tvl": "0.00010646118750674342", "token0_ref_price": "0.11212527841115029"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["themunkymonkey.near", "wrap.near"], "amounts": ["0", "0"], "total_fee": 30, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["PSNG", "wNEAR"], "decimals": [8, 24], "id": "2717", "tvl": "0", "token0_ref_price": "N/A"}, {"pool_kind": "SIMPLE_POOL", "token_account_ids": ["501ace9c35e60f03a2af4d484f49f9b1efde9f40.factory.bridge.near", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near"], "amounts": ["0", "0"], "total_fee": 30, "shares_total_supply": "0", "amp": 0, "farming": False, "token_symbols": ["SOLACE", "USDC.e"], "decimals": [18, 6], "id": "2687", "tvl": "0", "token0_ref_price": "N/A"}]
    res = pagination(2, 10, pools)
    print(res)
