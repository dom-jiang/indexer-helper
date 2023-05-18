import sys

sys.path.append('../')
import json
import time
from token_flow_utils import get_swapped_amount, get_token_flow_ratio, format_decimal_float, add_token_flow_to_redis
from token_flow_utils import handle_whitelist_token_pair, get_list_top_pools, get_stable_and_rated_pool_data
from token_flow_utils import handle_list_pool_data, get_token_decimal, query_one_pools, query_two_pools, query_three_pools

grade_number = 3


def handle_flow_grade_new(pools_list_ten, network_id, decimals_data, pools_list_zero):
    token_flow_insert_all_data_list = []
    token_pair_list = handle_whitelist_token_pair(network_id)
    handle_grade_one(decimals_data, token_flow_insert_all_data_list, pools_list_ten, token_pair_list, pools_list_zero)
    return token_flow_insert_all_data_list


def handle_grade_one(decimals_data, token_flow_insert_all_data_list, pools_list_ten, token_pair_list, pools_list_zero):
    start_time11 = int(time.time())
    for token_pair in token_pair_list:
        token_pair_one = token_pair.split("->")[0]
        token_pair_two = token_pair.split("->")[1]
        token_pair_one_data_list = query_one_pools(token_pair_one, token_pair_two, pools_list_zero)
        for token_pair_one_data in token_pair_one_data_list:
            swap_number_grade = 1
            for nu in range(0, grade_number):
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
                    "revolve_one_out_amount": "0",
                    "revolve_one_in_amount": "0",
                    "revolve_two_out_amount": "0",
                    "revolve_two_in_amount": "0",
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
                    "swap_number_grade": swap_number_grade,
                }
                if token_pair_one_data["token_one"] == token_pair_one and token_pair_one_data[
                    "token_two"] == token_pair_two:
                    token_flow_insert_data["token_in"] = token_pair_one_data["token_one"]
                    token_flow_insert_data["token_out"] = token_pair_one_data["token_two"]
                    token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_one_amount"]
                    token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_two_amount"]
                elif token_pair_one_data["token_one"] == token_pair_one and token_pair_one_data[
                    "token_three"] == token_pair_two:
                    token_flow_insert_data["token_in"] = token_pair_one_data["token_one"]
                    token_flow_insert_data["token_out"] = token_pair_one_data["token_three"]
                    token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_one_amount"]
                    token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_three_amount"]
                elif token_pair_one_data["token_two"] == token_pair_one and token_pair_one_data[
                    "token_one"] == token_pair_two:
                    token_flow_insert_data["token_in"] = token_pair_one_data["token_two"]
                    token_flow_insert_data["token_out"] = token_pair_one_data["token_one"]
                    token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_two_amount"]
                    token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_one_amount"]
                elif token_pair_one_data["token_two"] == token_pair_one and token_pair_one_data[
                    "token_three"] == token_pair_two:
                    token_flow_insert_data["token_in"] = token_pair_one_data["token_two"]
                    token_flow_insert_data["token_out"] = token_pair_one_data["token_three"]
                    token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_two_amount"]
                    token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_three_amount"]
                elif token_pair_one_data["token_three"] == token_pair_one and token_pair_one_data[
                    "token_one"] == token_pair_two:
                    token_flow_insert_data["token_in"] = token_pair_one_data["token_three"]
                    token_flow_insert_data["token_out"] = token_pair_one_data["token_one"]
                    token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_three_amount"]
                    token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_one_amount"]
                elif token_pair_one_data["token_three"] == token_pair_one and token_pair_one_data[
                    "token_two"] == token_pair_two:
                    token_flow_insert_data["token_in"] = token_pair_one_data["token_three"]
                    token_flow_insert_data["token_out"] = token_pair_one_data["token_two"]
                    token_flow_insert_data["token_in_amount"] = token_pair_one_data["token_three_amount"]
                    token_flow_insert_data["token_out_amount"] = token_pair_one_data["token_two_amount"]
                if token_flow_insert_data["token_in"] in decimals_data and token_flow_insert_data[
                    "token_out"] in decimals_data:
                    if token_pair_one_data["pool_kind"] == "SIMPLE_POOL":
                        token_in_balance = int(token_flow_insert_data["token_in_amount"]) / int(
                            "1" + "0" * decimals_data[token_flow_insert_data["token_in"]])
                        token_out_balance = int(token_flow_insert_data["token_out_amount"]) / int(
                            "1" + "0" * decimals_data[token_flow_insert_data["token_out"]])
                        token_pair_ratio = get_token_flow_ratio(swap_number_grade, token_in_balance, token_out_balance,
                                                                token_pair_one_data["total_fee"])
                        token_flow_insert_data["token_pair_ratio"] = token_pair_ratio
                        token_flow_insert_data["final_ratio"] = (token_pair_ratio / swap_number_grade)
                        token_flow_insert_data["token_in_amount"] = token_in_balance
                        token_flow_insert_data["token_out_amount"] = token_out_balance
                    else:
                        token_in_balance = int(token_flow_insert_data["token_in_amount"])
                        token_out_balance = int(token_flow_insert_data["token_out_amount"])
                        c_amounts = [token_in_balance, token_out_balance]
                        old_rates = token_pair_one_data["rates"]
                        if len(token_pair_one_data["token_account_ids"]) > 2:
                            c_amounts = token_pair_one_data["three_c_amount"]
                            token_flow_insert_data["three_c_amount"] = json.dumps(c_amounts)
                            token_flow_insert_data["three_pool_ids"] = json.dumps(
                                token_pair_one_data["token_account_ids"])
                            token_flow_insert_data["pool_token_number"] = "3"
                            new_rates = old_rates
                        else:
                            token_account_ids = token_pair_one_data["token_account_ids"]
                            token_in_index = token_account_ids.index(token_flow_insert_data["token_in"])
                            token_out_index = token_account_ids.index(token_flow_insert_data["token_out"])
                            new_rates = [old_rates[token_in_index], old_rates[token_out_index]]
                        stable_pool = {"amp": token_pair_one_data["amp"], "total_fee": token_pair_one_data["total_fee"],
                                       "token_account_ids": token_pair_one_data["token_account_ids"],
                                       "c_amounts": c_amounts, "rates": token_pair_one_data["rates"]}
                        token_pair_ratio = get_swapped_amount(token_pair_one, token_pair_two, swap_number_grade,
                                                              stable_pool,
                                                              token_pair_one_data["stable_pool_decimal"])
                        token_flow_insert_data["token_pair_ratio"] = token_pair_ratio
                        token_flow_insert_data["final_ratio"] = token_pair_ratio / swap_number_grade
                        token_flow_insert_data["token_in_amount"] = token_in_balance
                        token_flow_insert_data["token_out_amount"] = token_out_balance
                        token_flow_insert_data["amp"] = token_pair_one_data["amp"]
                        token_flow_insert_data["rates"] = json.dumps(new_rates)
                else:
                    continue
                token_flow_insert_all_data_list.append(token_flow_insert_data)
                swap_number_grade = swap_number_grade * 10
        handle_grade_two(token_pair, token_pair_one, token_pair_two, pools_list_ten, token_flow_insert_all_data_list, decimals_data)
    end_time11 = int(time.time())
    print("handle_grade_one consuming:", end_time11 - start_time11)


def handle_grade_two(token_pair, token_pair_one, token_pair_two, list_pool_data, token_flow_insert_all_data_list,
                     decimals_data):
    token_two_data_list = query_two_pools(token_pair_one, token_pair_two, list_pool_data)
    for token_two_data in token_two_data_list:
        token_one_data_list = []
        token_in_amount = 0
        revolve_token_one = ""
        revolve_one_out_amount = 0
        if token_two_data["token_one"] == token_pair_one:
            revolve_token_one = token_two_data["token_two"]
            token_in_amount = token_two_data["token_one_amount"]
            revolve_one_out_amount = token_two_data["token_two_amount"]
        elif token_two_data["token_two"] == token_pair_one:
            revolve_token_one = token_two_data["token_one"]
            token_in_amount = token_two_data["token_two_amount"]
            revolve_one_out_amount = token_two_data["token_one_amount"]
        elif token_two_data["token_three"] == token_pair_one:
            revolve_token_one = token_two_data["token_one"]
            token_in_amount = token_two_data["token_three_amount"]
            revolve_one_out_amount = token_two_data["token_two_amount"]
            token_one_data_list = query_one_pools(token_two_data["token_two"], token_pair_two, list_pool_data)
        token_one_data_list += query_one_pools(revolve_token_one, token_pair_two, list_pool_data)
        total_fee = token_two_data["total_fee"]
        for token_one_data in token_one_data_list:
            pool_ids = [token_two_data["pool_id"], token_one_data["pool_id"]]
            swap_number_grade = 1
            for i in range(0, grade_number):
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
                    "revolve_one_out_amount": revolve_one_out_amount,
                    "revolve_one_in_amount": "0",
                    "revolve_two_out_amount": "0",
                    "revolve_two_in_amount": "0",
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
                    "swap_number_grade": swap_number_grade
                }
                if token_one_data["token_one"] == revolve_token_one and token_one_data["token_two"] == token_pair_two:
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_one_amount"]
                    token_flow_two_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
                elif token_one_data["token_one"] == revolve_token_one and token_one_data[
                    "token_three"] == token_pair_two:
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_one_amount"]
                    token_flow_two_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
                elif token_one_data["token_two"] == revolve_token_one and token_one_data["token_one"] == token_pair_two:
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_two_amount"]
                    token_flow_two_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
                elif token_one_data["token_two"] == revolve_token_one and token_one_data[
                    "token_three"] == token_pair_two:
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_two_amount"]
                    token_flow_two_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
                elif token_one_data["token_three"] == revolve_token_one and token_one_data[
                    "token_one"] == token_pair_two:
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_three_amount"]
                    token_flow_two_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
                elif token_one_data["token_three"] == revolve_token_one and token_one_data[
                    "token_two"] == token_pair_two:
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_one_data["token_three_amount"]
                    token_flow_two_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
                if token_two_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_two_insert_data["token_in_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_two_insert_data["token_in"]])
                    token_out_balance = int(token_flow_two_insert_data["revolve_one_out_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_two_insert_data["revolve_token_one"]])
                    token_flow_two_insert_data["token_pair_ratio"] = get_token_flow_ratio(swap_number_grade,
                                                                                          token_in_balance,
                                                                                          token_out_balance, total_fee)
                    token_flow_two_insert_data["token_in_amount"] = token_in_balance
                    token_flow_two_insert_data["revolve_one_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_two_insert_data["token_in_amount"])
                    token_out_balance = int(token_flow_two_insert_data["revolve_one_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    old_rates1 = token_two_data["rates"]
                    if len(token_two_data["token_account_ids"]) > 2:
                        c_amounts = token_two_data["three_c_amount"]
                        token_flow_two_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_two_insert_data["three_pool_ids"] = json.dumps(token_two_data["token_account_ids"])
                        token_flow_two_insert_data["pool_token_number"] = "3"
                        new_rates1 = old_rates1
                    else:
                        token_account_ids = token_two_data["token_account_ids"]
                        token_in_index = token_account_ids.index(token_flow_two_insert_data["token_in"])
                        token_out_index = token_account_ids.index(token_flow_two_insert_data["revolve_token_one"])
                        new_rates1 = [old_rates1[token_in_index], old_rates1[token_out_index]]
                    stable_pool = {"amp": token_two_data["amp"], "total_fee": token_two_data["total_fee"],
                                   "token_account_ids": token_two_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_two_data["rates"]}
                    token_pair_ratio = get_swapped_amount(token_flow_two_insert_data["token_in"],
                                                          token_flow_two_insert_data["revolve_token_one"],
                                                          swap_number_grade, stable_pool,
                                                          token_two_data["stable_pool_decimal"])
                    token_flow_two_insert_data["token_pair_ratio"] = token_pair_ratio
                    token_flow_two_insert_data["token_in_amount"] = token_in_balance
                    token_flow_two_insert_data["revolve_one_out_amount"] = token_out_balance
                    token_flow_two_insert_data["amp"] = token_two_data["amp"]
                    token_flow_two_insert_data["rates"] = json.dumps(new_rates1)
                if token_one_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_two_insert_data["revolve_one_in_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_two_insert_data["revolve_token_one"]])
                    token_out_balance = int(token_flow_two_insert_data["token_out_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_two_insert_data["token_out"]])
                    token_flow_two_insert_data["revolve_token_one_ratio"] = get_token_flow_ratio(swap_number_grade,
                                                                                                 token_in_balance,
                                                                                                 token_out_balance,
                                                                                                 token_one_data[
                                                                                                     "total_fee"])
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_two_insert_data["token_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_two_insert_data["revolve_one_in_amount"])
                    token_out_balance = int(token_flow_two_insert_data["token_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    old_rates2 = token_one_data["rates"]
                    if len(token_one_data["token_account_ids"]) > 2:
                        c_amounts = token_one_data["three_c_amount"]
                        token_flow_two_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_two_insert_data["three_pool_ids"] = json.dumps(token_one_data["token_account_ids"])
                        token_flow_two_insert_data["revolve_one_pool_token_number"] = "3"
                        new_rates2 = old_rates2
                    else:
                        token_account_ids = token_one_data["token_account_ids"]
                        token_in_index = token_account_ids.index(token_flow_two_insert_data["revolve_token_one"])
                        token_out_index = token_account_ids.index(token_flow_two_insert_data["token_out"])
                        new_rates2 = [old_rates2[token_in_index], old_rates2[token_out_index]]
                    stable_pool = {"amp": token_one_data["amp"], "total_fee": token_one_data["total_fee"],
                                   "token_account_ids": token_one_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_one_data["rates"]}
                    token_pair_ratio = get_swapped_amount(token_flow_two_insert_data["revolve_token_one"],
                                                          token_flow_two_insert_data["token_out"], swap_number_grade,
                                                          stable_pool, token_one_data["stable_pool_decimal"])
                    token_flow_two_insert_data["revolve_token_one_ratio"] = token_pair_ratio
                    token_flow_two_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_two_insert_data["token_out_amount"] = token_out_balance
                    token_flow_two_insert_data["revolve_one_pool_amp"] = token_one_data["amp"]
                    token_flow_two_insert_data["revolve_one_pool_rates"] = json.dumps(new_rates2)
                final_ratio = (token_flow_two_insert_data["revolve_token_one_ratio"] / swap_number_grade) * (
                            token_flow_two_insert_data["token_pair_ratio"] / swap_number_grade)
                token_flow_two_insert_data["final_ratio"] = format_decimal_float(final_ratio)
                token_flow_insert_all_data_list.append(token_flow_two_insert_data)
                swap_number_grade = swap_number_grade * 10
            handle_grade_three(token_pair, token_pair_one, token_pair_two, token_two_data["pool_id"],
                               revolve_token_one, token_in_amount, revolve_one_out_amount, total_fee,
                               list_pool_data, token_flow_insert_all_data_list, token_two_data["total_fee"],
                               token_two_data["pool_kind"], token_two_data, decimals_data)


def handle_grade_three(token_pair, token_pair_one, token_pair_two, pool_id, revolve_token_one, token_in_amount,
                       revolve_one_out_amount, total_fee, list_pool_data, token_flow_insert_all_data_list, pool_fee,
                       pool_kind, one_pool_data, decimals_data):
    token_three_data_list = query_three_pools(revolve_token_one, token_pair_two, token_pair_one, list_pool_data)
    for token_three_data in token_three_data_list:
        revolve_token_two = ""
        revolve_one_in_amount = 0
        revolve_two_out_amount = 0
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
            swap_number_grade = 1
            for i in range(0, grade_number):
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
                    "revolve_one_out_amount": revolve_one_out_amount,
                    "revolve_one_in_amount": revolve_one_in_amount,
                    "revolve_two_out_amount": revolve_two_out_amount,
                    "revolve_two_in_amount": "0",
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
                    "swap_number_grade": swap_number_grade
                }
                if token_one_data["token_one"] == revolve_token_two and token_one_data["token_two"] == token_pair_two:
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_one_amount"]
                    token_flow_three_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
                elif token_one_data["token_one"] == revolve_token_two and token_one_data[
                    "token_three"] == token_pair_two:
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_one_amount"]
                    token_flow_three_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
                elif token_one_data["token_two"] == revolve_token_two and token_one_data["token_one"] == token_pair_two:
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_two_amount"]
                    token_flow_three_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
                elif token_one_data["token_two"] == revolve_token_two and token_one_data[
                    "token_three"] == token_pair_two:
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_two_amount"]
                    token_flow_three_insert_data["token_out_amount"] = token_one_data["token_three_amount"]
                elif token_one_data["token_three"] == revolve_token_two and token_one_data[
                    "token_one"] == token_pair_two:
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_three_amount"]
                    token_flow_three_insert_data["token_out_amount"] = token_one_data["token_one_amount"]
                elif token_one_data["token_three"] == revolve_token_two and token_one_data[
                    "token_two"] == token_pair_two:
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_one_data["token_three_amount"]
                    token_flow_three_insert_data["token_out_amount"] = token_one_data["token_two_amount"]
                if one_pool_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_three_insert_data["token_in_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_three_insert_data["token_in"]])
                    token_out_balance = int(token_flow_three_insert_data["revolve_one_out_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_one"]])
                    token_flow_three_insert_data["token_pair_ratio"] = get_token_flow_ratio(swap_number_grade,
                                                                                            token_in_balance,
                                                                                            token_out_balance,
                                                                                            total_fee)
                    token_flow_three_insert_data["token_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_one_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_three_insert_data["token_in_amount"])
                    token_out_balance = int(token_flow_three_insert_data["revolve_one_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    old_rates3 = one_pool_data["rates"]
                    if len(one_pool_data["token_account_ids"]) > 2:
                        c_amounts = one_pool_data["three_c_amount"]
                        token_flow_three_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_three_insert_data["three_pool_ids"] = json.dumps(one_pool_data["token_account_ids"])
                        token_flow_three_insert_data["pool_token_number"] = "3"
                        new_rates3 = old_rates3
                    else:
                        token_account_ids = one_pool_data["token_account_ids"]
                        token_in_index = token_account_ids.index(token_flow_three_insert_data["token_in"])
                        token_out_index = token_account_ids.index(token_flow_three_insert_data["revolve_token_one"])
                        new_rates3 = [old_rates3[token_in_index], old_rates3[token_out_index]]
                    stable_pool = {"amp": one_pool_data["amp"], "total_fee": one_pool_data["total_fee"],
                                   "token_account_ids": one_pool_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": one_pool_data["rates"]}
                    token_pair_ratio = get_swapped_amount(token_flow_three_insert_data["token_in"],
                                                          token_flow_three_insert_data["revolve_token_one"],
                                                          swap_number_grade, stable_pool,
                                                          one_pool_data["stable_pool_decimal"])
                    token_flow_three_insert_data["token_pair_ratio"] = token_pair_ratio
                    token_flow_three_insert_data["token_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_one_out_amount"] = token_out_balance
                    token_flow_three_insert_data["amp"] = one_pool_data["amp"]
                    token_flow_three_insert_data["rates"] = json.dumps(new_rates3)
                if token_three_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_three_insert_data["revolve_one_in_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_one"]])
                    token_out_balance = int(token_flow_three_insert_data["revolve_two_out_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_two"]])
                    token_flow_three_insert_data["revolve_token_one_ratio"] = get_token_flow_ratio(swap_number_grade,
                                                                                                   token_in_balance,
                                                                                                   token_out_balance,
                                                                                                   token_three_data[
                                                                                                       "total_fee"])
                    token_flow_three_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_two_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_three_insert_data["revolve_one_in_amount"])
                    token_out_balance = int(token_flow_three_insert_data["revolve_two_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    old_rates4 = token_three_data["rates"]
                    if len(token_three_data["token_account_ids"]) > 2:
                        c_amounts = token_three_data["three_c_amount"]
                        token_flow_three_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_three_insert_data["three_pool_ids"] = json.dumps(
                            token_three_data["token_account_ids"])
                        token_flow_three_insert_data["revolve_one_pool_token_number"] = "3"
                        new_rates4 = old_rates4
                    else:
                        token_account_ids = token_three_data["token_account_ids"]
                        token_in_index = token_account_ids.index(token_flow_three_insert_data["revolve_token_one"])
                        token_out_index = token_account_ids.index(token_flow_three_insert_data["revolve_token_two"])
                        new_rates4 = [old_rates4[token_in_index], old_rates4[token_out_index]]
                    stable_pool = {"amp": token_three_data["amp"], "total_fee": token_three_data["total_fee"],
                                   "token_account_ids": token_three_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_three_data["rates"]}
                    revolve_token_one_ratio = get_swapped_amount(token_flow_three_insert_data["revolve_token_one"],
                                                                 token_flow_three_insert_data["revolve_token_two"],
                                                                 swap_number_grade, stable_pool,
                                                                 token_three_data["stable_pool_decimal"])
                    token_flow_three_insert_data["revolve_token_one_ratio"] = revolve_token_one_ratio
                    token_flow_three_insert_data["revolve_one_in_amount"] = token_in_balance
                    token_flow_three_insert_data["revolve_two_out_amount"] = token_out_balance
                    token_flow_three_insert_data["revolve_one_pool_amp"] = token_three_data["amp"]
                    token_flow_three_insert_data["revolve_one_pool_rates"] = json.dumps(new_rates4)
                if token_one_data["pool_kind"] == "SIMPLE_POOL":
                    token_in_balance = int(token_flow_three_insert_data["revolve_two_in_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_three_insert_data["revolve_token_two"]])
                    token_out_balance = int(token_flow_three_insert_data["token_out_amount"]) / int(
                        "1" + "0" * decimals_data[token_flow_three_insert_data["token_out"]])
                    token_flow_three_insert_data["revolve_token_two_ratio"] = get_token_flow_ratio(swap_number_grade,
                                                                                                   token_in_balance,
                                                                                                   token_out_balance,
                                                                                                   token_one_data[
                                                                                                       "total_fee"])
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_in_balance
                    token_flow_three_insert_data["token_out_amount"] = token_out_balance
                else:
                    token_in_balance = int(token_flow_three_insert_data["revolve_two_in_amount"])
                    token_out_balance = int(token_flow_three_insert_data["token_out_amount"])
                    c_amounts = [token_in_balance, token_out_balance]
                    old_rates5 = token_one_data["rates"]
                    if len(token_one_data["token_account_ids"]) > 2:
                        c_amounts = token_one_data["three_c_amount"]
                        token_flow_three_insert_data["three_c_amount"] = json.dumps(c_amounts)
                        token_flow_three_insert_data["three_pool_ids"] = json.dumps(token_one_data["token_account_ids"])
                        token_flow_three_insert_data["revolve_two_pool_token_number"] = "3"
                        new_rates5 = old_rates5
                    else:
                        token_account_ids = token_one_data["token_account_ids"]
                        token_in_index = token_account_ids.index(token_flow_three_insert_data["revolve_token_two"])
                        token_out_index = token_account_ids.index(token_flow_three_insert_data["token_out"])
                        new_rates5 = [old_rates5[token_in_index], old_rates5[token_out_index]]
                    stable_pool = {"amp": token_one_data["amp"], "total_fee": token_one_data["total_fee"],
                                   "token_account_ids": token_one_data["token_account_ids"],
                                   "c_amounts": c_amounts, "rates": token_one_data["rates"]}
                    revolve_token_two_ratio = get_swapped_amount(token_flow_three_insert_data["revolve_token_two"],
                                                                 token_flow_three_insert_data["token_out"],
                                                                 swap_number_grade, stable_pool,
                                                                 token_one_data["stable_pool_decimal"])
                    token_flow_three_insert_data["revolve_token_two_ratio"] = revolve_token_two_ratio
                    token_flow_three_insert_data["revolve_two_in_amount"] = token_in_balance
                    token_flow_three_insert_data["token_out_amount"] = token_out_balance
                    token_flow_three_insert_data["revolve_two_pool_amp"] = token_one_data["amp"]
                    token_flow_three_insert_data["revolve_two_pool_rates"] = json.dumps(new_rates5)
                final_ratio = (token_flow_three_insert_data["token_pair_ratio"] / swap_number_grade) * \
                              (token_flow_three_insert_data["revolve_token_one_ratio"] / swap_number_grade) * \
                              (token_flow_three_insert_data["revolve_token_two_ratio"] / swap_number_grade)
                token_flow_three_insert_data["final_ratio"] = format_decimal_float(final_ratio)
                token_flow_insert_all_data_list.append(token_flow_three_insert_data)
                swap_number_grade = swap_number_grade * 10


if __name__ == "__main__":
    print("#########TOKEN FLOW START###########")

    if len(sys.argv) == 2:
        start_time = int(time.time())
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            start_time = int(time.time())
            list_top_pools_data = get_list_top_pools(network_id)
            token_decimals_data = get_token_decimal(list_top_pools_data)
            end_time1 = int(time.time())
            print("get_list_top_pools consuming:", end_time1 - start_time)
            stable_and_rated_pool = get_stable_and_rated_pool_data(network_id, list_top_pools_data)
            end_time10 = int(time.time())
            print("get_stable_and_rated_pool_data consuming:", end_time10 - end_time1)
            pools_data_list_ten, black_list_pool10 = handle_list_pool_data(stable_and_rated_pool, list_top_pools_data,
                                                                           10, network_id)
            pools_data_list_zero, black_list_pool = handle_list_pool_data(stable_and_rated_pool, list_top_pools_data,
                                                                          0, network_id)
            end_time2 = int(time.time())
            print("handle_list_pool_data consuming:", end_time2 - end_time1)
            token_flow_insert_data_list_new = handle_flow_grade_new(pools_data_list_ten, network_id,
                                                                    token_decimals_data, pools_data_list_zero)
            end_time3 = int(time.time())
            print("handle_flow_grade consuming:", end_time3 - end_time2)
            add_token_flow_to_redis(network_id, token_flow_insert_data_list_new, black_list_pool)
            end_time4 = int(time.time())
            print("add_token_flow_to_redis consuming:", end_time4 - end_time3)
            end_time = int(time.time())
            print("total consuming:", end_time - start_time)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("#########TOKEN FLOW START###########")

    # network_id = "MAINNET"
    # start_time = int(time.time())
    # list_top_pools_data = get_list_top_pools(network_id)
    # token_decimals_data = get_token_decimal(list_top_pools_data)
    # end_time1 = int(time.time())
    # print("get_list_top_pools consuming:", end_time1 - start_time)
    # stable_and_rated_pool = get_stable_and_rated_pool_data(network_id, list_top_pools_data)
    # end_time10 = int(time.time())
    # print("get_stable_and_rated_pool_data consuming:", end_time10 - end_time1)
    # pools_data_list_ten, black_list_pool10 = handle_list_pool_data(stable_and_rated_pool, list_top_pools_data,
    #                                                                10, network_id)
    # pools_data_list_zero, black_list_pool = handle_list_pool_data(stable_and_rated_pool, list_top_pools_data,
    #                                                               0, network_id)
    # end_time2 = int(time.time())
    # print("handle_list_pool_data consuming:", end_time2 - end_time1)
    # token_flow_insert_data_list_new = handle_flow_grade_new(pools_data_list_ten, network_id,
    #                                                         token_decimals_data, pools_data_list_zero)
    # end_time3 = int(time.time())
    # print("handle_flow_grade consuming:", end_time3 - end_time2)
    # add_token_flow_to_redis(network_id, token_flow_insert_data_list_new, black_list_pool)
    # end_time4 = int(time.time())
    # print("add_token_flow_to_redis consuming:", end_time4 - end_time3)
    # end_time = int(time.time())
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

    # network_id = "MAINNET"
    # list_top_pools_data = get_list_top_pools(network_id)
    # handle_token_pair(list_top_pools_data)
    # print(1)
