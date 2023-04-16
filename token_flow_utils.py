import sys

sys.path.append('/')
import json
from config import Cfg
import time
from near_multinode_rpc_provider import MultiNodeJsonProviderError, MultiNodeJsonProvider
from typing import *
import decimal

fee_divisor = 10000


class Fees:
    FeeDivsor = 10000

    def __init__(self, trade_fee: int, admin_fee: int):
        self.trade_fee = trade_fee
        self.admin_fee = admin_fee

    def tradeFee(self, amount: int) -> int:
        return amount * self.trade_fee / Fees.FeeDivsor

    def adminFee(self, amount: int) -> int:
        return amount * self.admin_fee / Fees.FeeDivsor

    def normalized_trade_fee(self, num_coins: int, amount: int) -> int:
        adjusted_trade_fee = int(
            (self.trade_fee * num_coins) / (4 * (num_coins - 1)))
        return amount * adjusted_trade_fee / Fees.FeeDivsor


"""
Calculate invariant D
:param amp: factor A, can get from get_stable_pool interface;
:param c_amounts: vector of c_amounts in the pool, can get from get_stable_pool interface;
:return: invariant D
"""


def calc_d(amp: int, c_amounts: List[int]) -> int:
    n_coins = len(c_amounts)
    sum_amounts = sum(c_amounts)
    # Newton Iteration to resolve equation of higher degree
    #   previous approaching value
    d_prev = 0
    #   initial approaching value
    d = sum_amounts
    #   Max 256 round approaching iteration
    for i in range(256):
        #   to calc D_prod, as much precise as possible
        d_prod = d
        for c_amount in c_amounts:
            d_prod = d_prod * d / (c_amount * n_coins)
        #   store previous approaching value
        d_prev = d
        #   get cur-round approaching value
        ann = amp * n_coins ** n_coins
        # d = (ann * sum_amounts + d_prod * n_coins) * d_prev / ((ann - 1) * d_prev + (n_coins + 1) * d_prod)
        numerator = d_prev * (d_prod * n_coins + ann * sum_amounts)
        denominator = d_prev * (ann - 1) + d_prod * (n_coins + 1)
        d = numerator / denominator
        #   iteration terminating condition
        if abs(d - d_prev) <= 1:
            break

    return d


def calc_y(
        amp: int,
        x_c_amount: int,
        current_c_amounts: List[int],
        index_x: int,
        index_y: int
) -> int:
    n_coins = len(current_c_amounts)
    ann = amp * n_coins ** n_coins

    # invariant D
    d = calc_d(amp, current_c_amounts)

    # Solve for y by approximating: y**2 + b*y = c
    s_ = x_c_amount
    c = d * d / x_c_amount
    for i in range(n_coins):
        if i != index_x and i != index_y:
            s_ += current_c_amounts[i]
            c = c * d / current_c_amounts[i]
    c = c * d / (ann * n_coins ** n_coins)
    b = d / ann + s_

    # Newton Iteration to resolve equation of higher degree
    y_prev = 0
    y = d
    for i in range(256):
        y_prev = y
        # $ y_{k+1} = \frac{y_k^2 + c}{2y_k + b - D} $
        y_numerator = y ** 2 + c
        y_denominator = 2 * y + b - d
        y = y_numerator / y_denominator
        #   iteration terminating condition
        if abs(y - y_prev) <= 1:
            break

    return y


"""
Calc swap result (get_return)
:param amp: factor A, can get from get_stable_pool interface;
:param in_token_idx: token in index, starts from 0
:param in_c_amount: depositing token c_amount
:param out_token_idx: token out index, starts from 0
:param old_c_amounts: vector of currently c_amounts in the pool, can get from get_stable_pool interface;
:param fees: (fee ratio in bps, protocol/fee rate in bps)
:return: [swap out token's c_amount, fee c_amount]
"""


def calc_swap(
        amp: int,
        in_token_idx: int,
        in_c_amount: int,
        out_token_idx: int,
        old_c_amounts: List[int],
        fees: Fees
) -> Tuple[int, int]:
    # the new Y token's c_amount
    y = calc_y(amp, in_c_amount + old_c_amounts[in_token_idx], old_c_amounts, in_token_idx, out_token_idx)
    # swap out c_amount if no fee
    dy = old_c_amounts[out_token_idx] - y
    if dy > 0:
        # off-by-one issue
        dy = dy - 1
    # apply fee policy
    trade_fee = fees.tradeFee(dy)
    # real swapped out c_amount
    amount_swapped = dy - trade_fee
    return amount_swapped, trade_fee


def handle_trade_fee(amount, trade_fee):
    return (amount * trade_fee) / fee_divisor


def handle_stable_pool_decimal(pool_kind):
    if pool_kind == "STABLE_SWAP":
        return 18
    else:
        return 24


def shrink_token(amount, decimals):
    return int(amount) / int("1" + "0" * decimals)


def expand_token(amount, decimals):
    return int(amount) * int("1" + "0" * decimals)


def get_stable_and_rated_pool(network_id, pool_ids):
    contract = Cfg.NETWORK[network_id]["REF_CONTRACT"]
    stable_pool_list = {}

    try:
        conn = MultiNodeJsonProvider(network_id)
        rated_pool_ids = pool_ids["rated_pool"]
        for i in range(0, len(rated_pool_ids)):
            print("pool_id:", rated_pool_ids[i])
            time.sleep(0.1)
            ret = conn.view_call(contract, "get_rated_pool", ('{"pool_id": %s}' % rated_pool_ids[i]).encode(encoding='utf-8'))
            # print("ret:", ret)
            json_str = "".join([chr(x) for x in ret["result"]])
            rated_pool = json.loads(json_str)
            stable_pool_list[rated_pool_ids[i]] = rated_pool

        stable_pool_pool_ids = pool_ids["stable_pool"]
        for i in range(0, len(stable_pool_pool_ids)):
            print("pool_id:", stable_pool_pool_ids[i])
            time.sleep(0.1)
            ret = conn.view_call(contract, "get_stable_pool", ('{"pool_id": %s}' % stable_pool_pool_ids[i]).encode(encoding='utf-8'))
            # print("ret:", ret)
            json_str = "".join([chr(x) for x in ret["result"]])
            stable_pool = json.loads(json_str)

            if len(stable_pool["token_account_ids"]) > 2:
                stable_pool["rates"] = [expand_token(1, 18), expand_token(1, 18), expand_token(1, 18)]
            else:
                stable_pool["rates"] = [expand_token(1, 18), expand_token(1, 18)]
            stable_pool_list[stable_pool_pool_ids[i]] = stable_pool
        return stable_pool_list
    except MultiNodeJsonProviderError as e:
        print("RPC Error: ", e)
    except Exception as e:
        print("Error: ", e)


def get_swapped_amount(token_in_id, token_out_id, amount_in, stable_pool, stable_pool_decimal):
    amp = stable_pool["amp"]
    trade_fee = Fees(stable_pool["total_fee"], 2000)
    in_token_idx = stable_pool["token_account_ids"].index(token_in_id)
    out_token_idx = stable_pool["token_account_ids"].index(token_out_id)
    stable_lp_token_decimals = stable_pool_decimal
    rates = stable_pool["rates"]
    base_old_c_amounts = []
    for c_amount in stable_pool["c_amounts"]:
        base_old_c_amounts.append(shrink_token(c_amount, stable_lp_token_decimals))
    old_c_amounts = []
    for i in range(0, len(base_old_c_amounts)):
        old_c_amounts.append(expand_token(base_old_c_amounts[i] * int(rates[i]), stable_lp_token_decimals))
    in_c_amount = expand_token(amount_in * int(rates[in_token_idx]), stable_lp_token_decimals)
    (amount_swapped, fee) = calc_swap(
        amp,
        in_token_idx,
        in_c_amount,
        out_token_idx,
        old_c_amounts,
        trade_fee
    )
    amount_swapped = amount_swapped / int(rates[out_token_idx])
    if amount_swapped < 0:
        amount_out = "0"
    else:
        amount_out = amount_swapped
    return shrink_token(amount_out, stable_lp_token_decimals)


def combine_token_flow(token_flow_data_list, swap_amount):
    now_time = int(time.time())
    max_ratio = 0.00
    max_token_pair_data = {}
    for token_pair_data in token_flow_data_list:
        # if "'1910'" in token_pair_data["pool_ids"]:
        #     continue
        grade_ratio = 0.00
        if token_pair_data["grade"] == "1":
            if token_pair_data["pool_kind"] == "SIMPLE_POOL":
                grade_1_ratio = get_token_flow_ratio(swap_amount, token_pair_data["token_in_amount"],
                                                     token_pair_data["token_out_amount"], token_pair_data["pool_fee"])
            else:
                grade_1_ratio = get_stable_and_rated_pool_ratio(token_pair_data["pool_token_number"],
                                                                json.loads(token_pair_data["three_pool_ids"]),
                                                                json.loads(token_pair_data["three_c_amount"]),
                                                                token_pair_data["token_in"],
                                                                token_pair_data["token_out"],
                                                                token_pair_data["token_in_amount"],
                                                                token_pair_data["token_out_amount"],
                                                                token_pair_data["amp"],
                                                                token_pair_data["pool_fee"],
                                                                json.loads(token_pair_data["rates"]),
                                                                token_pair_data["pool_kind"])
            token_pair_data["token_pair_ratio"] = '%.8f' % grade_1_ratio
            token_pair_data["final_ratio"] = '%.8f' % (float(grade_1_ratio) / float(swap_amount))
            grade_ratio = float(grade_1_ratio)
        if token_pair_data["grade"] == "2":
            if token_pair_data["pool_kind"] == "SIMPLE_POOL":
                grade_2_ratio_one = get_token_flow_ratio(swap_amount, token_pair_data["token_in_amount"],
                                                         token_pair_data["revolve_one_out_amount"],
                                                         token_pair_data["pool_fee"])
            else:
                grade_2_ratio_one = get_stable_and_rated_pool_ratio(token_pair_data["pool_token_number"],
                                                                    json.loads(token_pair_data["three_pool_ids"]),
                                                                    json.loads(token_pair_data["three_c_amount"]),
                                                                    token_pair_data["token_in"],
                                                                    token_pair_data["revolve_token_one"],
                                                                    token_pair_data["token_in_amount"],
                                                                    token_pair_data["revolve_one_out_amount"],
                                                                    token_pair_data["amp"],
                                                                    token_pair_data["pool_fee"],
                                                                    json.loads(token_pair_data["rates"]),
                                                                    token_pair_data["pool_kind"])
            if token_pair_data["revolve_one_pool_kind"] == "SIMPLE_POOL":
                grade_2_ratio_two = (get_token_flow_ratio(swap_amount, token_pair_data["revolve_one_in_amount"],
                                                          token_pair_data["token_out_amount"],
                                                          token_pair_data["revolve_one_pool_fee"])) / float(swap_amount)
            else:
                grade_2_ratio_two = get_stable_and_rated_pool_ratio(token_pair_data["revolve_one_pool_token_number"],
                                                                    json.loads(token_pair_data["three_pool_ids"]),
                                                                    json.loads(token_pair_data["three_c_amount"]),
                                                                    token_pair_data["revolve_token_one"],
                                                                    token_pair_data["token_out"],
                                                                    token_pair_data["revolve_one_in_amount"],
                                                                    token_pair_data["token_out_amount"],
                                                                    token_pair_data["revolve_one_pool_amp"],
                                                                    token_pair_data["revolve_one_pool_fee"],
                                                                    json.loads(
                                                                        token_pair_data["revolve_one_pool_rates"]),
                                                                    token_pair_data["revolve_one_pool_kind"])
            grade_2_ratio = '%.8f' % (grade_2_ratio_one * grade_2_ratio_two)
            token_pair_data["token_pair_ratio"] = '%.8f' % grade_2_ratio_one
            token_pair_data["revolve_token_one_ratio"] = '%.8f' % grade_2_ratio_two
            token_pair_data["final_ratio"] = '%.8f' % (float(grade_2_ratio) / float(swap_amount))
            grade_ratio = float(grade_2_ratio)
        if token_pair_data["grade"] == "3":
            if token_pair_data["pool_kind"] == "SIMPLE_POOL":
                grade_3_ratio_one = get_token_flow_ratio(swap_amount, token_pair_data["token_in_amount"],
                                                         token_pair_data["revolve_one_out_amount"],
                                                         token_pair_data["pool_fee"])
            else:
                grade_3_ratio_one = get_stable_and_rated_pool_ratio(token_pair_data["pool_token_number"],
                                                                    json.loads(token_pair_data["three_pool_ids"]),
                                                                    json.loads(token_pair_data["three_c_amount"]),
                                                                    token_pair_data["token_in"],
                                                                    token_pair_data["revolve_token_one"],
                                                                    token_pair_data["token_in_amount"],
                                                                    token_pair_data["revolve_one_out_amount"],
                                                                    token_pair_data["amp"],
                                                                    token_pair_data["pool_fee"],
                                                                    json.loads(token_pair_data["rates"]),
                                                                    token_pair_data["pool_kind"])
            if token_pair_data["revolve_one_pool_kind"] == "SIMPLE_POOL":
                grade_3_ratio_two = (get_token_flow_ratio(swap_amount, token_pair_data["revolve_one_in_amount"],
                                                          token_pair_data["revolve_two_out_amount"],
                                                          token_pair_data["revolve_one_pool_fee"])) / float(swap_amount)
            else:
                grade_3_ratio_two = get_stable_and_rated_pool_ratio(token_pair_data["revolve_one_pool_token_number"],
                                                                    json.loads(token_pair_data["three_pool_ids"]),
                                                                    json.loads(token_pair_data["three_c_amount"]),
                                                                    token_pair_data["revolve_token_one"],
                                                                    token_pair_data["revolve_token_two"],
                                                                    token_pair_data["revolve_one_in_amount"],
                                                                    token_pair_data["revolve_two_out_amount"],
                                                                    token_pair_data["revolve_one_pool_amp"],
                                                                    token_pair_data["revolve_one_pool_fee"],
                                                                    json.loads(
                                                                        token_pair_data["revolve_one_pool_rates"]),
                                                                    token_pair_data["revolve_one_pool_kind"])
            if token_pair_data["revolve_two_pool_kind"] == "SIMPLE_POOL":
                grade_3_ratio_three = (get_token_flow_ratio(swap_amount, token_pair_data["revolve_two_in_amount"],
                                                            token_pair_data["token_out_amount"],
                                                            token_pair_data["revolve_two_pool_fee"])) / float(
                    swap_amount)
            else:
                grade_3_ratio_three = get_stable_and_rated_pool_ratio(token_pair_data["revolve_two_pool_token_number"],
                                                                      json.loads(token_pair_data["three_pool_ids"]),
                                                                      json.loads(token_pair_data["three_c_amount"]),
                                                                      token_pair_data["revolve_token_two"],
                                                                      token_pair_data["token_out"],
                                                                      token_pair_data["revolve_two_in_amount"],
                                                                      token_pair_data["token_out_amount"],
                                                                      token_pair_data["revolve_two_pool_amp"],
                                                                      token_pair_data["revolve_two_pool_fee"],
                                                                      json.loads(
                                                                          token_pair_data["revolve_two_pool_rates"]),
                                                                      token_pair_data["revolve_two_pool_kind"])
            grade_3_ratio = '%.8f' % (grade_3_ratio_one * grade_3_ratio_two * grade_3_ratio_three)
            token_pair_data["token_pair_ratio"] = '%.8f' % grade_3_ratio_one
            token_pair_data["revolve_token_one_ratio"] = '%.8f' % grade_3_ratio_two
            token_pair_data["revolve_token_two_ratio"] = '%.8f' % grade_3_ratio_three
            token_pair_data["final_ratio"] = '%.8f' % (float(grade_3_ratio) / float(swap_amount))
            grade_ratio = float(grade_3_ratio)
        if grade_ratio > max_ratio:
            max_ratio = grade_ratio
            max_token_pair_data = token_pair_data
            max_token_pair_data["amount"] = max_ratio
            max_token_pair_data["swap_amount"] = swap_amount
            max_token_pair_data["timestamp"] = str(now_time)
    return token_flow_return_data(max_token_pair_data)


def token_flow_return_data(max_token_pair_data):
    ret = []
    all_tokens = []
    all_pool_fees = []
    all_tokens.append(max_token_pair_data["token_in"])
    all_pool_fees.append(max_token_pair_data["pool_fee"])
    if max_token_pair_data["revolve_token_one"] != "":
        all_tokens.append(max_token_pair_data["revolve_token_one"])
        all_pool_fees.append(max_token_pair_data["revolve_one_pool_fee"])
    if max_token_pair_data["revolve_token_two"] != "":
        all_tokens.append(max_token_pair_data["revolve_token_two"])
        all_pool_fees.append(max_token_pair_data["revolve_two_pool_fee"])
    all_tokens.append(max_token_pair_data["token_out"])
    ret_data = {
        "token_pair": max_token_pair_data["token_pair"],
        "grade": max_token_pair_data["grade"],
        "pool_ids": json.loads(max_token_pair_data["pool_ids"]),
        "token_in": max_token_pair_data["token_in"],
        "revolve_token_one": max_token_pair_data["revolve_token_one"],
        "revolve_token_two": max_token_pair_data["revolve_token_two"],
        "token_out": max_token_pair_data["token_out"],
        "final_ratio": float(max_token_pair_data["final_ratio"]),
        "pool_fee": max_token_pair_data["pool_fee"],
        "revolve_one_pool_fee": max_token_pair_data["revolve_one_pool_fee"],
        "revolve_two_pool_fee": max_token_pair_data["revolve_two_pool_fee"],
        "amount": max_token_pair_data["amount"],
        "swap_amount": max_token_pair_data["swap_amount"],
        "all_tokens": all_tokens,
        "all_pool_fees": all_pool_fees,
        "timestamp": max_token_pair_data["timestamp"]
    }
    ret.append(ret_data)
    return ret


def get_stable_and_rated_pool_ratio(pool_token_number, three_pool_ids, three_c_amount, token_in, token_out,
                                    token_in_amount, token_out_amount, amp, total_fee, rates, pool_kind):
    if pool_token_number == "3":
        token_account_ids = three_pool_ids
        c_amounts = three_c_amount
    else:
        token_account_ids = [token_in, token_out]
        c_amounts = [token_in_amount, token_out_amount]
    stable_pool = {"amp": amp, "total_fee": total_fee, "token_account_ids": token_account_ids,
                   "c_amounts": c_amounts, "rates": rates}
    revolve_token_one_ratio = get_swapped_amount(token_in, token_out, 1, stable_pool,
                                                 handle_stable_pool_decimal(pool_kind))
    revolve_token_one_ratio = '%.8f' % revolve_token_one_ratio
    return float(revolve_token_one_ratio)


def get_token_flow_ratio(token_in_amount, token_in_balance, token_out_balance, fee):
    try:
        token_in_amount = decimal.Decimal(token_in_amount)
        token_in_balance = decimal.Decimal(token_in_balance)
        token_out_balance = decimal.Decimal(token_out_balance)
        fee = decimal.Decimal(fee)
        ratio = token_in_amount * (10000 - fee) * token_out_balance / (
                10000 * token_in_balance + token_in_amount * (10000 - fee))
        ratio = '%.8f' % ratio
        return float(ratio)
    except Exception as e:
        print("get ratio error:", e)
        return 0
    # a, b = str(ratio).split('.')
    # return float(a + '.' + b[0:6])


if __name__ == "__main__":
    print("#########TOKEN FLOW START###########")

    # start_time = int(time.time())
    # # pool_data = get_stable_pool("MAINNET", )
    # pool_ids = {"rated_pool": ["3514", "3689", "3515", "3688", "3612"], "stable_pool": ["3020", "3433", "3364", "1910"]}
    # pool_data = get_stable_and_rated_pool("MAINNET", pool_ids)
    # print(pool_data)
    # end_time1 = int(time.time())
    # print("get_stable_pool consuming:", end_time1 - start_time)
    stable_pool_test = {'token_account_ids': ['v2-nearx.stader-labs.near', 'wrap.near'], 'decimals': [24, 24],
                        'amounts': ['595424567616013857394550484133', '468570579301070362343190123358'],
                        'c_amounts': ['595424567616013857394550484133', '468570579301070362343190123358'],
                        'total_fee': 5, 'shares_total_supply': '1072529148949520003479920392363', 'amp': 240,
                        'rates': ['1107403203106830712636824', '1000000000000000000000000']}
    # stable_pool_test["rates"] = [expand_token(1, 18), expand_token(1, 18)]
    res = get_swapped_amount("v2-nearx.stader-labs.near", "wrap.near", 1, stable_pool_test, 24)
    print(res)
