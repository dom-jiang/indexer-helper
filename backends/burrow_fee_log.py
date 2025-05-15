import sys
import requests
import json
sys.path.append('../')
from redis_provider import RedisProvider, get_burrow_total_fee, get_burrow_total_revenue
from db_provider import get_burrow_fee_log_data, update_burrow_fee_log_data


def handel_burrow_fee_log(network_id):
    log_id_list = []
    price_url = "https://api.ref.finance/list-token-price"
    token_price_data = requests.get(price_url).text
    token_price_data = json.loads(token_price_data)
    config_url = "https://api.burrow.finance/get_assets_paged_detailed"
    token_config_ret = requests.get(config_url).text
    token_config_ret = json.loads(token_config_ret)
    if token_config_ret["code"] == "0":
        token_config_data = {}
        for token_config in token_config_ret["data"]:
            token_config_data[token_config["token_id"]] = token_config["config"]["extra_decimals"]
        burrow_fee_log_list = get_burrow_fee_log_data(network_id)
        burrow_total_fee = get_burrow_total_fee()
        if burrow_total_fee is None:
            burrow_total_fee = 0
        else:
            burrow_total_fee = float(burrow_total_fee)
        burrow_total_revenue = get_burrow_total_revenue()
        if burrow_total_revenue is None:
            burrow_total_revenue = 0
        else:
            burrow_total_revenue = float(burrow_total_revenue)
        for burrow_fee_log in burrow_fee_log_list:
            token_id = burrow_fee_log["token_id"]
            if token_id not in token_price_data:
                print("token not in token_price_data:", token_id)
                continue
            if token_id not in token_config_data:
                print("token not in token_config_ret:", token_id)
                continue
            usd_token_decimal = token_price_data[token_id]["decimal"] + token_config_data[token_id]
            burrow_total_fee += int(burrow_fee_log["interest"]) / int("1" + "0" * usd_token_decimal)
            burrow_total_revenue += (int(burrow_fee_log["prot_fee"]) + int(burrow_fee_log["reserved"])) / int("1" + "0" * usd_token_decimal)
            log_id_list.append(burrow_fee_log["id"])

        conn = RedisProvider()
        conn.begin_pipe()
        conn.add_burrow_total_fee(burrow_total_fee)
        conn.add_burrow_total_revenue(burrow_total_revenue)
        conn.end_pipe()
        conn.close()
        if len(log_id_list) > 0:
            update_burrow_fee_log_data(network_id, log_id_list)


if __name__ == '__main__':
    print("start burrow fee log task")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            handel_burrow_fee_log(network_id)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("end burrow fee log task")
