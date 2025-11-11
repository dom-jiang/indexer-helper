import json
import sys
import requests
sys.path.append('../')

from db_provider import update_multichain_lending_report_data, get_multichain_lending_report_data, get_multichain_lending_requests_history
from near_multinode_rpc_provider import MultiNodeJsonProvider


def handle_multichain_lending_data(network_id):
    try:
        multichain_lending_list = get_multichain_lending_report_data(network_id)
        for multichain_lending in multichain_lending_list:
            if multichain_lending["type"] == 1:
                requests_history = get_multichain_lending_requests_history(network_id, multichain_lending["batch_id"])
                if requests_history is not None:
                    request_result = json.loads(requests_history["request_result"])
                    tx_hash = ""
                    if "tx_hash" in request_result:
                        tx_hash = request_result["tx_hash"]
                    update_multichain_lending_report_data(network_id, requests_history["request_result"], multichain_lending["id"], tx_hash)
            elif multichain_lending["type"] == 2:
                request_hash = multichain_lending["request_hash"]
                tx_status = check_tx_status_direct(network_id, request_hash)
                if tx_status is not None:
                    update_multichain_lending_report_data(network_id, json.dumps(tx_status), multichain_lending["id"], request_hash)
    except Exception as e:
        print("Error handle_multichain_lending_data, Error is: ", e)


def check_tx_status_direct(network_id, tx_hash):
    ret_data = {"tx_hash": tx_hash, "tx_err_msg": ""}
    conn = MultiNodeJsonProvider(network_id)
    try:
        ret = conn.get_tx(tx_hash, "intents.near")
        if "status" in ret:
            status = ret['status']
            if 'SuccessValue' in status:
                return ret_data
            elif 'Failure' in status:
                ret_data["tx_err_msg"] = json.dumps(status['Failure'])
                return ret_data
    except requests.exceptions.RequestException as e:
        print(f"网络请求错误: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON 解析错误: {e}")
    except Exception as e:
        print(f"未知错误: {e}")
    return None


if __name__ == '__main__':
    print("start handle_multichain_lending_data")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            handle_multichain_lending_data(network_id)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)

    # handle_multichain_lending_data("MAINNET")
    # rr = check_tx_status_direct("MAINNET", "GGb8aFcY4KepPYtPVVp4eDqLKtdXLDeVzsXWnuiVuWi4")
    # print("1111111:", json.dumps(rr))
    print("end handle_multichain_lending_data")
