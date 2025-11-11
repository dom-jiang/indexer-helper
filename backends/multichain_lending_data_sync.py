import json
import sys
sys.path.append('../')

import sys
from db_provider import update_multichain_lending_report_data, get_multichain_lending_report_data, get_multichain_lending_requests_history


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
    except Exception as e:
        print("Error handle_multichain_lending_data, Error is: ", e)


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
    print("end handle_multichain_lending_data")
