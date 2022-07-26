import sys
sys.path.append('../')

import sys
from db_provider import summary_hourly_price, price_report


def handle_token_price_report(network_id):
    try:
        summary_hourly_price()
        price_report(network_id)
    except Exception as e:
        print("Error occurred when token price report, Error is: ", e)


if __name__ == '__main__':
    # update_price("TESTNET")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            handle_token_price_report(network_id)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
