import sys
import requests
import json
sys.path.append('../')


def handel_dcl_points():
    dcl_pool_list = []
    request_url = "https://172.17.0.1:8081/pool/search?type=dcl&sort=24h&limit=30&labels=&offset=0&hide_low_pool=false&hide_uncertified_token=false&order_by=desc&token_type=&token_list=&pool_id_list="
    dcl_pool_list_ret = requests.get(request_url).text
    dcl_pool_data_ret = json.loads(dcl_pool_list_ret)
    if dcl_pool_data_ret["code"] == 0:
        dcl_pool_data_list = dcl_pool_data_ret["data"]["list"]
        for dcl_pool_data in dcl_pool_data_list:
            if float(dcl_pool_data["volume_24h"]) > 0:
                dcl_pool_list.append(dcl_pool_data["id"])
    for pool_id in dcl_pool_list:
        request_url = "https://172.17.0.1:8000/get-dcl-points?pool_id=" + pool_id
        print("request_url:", request_url)
        dcl_pool_point = requests.get(request_url).text
        print("dcl_pool_point:", dcl_pool_point)


if __name__ == '__main__':
    print("start rhea data task")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            handel_dcl_points()
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("end dcl_points task")
