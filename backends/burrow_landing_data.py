import json
import time
import requests


def handel_burrow_data():
    yield_apy_up_to_token = ["17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1",
                             "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near",
                             "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near",
                             "6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near"]
    burrow_data = {"total_supplied": "", "yield_apy": "", "yield_apy_up_to": ""}
    burrow_token_url = "https://api.burrow.finance/list_token_data"
    requests.packages.urllib3.disable_warnings()
    token_data_ret = requests.get(url=burrow_token_url, verify=False)
    ret_token_data = json.loads(token_data_ret.text)
    if "code" in ret_token_data and ret_token_data["code"] == "0":
        api_data_list = []
        total_supplied = 0
        token_data_list = ret_token_data["data"]
        for token_data in token_data_list:
            if token_data["token"] == "wrap.near":
                burrow_data["yield_apy"] = token_data["supply_apy"] + "%"
            total_supplied += float(token_data["total_supplied_balance"])
            if token_data["token"] in yield_apy_up_to_token:
                api_data_list.append(float(token_data["base_apy"]))
        max_api = 0
        for da in api_data_list:
            if da > max_api:
                max_api = da
        burrow_data["yield_apy_up_to"] = str(max_api) + "%"
        burrow_data["total_supplied"] = str(total_supplied)
        query_url = "https://api.data-service.burrow.finance/add_burrow_data/" + json.dumps(burrow_data)
        requests.packages.urllib3.disable_warnings()
        ret = requests.post(url=query_url, verify=False)
        ret_data = ret.text
    else:
        ret_data = ret_token_data
    return ret_data


if __name__ == "__main__":
    print("Staring handel_burrow_data ...")
    start_time = int(time.time())
    data = handel_burrow_data()
    print(data)
    end_time = int(time.time())
    print("handel_burrow_data consuming time:{}", start_time - end_time)
