import json
import sys

sys.path.append('../')
from selenium import webdriver
import time
from bs4 import BeautifulSoup
import requests


def handel_burrow_data():
    # 实例化参数方法
    chrome_options = webdriver.ChromeOptions()
    # 设置浏览器的无头浏览器, 无界面, 浏览器将不提供界面, Linux操作系统无界面下就可以运行
    chrome_options.add_argument("--headless")
    # 解决devtoolsactiveport文件不存在的报错
    chrome_options.add_argument("--no-sandbox")
    # 官方推荐的关闭选项, 规避一些BUG
    chrome_options.add_argument("--disable-gpu")
    # 设置中文
    chrome_options.add_argument('lang=zh_CN.UTF-8')
    chrome_options.add_argument('sec-fetch-site=same-origin')
    chrome_options.add_argument('sec-fetch-mode=no-cors')
    chrome_options.add_argument('sec-ch-ua-platform="Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"')
    chrome_options.add_argument('sec-ch-ua="Windows"')
    chrome_options.add_argument('sec-ch-ua-mobile=?0')
    chrome_options.add_argument('referer=https://app.burrow.fun/')
    chrome_options.add_argument('accept-language=zh-CN,zh;q=0.9')
    # 更换头部
    chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"')
    # 实例化chrome, 导入设置项
    wd = webdriver.Chrome(options=chrome_options)
    # 最大化
    wd.maximize_window()
    # 打开网页
    wd.get("https://app.burrow.finance/")
    time.sleep(2)
    # 获取网页内容
    content = wd.page_source
    # 释放,退出
    wd.quit()

    # driver = webdriver.Chrome()
    # driver.get("https://app.burrow.finance/")
    # element = driver.page_source
    # driver.quit()

    # print(content)
    burrow_data = {"total_supplied": "", "yield_apy": "", "yield_apy_up_to": ""}
    data_list = []
    api_data_list = []
    soup = BeautifulSoup(content, 'html.parser')
    elements = soup.find_all('span')
    for element in elements:
        data_list.append(element.get_text())
    burrow_data["total_supplied"] = data_list[data_list.index("Total Supplied") + 1]
    burrow_data["yield_apy"] = data_list[data_list.index("NEAR") + 5]
    usdc_api = float(data_list[data_list.index("USDC") + 6][:-1])
    api_data_list.append(usdc_api)
    usdc_e_api = float(data_list[data_list.index("USDC.e") + 5][:-1])
    api_data_list.append(usdc_e_api)
    usdt_api = float(data_list[data_list.index("USDt") + 6][:-1])
    api_data_list.append(usdt_api)
    usdt_e_api = float(data_list[data_list.index("USDT.e") + 5][:-1])
    api_data_list.append(usdt_e_api)
    dai_api = float(data_list[data_list.index("DAI") + 5][:-1])
    api_data_list.append(dai_api)
    max_api = 0
    for da in api_data_list:
        if da > max_api:
            max_api = da
    burrow_data["yield_apy_up_to"] = str(max_api) + "%"
    # print(burrow_data)
    query_url = "https://api.data-service.burrow.finance/add_burrow_data/" + json.dumps(burrow_data)
    requests.packages.urllib3.disable_warnings()
    ret = requests.post(url=query_url, verify=False)
    ret_data = ret.text
    return ret_data


if __name__ == "__main__":

    if len(sys.argv) == 2:
        start_time = int(time.time())
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            print("Staring handel_burrow_data ...")
            data = handel_burrow_data()
            print(data)
            end_time = int(time.time())
            print("handel_burrow_data consuming time:{}", start_time - end_time)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)

    # data = handel_burrow_data()
    # print(data)

    # with open(r'C:\Users\86176\Desktop\aa\burrow_data.txt', 'r') as f:
    #     content = f.read()
    # # print(content)
    # burrow_data = {"total_supplied": "", "yield_apy": "", "yield_apy_up_to": ""}
    # data_list = []
    # api_data_list = []
    # soup = BeautifulSoup(content, 'html.parser')
    # elements = soup.find_all('span')
    # for element in elements:
    #     data_list.append(element.get_text())
    # burrow_data["total_supplied"] = data_list[data_list.index("Total Supplied") + 1]
    # burrow_data["yield_apy"] = data_list[data_list.index("NEAR") + 5]
    # usdc_api = float(data_list[data_list.index("USDC") + 6][:-1])
    # api_data_list.append(usdc_api)
    # usdc_e_api = float(data_list[data_list.index("USDC.e") + 5][:-1])
    # api_data_list.append(usdc_e_api)
    # usdt_api = float(data_list[data_list.index("USDt") + 6][:-1])
    # api_data_list.append(usdt_api)
    # usdt_e_api = float(data_list[data_list.index("USDT.e") + 5][:-1])
    # api_data_list.append(usdt_e_api)
    # dai_api = float(data_list[data_list.index("DAI") + 5][:-1])
    # api_data_list.append(dai_api)
    # max_api = 0
    # for da in api_data_list:
    #     if da > max_api:
    #         max_api = da
    # burrow_data["yield_apy_up_to"] = str(max_api) + "%"
    # print(burrow_data)
    # query_url = "https://api.data-service.burrow.finance/add_burrow_data/" + json.dumps(burrow_data)
    # requests.packages.urllib3.disable_warnings()
    # ret = requests.post(url=query_url, verify=False)
    # ret_data = ret.text
    # print(ret_data)
