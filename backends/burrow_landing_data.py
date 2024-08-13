import json
import sys

sys.path.append('../')
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import requests


def handel_burrow_data():
    # 实例化参数方法
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument('lang=zh_CN.UTF-8')
    chrome_options.add_argument('sec-fetch-site=same-origin')
    chrome_options.add_argument('sec-fetch-mode=no-cors')
    chrome_options.add_argument('sec-ch-ua-platform="Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"')
    chrome_options.add_argument('sec-ch-ua="Windows"')
    chrome_options.add_argument('sec-ch-ua-mobile=?0')
    chrome_options.add_argument('referer=https://app.burrow.fun/')
    chrome_options.add_argument('accept-language=zh-CN,zh;q=0.9')
    chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"')
    # 实例化 Chrome Service 对象
    service = ChromeService(executable_path=ChromeDriverManager().install())
    # 实例化 Chrome 浏览器对象，导入设置项
    wd = webdriver.Chrome(service=service, options=chrome_options)
    try:
        # 最大化
        wd.maximize_window()
        # 打开网页
        wd.get("https://app.burrow.finance/")
        time.sleep(5)
        # 获取网页内容
        content = wd.page_source
    finally:
        # 释放, 退出
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
    # 找到第一次出现'USDC'的索引
    first_usdc_index = data_list.index('USDC')
    # 找到第二次出现'USDC'的索引
    # second_usdc_index = data_list.index('USDC', first_usdc_index + 1)
    # 找到紧随第二次'USDC'出现的'Supply APY'的下一个元素
    # next_element_index = data_list.index('Supply APY', second_usdc_index) + 1
    # usdc_api = float(data_list[next_element_index][:-1])
    # api_data_list.append(usdc_api)
    usdc_e_api = float(data_list[data_list.index("USDC.e") + 5][:4])
    api_data_list.append(usdc_e_api)
    # usdt_api = float(data_list[data_list.index("USDt") + 6][:-1])
    # api_data_list.append(usdt_api)
    usdt_e_api = float(data_list[data_list.index("USDT.e") + 5][:4])
    api_data_list.append(usdt_e_api)
    dai_api = float(data_list[data_list.index("DAI") + 5][:4])
    api_data_list.append(dai_api)
    max_api = 0
    for da in api_data_list:
        if da > max_api:
            max_api = da
    burrow_data["yield_apy_up_to"] = str(max_api) + "%"
    query_url = "https://api.data-service.burrow.finance/add_burrow_data/" + json.dumps(burrow_data)
    requests.packages.urllib3.disable_warnings()
    ret = requests.post(url=query_url, verify=False)
    ret_data = ret.text
    return ret_data


if __name__ == "__main__":
    print("Staring handel_burrow_data ...")
    start_time = int(time.time())
    data = handel_burrow_data()
    print(data)
    end_time = int(time.time())
    print("handel_burrow_data consuming time:{}", start_time - end_time)

    # data = handel_burrow_data()
    # print(data)

    # with open(r'C:\Users\86176\Desktop\aa\burrow_data.txt', 'r') as f:
    #     content = f.read()
    # print(content)
    #
    # burrow_data = {"total_supplied": "", "yield_apy": "", "yield_apy_up_to": ""}
    # data_list = ['Docs', 'Bridge', 'Rainbow', 'Ethereum | Aurora', 'Aggregate Bridge', 'Arbitrum | Ethereum | Base Optimism | Scroll', '', 'Show Dust', '', '', 'USDC', 'USDT', 'FRAX', 'USDC', 'USDT', 'FRAX', 'Total Supplied', '$181.75M', 'Total Borrowed', '$49.32M', 'Available Liquidity', '$132.44M', 'Daily Rewards', '$9,403.05', 'All Markets', 'Sort by', 'Available Liquidity', 'Total Supplied', 'Supply APY', 'Total Borrowed', 'Borrow APY', 'Price', 'FRAX', 'Total Supplied', '5.26M', '$5.24M', 'Supply APY', '10.85% ~ 15.60%', '10.85% ~ 15.60%', 'Total Borrowed', '2.60M', '$2.59M', 'Borrow APY', '3.67%', 'Available Liquidity', '2.66M', '$2.65M', 'Price', '$1.00', 'USDCNative', 'Native', 'Total Supplied', '23.48M', '$23.48M', 'Supply APY', '9.00% ~ 11.74%', '9.00% ~ 11.74%', 'Total Borrowed', '18.42M', '$18.41M', 'Borrow APY', '5.88%', 'Available Liquidity', '5.06M', '$5.06M', 'Price', '$1.00', 'USDtNative', 'Native', 'Total Supplied', '26.96M', '$26.97M', 'Supply APY', '10.45% ~ 14.01%', '10.45% ~ 14.01%', 'Total Borrowed', '20.65M', '$20.65M', 'Borrow APY', '5.74%', 'Available Liquidity', '6.31M', '$6.31M', 'Price', '$1.00', 'LINEAR', 'Total Supplied', '15.51M', '$76.89M', 'Supply APY', '<0.01% ', '<0.01% ', 'Total Borrowed', '199.27K', '$988.07K', 'Borrow APY', '0.18%', 'Available Liquidity', '15.29M', '$75.83M', 'Price', '$4.96', 'STNEAR', 'Total Supplied', '5.88M', '$31.02M', 'Supply APY', '<0.01% ', '<0.01% ', 'Total Borrowed', '15.78K', '$83.28K', 'Borrow APY', '0.04%', 'Available Liquidity', '5.86M', '$30.90M', 'Price', '$5.28', 'NEAR', 'Total Supplied', '2.17M', '$8.70M', 'Supply APY', '0.44% ', '0.44% ', 'Total Borrowed', '439.07K', '$1.76M', 'Borrow APY', '2.90%', 'Available Liquidity', '1.73M', '$6.94M', 'Price', '$4.00', 'DAI', 'Total Supplied', '2.55M', '$2.55M', 'Supply APY', '3.19% ', '3.19% ', 'Total Borrowed', '1.80M', '$1.80M', 'Borrow APY', '5.27%', 'Available Liquidity', '749.73K', '$749.50K', 'Price', '$1.00', 'USDC.e', 'Total Supplied', '1.91M', '$1.91M', 'Supply APY', '3.67% ', '3.67% ', 'Total Borrowed', '1.32M', '$1.32M', 'Borrow APY', '5.15%', 'Available Liquidity', '592.29K', '$592.17K', 'Price', '$1.00', 'USDT.e', 'Total Supplied', '1.72M', '$1.72M', 'Supply APY', '2.93% ', '2.93% ', 'Total Borrowed', '1.04M', '$1.04M', 'Borrow APY', '4.51%', 'Available Liquidity', '677.82K', '$677.95K', 'Price', '$1.00', 'ETH', 'Total Supplied', '631.30', '$1.68M', 'Supply APY', '0.25% ', '0.25% ', 'Total Borrowed', '146.45', '$388.61K', 'Borrow APY', '1.42%', 'Available Liquidity', '484.36', '$1.29M', 'Price', '$2,653.43', 'WBTC', 'Total Supplied', '16.35', '$968.67K', 'Supply APY', '0.40% ', '0.40% ', 'Total Borrowed', '4.75', '$281.59K', 'Borrow APY', '1.79%', 'Available Liquidity', '11.59', '$686.39K', 'Price', '$59,236', 'BRRR', 'Total Supplied', '114.18M', '$607.71K', 'Supply APY', '0% ', '0% ', 'Total Borrowed', '-', 'Borrow APY', '-', 'Available Liquidity', '-', 'Price', '<$0.01', 'AURORA', 'Total Supplied', '261.70K', '$35.13K', 'Supply APY', '0.05% ', '0.05% ', 'Total Borrowed', '10.17K', '$1.37K', 'Borrow APY', '1.65%', 'Available Liquidity', '251.28K', '$33.73K', 'Price', '$0.13', 'WOO', 'Total Supplied', '23.09K', '$3.42K', 'Supply APY', '7.47% ', '7.47% ', 'Total Borrowed', '14.12K', '$2.09K', 'Borrow APY', '14.95%', 'Available Liquidity', '8.96K', '$1.33K', 'Price', '$0.15', 'NearX', 'Total Supplied', '596.47', '$2.80K', 'Supply APY', '<0.01% ', '<0.01% ', 'Total Borrowed', '-', 'Borrow APY', '-', 'Available Liquidity', '-', 'Price', '$4.69', 'sFRAX', 'Total Supplied', '5.00', '$5.34', 'Supply APY', '0% ', '0% ', 'Total Borrowed', '-', 'Borrow APY', '-', 'Available Liquidity', '-', 'Price', '$1.07', 'USDC', 'USDT', 'FRAX', 'USDC', 'USDT', 'FRAX', '']
    # api_data_list = []
    # # soup = BeautifulSoup(content, 'html.parser')
    # # elements = soup.find_all('span')
    # # for element in elements:
    # #     data_list.append(element.get_text())
    # burrow_data["total_supplied"] = data_list[data_list.index("Total Supplied") + 1]
    # burrow_data["yield_apy"] = data_list[data_list.index("NEAR") + 5]
    # # 找到第一次出现'USDC'的索引
    # first_usdc_index = data_list.index('USDC')
    # # 找到第二次出现'USDC'的索引
    # # second_usdc_index = data_list.index('USDC', first_usdc_index + 1)
    # # 找到紧随第二次'USDC'出现的'Supply APY'的下一个元素
    # # next_element_index = data_list.index('Supply APY', second_usdc_index) + 1
    # # usdc_api = float(data_list[next_element_index][:-1])
    # # api_data_list.append(usdc_api)
    # usdc_e_api = float(data_list[data_list.index("USDC.e") + 5][:4])
    # api_data_list.append(usdc_e_api)
    # # usdt_api = float(data_list[data_list.index("USDt") + 6][:-1])
    # # api_data_list.append(usdt_api)
    # usdt_e_api = float(data_list[data_list.index("USDT.e") + 5][:4])
    # api_data_list.append(usdt_e_api)
    # dai_api = float(data_list[data_list.index("DAI") + 5][:4])
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
