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
    time.sleep(5)
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
    # 找到第一次出现'USDC'的索引
    first_usdc_index = data_list.index('USDC')
    # 找到第二次出现'USDC'的索引
    second_usdc_index = data_list.index('USDC', first_usdc_index + 1)
    # 找到紧随第二次'USDC'出现的'Supply APY'的下一个元素
    next_element_index = data_list.index('Supply APY', second_usdc_index) + 1
    usdc_api = float(data_list[next_element_index][:-1])
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
    # print(content)
    #
    # burrow_data = {"total_supplied": "", "yield_apy": "", "yield_apy_up_to": ""}
    # data_list = ['Help', 'Bridge', 'Rainbow', 'Ethereum | Aurora', 'Electron Labs', 'Ethereum', '', 'Show Dust', '', '', 'USDC', 'USDT', 'FRAX', 'Total Supplied', '$220.72M', 'Total Borrowed', '$18.77M', 'Available Liquidity', '$201.94M', 'Daily Rewards', '$12,484', 'All Markets', 'Sort by', 'Available Liquidity', 'Total Supplied', 'Supply APY', 'Total Borrowed', 'Borrow APY', 'Price', 'USDt', 'Native', 'Total Supplied', '6.04M', '$6.04M', 'Supply APY', '44.09%', '44.09%', 'Total Borrowed', '4.11M', '$4.11M', 'Borrow APY', '5.08%', 'Available Liquidity', '1.93M', '$1.93M', 'Price', '$1.0006', 'USDC', 'Native', 'Total Supplied', '7.33M', '$7.33M', 'Supply APY', '44.52%', '44.52%', 'Total Borrowed', '5.38M', '$5.38M', 'Borrow APY', '5.49%', 'Available Liquidity', '1.95M', '$1.95M', 'Price', '$1.0003', 'FRAX', 'Total Supplied', '503.74K', '$502.43K', 'Supply APY', '41.37%', '41.37%', 'Total Borrowed', '-', 'Borrow APY', '-', 'Available Liquidity', '-', 'Price', '$0.9974', 'BRRR', 'Total Supplied', '184.95M', '$0', 'Supply APY', '0%', '0%', 'Total Borrowed', '-', 'Borrow APY', '-', 'Available Liquidity', '-', 'Price', '$0', 'LINEAR', 'Total Supplied', '13.46M', '$131.68M', 'Supply APY', '<0.01%', '<0.01%', 'Total Borrowed', '2.42K', '$23.73K', 'Borrow APY', '<0.01%', 'Available Liquidity', '13.44M', '$131.52M', 'Price', '$9.7853', 'STNEAR', 'Total Supplied', '5.63M', '$58.63M', 'Supply APY', '<0.01%', '<0.01%', 'Total Borrowed', '13.26K', '$138.05K', 'Borrow APY', '0.03%', 'Available Liquidity', '5.61M', '$58.43M', 'Price', '$10.415', 'USDT.e', 'Total Supplied', '2.43M', '$2.43M', 'Supply APY', '4.36%', '4.36%', 'Total Borrowed', '1.91M', '$1.91M', 'Borrow APY', '5.89%', 'Available Liquidity', '519.55K', '$519.86K', 'Price', '$1.0006', 'USDC.e', 'Total Supplied', '2.29M', '$2.29M', 'Supply APY', '7.67%', '7.67%', 'Total Borrowed', '1.88M', '$1.88M', 'Borrow APY', '9.77%', 'Available Liquidity', '411.46K', '$411.58K', 'Price', '$1.0003', 'DAI', 'Total Supplied', '1.44M', '$1.44M', 'Supply APY', '13.74%', '13.74%', 'Total Borrowed', '1.24M', '$1.24M', 'Borrow APY', '16.87%', 'Available Liquidity', '207.49K', '$207.41K', 'Price', '$0.9996', 'NEAR', 'Total Supplied', '795.15K', '$6.50M', 'Supply APY', '3.23%', '3.23%', 'Total Borrowed', '428.64K', '$3.50M', 'Borrow APY', '7.94%', 'Available Liquidity', '366.14K', '$2.99M', 'Price', '$8.1701', 'AURORA', 'Total Supplied', '264.82K', '$120.93K', 'Supply APY', '0.22%', '0.22%', 'Total Borrowed', '21.52K', '$9.83K', 'Borrow APY', '3.48%', 'Available Liquidity', '243.06K', '$110.99K', 'Price', '$0.45665', 'WOO', 'Total Supplied', '20.97K', '$10.66K', 'Supply APY', '7.76%', '7.76%', 'Total Borrowed', '13.09K', '$6.65K', 'Borrow APY', '15.29%', 'Available Liquidity', '7.87K', '$4.00K', 'Price', '$0.508255', 'NearX', 'Total Supplied', '4.61K', '$44.16K', 'Supply APY', '<0.01%', '<0.01%', 'Total Borrowed', '-', 'Borrow APY', '-', 'Available Liquidity', '-', 'Price', '$9.5787', 'ETH', 'Total Supplied', '157.42', '$589.93K', 'Supply APY', '0.92%', '0.92%', 'Total Borrowed', '68.06', '$255.06K', 'Borrow APY', '2.67%', 'Available Liquidity', '89.27', '$334.53K', 'Price', '$3747.45', 'WBTC', 'Total Supplied', '12.51', '$857.63K', 'Supply APY', '0.70%', '0.70%', 'Total Borrowed', '4.75', '$325.76K', 'Borrow APY', '2.34%', 'Available Liquidity', '7.75', '$531.34K', 'Price', '$68566.09', 'sFRAX', 'Total Supplied', '5.00', '$5.10', 'Supply APY', '0%', '0%', 'Total Borrowed', '-', 'Borrow APY', '-', 'Available Liquidity', '-', 'Price', '$1.0191', 'USDC', 'USDT', 'FRAX']
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
    # second_usdc_index = data_list.index('USDC', first_usdc_index + 1)
    # # 找到紧随第二次'USDC'出现的'Supply APY'的下一个元素
    # next_element_index = data_list.index('Supply APY', second_usdc_index) + 1
    # usdc_api = float(data_list[next_element_index][:-1])
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
