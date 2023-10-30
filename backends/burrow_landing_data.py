import sys

sys.path.append('../')
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
from webdriver_manager.chrome import ChromeDriverManager


def get_burrow_data():
    # ser = Service()
    # ser.path = r"/usr/bin/chromedriver"
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
    test_webdriver = webdriver.Chrome(ChromeDriverManager().install())
    # 最大化
    test_webdriver.maximize_window()
    # 打开网页
    test_webdriver.get("https://app.burrow.fun/")
    # time.sleep(100)
    # 获取网页内容
    element = test_webdriver.page_source
    # 释放,退出
    test_webdriver.quit()

    # driver = webdriver.Chrome()
    # driver.get("https://app.burrow.fun/")
    # element = driver.page_source
    # driver.quit()
    return element


if __name__ == "__main__":

    # if len(sys.argv) == 2:
    #     start_time = int(time.time())
    #     network_id = str(sys.argv[1]).upper()
    #     if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
    #         print("Staring update_dcl_pools ...")
    #         update_dcl_pools(network_id)
    #         end_time = int(time.time())
    #         print("update_dcl_pools consuming time:{}", start_time - end_time)
    #     else:
    #         print("Error, network_id should be MAINNET, TESTNET or DEVNET")
    #         exit(1)
    # else:
    #     print("Error, must put NETWORK_ID as arg")
    #     exit(1)
    data = get_burrow_data()
    print(data)
