import requests
from config import Cfg
import json


def send_message(type, content):
    try:
        args = {
            "content": json.dumps(content),
            "product": content["source"],
            "level": "medium",
            "email": True,
            "telegram": True,
            "slack": True,
            "type": type
        }
        requests.packages.urllib3.disable_warnings()
        ret = requests.post(url=Cfg.MESSAGE_SERVICE_URL, json=args, verify=False, timeout=60)
        print(ret.text)
        print("sendMessage end")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    print("--------------monitor_indexer_api start-------------")
    c = {
        "service": "stats"
    }
    send_message("test", c)
    print("--------------monitor_indexer_api end-------------")

