import requests
import base64
import json


class JsonProviderError(Exception):
    pass


class JsonProvider(object):

    def __init__(self, rpc_addr):
        if isinstance(rpc_addr, tuple):
            self._rpc_addr = "http://%s:%s" % rpc_addr
        else:
            self._rpc_addr = rpc_addr

    def rpc_addr(self):
        return self._rpc_addr

    def json_rpc(self, method, params, timeout=2):
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        r = requests.post(self.rpc_addr(), json=j, timeout=timeout)
        r.raise_for_status()
        content = json.loads(r.content)
        if "error" in content:
            raise JsonProviderError(content["error"])
        return content["result"]

    def send_tx(self, signed_tx):
        return self.json_rpc('broadcast_tx_async', [base64.b64encode(signed_tx).decode('utf8')])

    def send_tx_and_wait(self, signed_tx, timeout):
        return self.json_rpc('broadcast_tx_commit', [base64.b64encode(signed_tx).decode('utf8')], timeout=timeout)

    def get_status(self):
        r = requests.get("%s/status" % self.rpc_addr(), timeout=2)
        r.raise_for_status()
        return json.loads(r.content)

    def get_validators(self):
        return self.json_rpc('validators', [None])

    def query(self, query_object):
        return self.json_rpc('query', query_object)

    def get_account(self, account_id, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "view_account", "account_id": account_id, "finality": finality})

    def get_access_key_list(self, account_id, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "view_access_key_list", "account_id": account_id, "finality": finality})

    def get_access_key(self, account_id, public_key, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "view_access_key", "account_id": account_id,
                                       "public_key": public_key, "finality": finality})

    def view_call(self, account_id, method_name, args, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "call_function", "account_id": account_id,
                                       "method_name": method_name, "args_base64": base64.b64encode(args).decode('utf8'), "finality": finality})

    def get_block(self, block_id):
        return self.json_rpc('block', [block_id])

    def get_chunk(self, chunk_id):
        return self.json_rpc('chunk', [chunk_id])

    def get_tx(self, tx_hash, tx_recipient_id):
        return self.json_rpc('tx', [tx_hash, tx_recipient_id])

    def get_changes_in_block(self, changes_in_block_request):
        return self.json_rpc('EXPERIMENTAL_changes_in_block', changes_in_block_request)
    
    def get_tx_status(self, tx_hash, sender_id):
        return self.json_rpc('EXPERIMENTAL_tx_status', [tx_hash, sender_id])


if __name__ == "__main__":
    conn = JsonProvider("https://rpc.testnet.near.org")
    status = conn.get_status()
    if "version" in status:
        print(status["version"])
    if "sync_info" in status:
        print(status['sync_info'])
    # print(status)
    # ret = conn.view_call("ref-farming.testnet", "get_number_of_farms", b"")
    # # print(ret["result"])
    # a = "".join([chr(x) for x in ret["result"]])
    # print(a)
    # print()
    # ret = conn.view_call("ref-farming.testnet", "list_farms", b'{"from_index": 0, "limit": 100}')
    # # print(ret["result"])
    # b = "".join([chr(x) for x in ret["result"]])
    # # print(b)
    # c = json.loads(b)
    # for item in c:
    #     print(item)
    # print("In tx GfcyYBJeQDUMbJrCkdymx6zPHcoZCEwYCPLNpTReACpP")
    # print("Yams.near calls remove_liquidity @1: 11529056751499847000000")

    # conn = JsonProvider("https://rpc.mainnet.near.org")
    # # ret = conn.view_call("6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near", "ft_metadata", b'')
    # ret = conn.view_call("ref-finance.near", "mft_balance_of", b'{"token_id": "1", "account_id": "yams.near"}')
    # b = "".join([chr(x) for x in ret["result"]])
    # obj = json.loads(b)
    # print("mft_balance_of yams.near on pool  1:", obj)
    # ret = conn.view_call("ref-finance.near", "get_pool_shares", b'{"pool_id": 1, "account_id": "yams.near"}')
    # b = "".join([chr(x) for x in ret["result"]])
    # obj = json.loads(b)
    # print("get_pool_shares yams.near on pool 1:", obj)
    # for token_id in obj:
    #     import time
    #     time.sleep(0.1)
    #     ret = conn.view_call(token_id, "ft_metadata", b'')
    #     json_str = "".join([chr(x) for x in ret["result"]])
    #     token_metadata = json.loads(json_str)
    #     print("%s: %s, %s" % (token_id, token_metadata["symbol"], token_metadata["decimals"]))
    # print("Total %s whitelisted tokens" % len(obj))

    # conn = JsonProvider("https://rpc.testnet.near.org")
    # ret = conn.view_call("ref-farming.testnet", "get_unclaimed_reward", b'{"account_id": "pika8.testnet", "farm_id": "ref-finance."}')
    # b = "".join([chr(x) for x in ret["result"]])
    # obj = json.loads(b)

    # conn = JsonProvider("https://rpc.testnet.near.org")
    # ret = conn.view_call("ref-finance.testnet", "get_return", b'{"pool_id": 24, "token_in": "rft.tokenfactory.testnet", "amount_in": "100000000", "token_out": "wrap.testnet"}')
    # b = "".join([chr(x) for x in ret["result"]])
    # obj = json.loads(b)
    # print(" pool  24: %s in type %s" % (obj[:-16], type(obj)))
    # price = int(obj[:-16]) / 100000000
    # print(price)
    # 0.996_505_985_279_683_515_693_096
