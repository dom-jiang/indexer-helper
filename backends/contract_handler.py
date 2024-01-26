import json


class RpcHandler:
    def __init__(self, signer, contract_id):
        self._signer = signer
        self._contract_id = contract_id

    def do_buyback(self, actions):
        msg = {
            "actions": actions
        }
        return self._signer.function_call(
            self._contract_id,
            "do_buyback",
            {
                "swap_msg": json.dumps(msg)
            }, gas=300000000000000
        )


