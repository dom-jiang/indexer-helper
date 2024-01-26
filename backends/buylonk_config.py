import os


class GlobalConfig:
    def __init__(self):
        near_env = "mainnet"
        if near_env:
            if near_env not in ["mainnet", "testnet"]:
                raise Exception("Invalid NEAR_ENV!")
            self._near_env = near_env
        else:
            raise Exception("Missing NEAR_ENV!")

        if self._near_env == "mainnet":
            self._rpc_url = "https://rpc.mainnet.near.org" if not os.getenv('NEAR_RPC_URL') else os.getenv('NEAR_RPC_URL')
            self._private_key = "" if not os.getenv('PRIVATE_KEY') else os.getenv('PRIVATE_KEY')
            self._deposit_yocto = 1
            self._near_contract = "wrap.near"
            self._signer_account_id = "dom1.near" if not os.getenv('SIGNER_ACCOUNT_ID') else os.getenv('SIGNER_ACCOUNT_ID')
            self._buylonk_contract = "lonkbuy.near"
            self._buylonk_token_in_contract = "wrap.near"
            self._buylonk_token_out_contract = "token.lonkingnearbackto2024.near"
            self._buylonk_pool_one = "4314"
        else:
            raise Exception("Invalid NEAR_ENV!")

    @property
    def near_env(self):
        return self._near_env

    @property
    def rpc_url(self):
        return self._rpc_url

    @property
    def private_key(self):
        return self._private_key

    @property
    def deposit_yocto(self):
        return self._deposit_yocto

    @property
    def near_contract(self):
        return self._near_contract

    @property
    def signer_account_id(self):
        return self._signer_account_id

    @property
    def buylonk_contract(self):
        return self._buylonk_contract

    @property
    def buylonk_token_in_contract(self):
        return self._buylonk_token_in_contract

    @property
    def buylonk_token_out_contract(self):
        return self._buylonk_token_out_contract

    @property
    def buylonk_pool_one(self):
        return self._buylonk_pool_one

