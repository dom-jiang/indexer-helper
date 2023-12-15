import os


class GlobalConfig:
    def __init__(self):
        near_env = os.getenv('NEAR_ENV')
        if near_env:
            if near_env not in ["mainnet", "testnet"]:
                raise Exception("Invalid NEAR_ENV!")
            self._near_env = near_env
        else:
            raise Exception("Missing NEAR_ENV!")

        if self._near_env == "mainnet":
            self._rpc_url = "https://rpc.mainnet.near.org" if not os.getenv('NEAR_RPC_URL') else os.getenv('NEAR_RPC_URL')
            self._burrow_contract = "contract.main.burrow.near"
            self._private_key = "" if not os.getenv('PRIVATE_KEY') else os.getenv('PRIVATE_KEY')
            self._deposit_yocto = 1
            self._priceoracle_contract = "priceoracle.near"
            self._near_contract = "wrap.near"
            self._signer_account_id = "juaner.near" if not os.getenv('SIGNER_ACCOUNT_ID') else os.getenv('SIGNER_ACCOUNT_ID')
            self._buyback_contract = "buyback.juaner.near"
            self._buyback_token_in_contract = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near"
            self._buyback_token_out_contract = "token.burrow.near"
            self._buyback_pool_one = "3"
            self._buyback_pool_two = "3474"
        elif self._near_env == "testnet":
            self._rpc_url = "https://rpc.testnet.near.org" if not os.getenv('NEAR_RPC_URL') else os.getenv('NEAR_RPC_URL')
            self._burrow_contract = "contract.1689937928.burrow.testnet"
            self._private_key = "" if not os.getenv('PRIVATE_KEY_DEV') else os.getenv('PRIVATE_KEY_DEV')
            self._deposit_yocto = 1
            self._priceoracle_contract = "priceoracle.testnet"
            self._near_contract = "wrap.testnet"
            self._signer_account_id = "juaner.testnet" if not os.getenv('SIGNER_ACCOUNT_ID_DEV') else os.getenv('SIGNER_ACCOUNT_ID_DEV')
            self._buyback_contract = "dev-1702289480516-51259361492553"
            self._buyback_token_in_contract = "usdt.fakes.testnet"
            self._buyback_token_out_contract = "token.1689937928.burrow.testnet"
            self._buyback_pool_one = "465"
            self._buyback_pool_two = "714"
        else:
            raise Exception("Invalid NEAR_ENV!")

    @property
    def near_env(self):
        return self._near_env

    @property
    def rpc_url(self):
        return self._rpc_url

    @property
    def burrow_contract(self):
        return self._burrow_contract

    @property
    def private_key(self):
        return self._private_key

    @property
    def deposit_yocto(self):
        return self._deposit_yocto

    @property
    def priceoracle_contract(self):
        return self._priceoracle_contract

    @property
    def near_contract(self):
        return self._near_contract

    @property
    def signer_account_id(self):
        return self._signer_account_id

    @property
    def buyback_contract(self):
        return self._buyback_contract

    @property
    def buyback_token_in_contract(self):
        return self._buyback_token_in_contract

    @property
    def buyback_token_out_contract(self):
        return self._buyback_token_out_contract

    @property
    def buyback_pool_one(self):
        return self._buyback_pool_one

    @property
    def buyback_pool_two(self):
        return self._buyback_pool_two

