#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'Marco'

# load private info

try:
    from db_info import DATA_DB_DSN, DATA_DB_UID, DATA_DB_PWD, DATA_DB_HOST, DATA_DB_PORT
except ImportError:
    DATA_DB_DSN = "ref"
    DATA_DB_UID = "root"
    DATA_DB_PWD = "root"
    DATA_DB_HOST = "127.0.0.1"
    DATA_DB_PORT = "3306"

class Cfg:
    NETWORK_ID = "MAINNET"
    REFSUBGRAPH_URL = "https://api.thegraph.com/subgraphs/name/coolsnake/refsubgraph"

    NETWORK = {
        "MAINNET": {
            "FARMING_CONTRACT": "v2.ref-farming.near",
            "REF_CONTRACT": "v2.ref-finance.near",
            "XREF_CONTRACT": "xtoken.ref-finance.near",
            "BOOSTFARM_CONTRACT": "boostfarm.ref-finance.near",
            "USN_CONTRACT": "usn",
            "REDIS_KEY": "FARMS_MAINNET",
            "REDIS_POOL_BY_TOKEN_KEY": "POOLS_BY_TOKEN_MAINNET",
            "REDIS_POOL_KEY": "POOLS_MAINNET",
            "REDIS_TOP_POOL_KEY": "TOP_POOLS_MAINNET",
            "REDIS_TOKEN_PRICE_KEY": "TOKEN_PRICE_MAINNET",
            "REDIS_HISTORY_TOKEN_PRICE_KEY": "HISTORY_TOKEN_PRICE_MAINNET",
            "REDIS_PROPOSAL_ID_HASH_KEY": "PROPOSAL_ID_HASH_MAINNET",
            "REDIS_TOKEN_METADATA_KEY": "TOKEN_METADATA_MAINNET",
            "REDIS_WHITELIST_KEY": "WHITELIST_MAINNET",
            "DATA_DB_DSN": DATA_DB_DSN,
            "DATA_DB_UID": DATA_DB_UID,
            "DATA_DB_PWD": DATA_DB_PWD,
            "DATA_DB_HOST": DATA_DB_HOST,
            "DATA_DB_PORT": DATA_DB_PORT,
        }
    }
    TOKENS = {
        "MAINNET": [
            {"SYMBOL": "near", "NEAR_ID": "wrap.near", "MD_ID": "near", "DECIMAL": 24},
            {"SYMBOL": "nUSDC", "NEAR_ID": "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near", "MD_ID": "usd-coin", "DECIMAL": 6},
            {"SYMBOL": "nUSDT", "NEAR_ID": "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near", "MD_ID": "tether", "DECIMAL": 6},            
            {"SYMBOL": "nDAI", "NEAR_ID": "6b175474e89094c44da98b954eedeac495271d0f.factory.bridge.near", "MD_ID": "dai", "DECIMAL": 18},
            {"SYMBOL": "nWETH", "NEAR_ID": "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.factory.bridge.near", "MD_ID": "weth", "DECIMAL": 18},
            {"SYMBOL": "n1INCH", "NEAR_ID": "111111111117dc0aa78b770fa6a738034120c302.factory.bridge.near", "MD_ID": "1inch", "DECIMAL": 18},
            {"SYMBOL": "nGRT", "NEAR_ID": "c944e90c64b2c07662a292be6244bdf05cda44a7.factory.bridge.near", "MD_ID": "the-graph", "DECIMAL": 18},
            {"SYMBOL": "SKYWARD", "NEAR_ID": "token.skyward.near", "MD_ID": "v2.ref-finance.near|0|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "REF", "NEAR_ID": "token.v2.ref-finance.near", "MD_ID": "v2.ref-finance.near|79|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "BANANA", "NEAR_ID": "berryclub.ek.near", "MD_ID": "v2.ref-finance.near|5|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "nHT", "NEAR_ID": "6f259637dcd74c767781e37bc6133cd6a68aa161.factory.bridge.near", "MD_ID": "huobi-token", "DECIMAL": 18},
            {"SYMBOL": "nGTC", "NEAR_ID": "de30da39c46104798bb5aa3fe8b9e0e1f348163f.factory.bridge.near", "MD_ID": "gitcoin", "DECIMAL": 18},
            {"SYMBOL": "nUNI", "NEAR_ID": "1f9840a85d5af5bf1d1762f925bdaddc4201f984.factory.bridge.near", "MD_ID": "uniswap", "DECIMAL": 18},
            {"SYMBOL": "nWBTC", "NEAR_ID": "2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near", "MD_ID": "wrapped-bitcoin", "DECIMAL": 8},
            {"SYMBOL": "nLINK", "NEAR_ID": "514910771af9ca656af840dff83e8264ecf986ca.factory.bridge.near", "MD_ID": "chainlink", "DECIMAL": 18},
            {"SYMBOL": "PARAS", "NEAR_ID": "token.paras.near", "MD_ID": "v2.ref-finance.near|377|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "STNEAR", "NEAR_ID": "meta-pool.near", "MD_ID": "v2.ref-finance.near|3514|wrap.near", "DECIMAL": 24},
            {"SYMBOL": "marmaj", "NEAR_ID": "marmaj.tkn.near", "MD_ID": "v2.ref-finance.near|11|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "PULSE", "NEAR_ID": "52a047ee205701895ee06a375492490ec9c597ce.factory.bridge.near", "MD_ID": "v2.ref-finance.near|852|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "ETH", "NEAR_ID": "aurora", "MD_ID": "ethereum", "DECIMAL": 18},
            {"SYMBOL": "AURORA", "NEAR_ID": "aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near", "MD_ID": "v2.ref-finance.near|1395|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "DBIO", "NEAR_ID": "dbio.near", "MD_ID": "v2.ref-finance.near|1371|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "OCT", "NEAR_ID": "f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near", "MD_ID": "v2.ref-finance.near|47|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "HAPI", "NEAR_ID": "d9c2d319cd7e6177336b0a9c93c21cb48d84fb54.factory.bridge.near", "MD_ID": "v2.ref-finance.near|250|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "META", "NEAR_ID": "meta-token.near", "MD_ID": "v2.ref-finance.near|1559|wrap.near", "DECIMAL": 24},
            {"SYMBOL": "nUSDO", "NEAR_ID": "v3.oin_finance.near", "MD_ID": "v2.ref-finance.near|2043|wrap.near", "DECIMAL": 8},
            {"SYMBOL": "FLX", "NEAR_ID": "3ea8ea4237344c9931214796d9417af1a1180770.factory.bridge.near", "MD_ID": "v2.ref-finance.near|2330|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "PXT", "NEAR_ID": "pixeltoken.near", "MD_ID": "v2.ref-finance.near|1178|wrap.near", "DECIMAL": 6},
            {"SYMBOL": "MYRIA", "NEAR_ID": "myriadcore.near", "MD_ID": "v2.ref-finance.near|2448|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "CELO", "NEAR_ID": "celo.token.a11bd.near", "MD_ID": "celo", "DECIMAL": 24},
            {"SYMBOL": "cUSD", "NEAR_ID": "cusd.token.a11bd.near", "MD_ID": "celo-dollar", "DECIMAL": 24},
            {"SYMBOL": "ABR", "NEAR_ID": "abr.a11bd.near", "MD_ID": "allbridge", "DECIMAL": 24},
            {"SYMBOL": "SOL", "NEAR_ID": "sol.token.a11bd.near", "MD_ID": "solana", "DECIMAL": 24},
            {"SYMBOL": "UTO", "NEAR_ID": "utopia.secretskelliessociety.near", "MD_ID": "v2.ref-finance.near|2973|wrap.near", "DECIMAL": 8},
            {"SYMBOL": "WOO", "NEAR_ID": "4691937a7508860f876c9c0a2a617e7d9e945d4b.factory.bridge.near", "MD_ID": "woo-network", "DECIMAL": 18},
            {"SYMBOL": "LINEAR", "NEAR_ID": "linear-protocol.near", "MD_ID": "v2.ref-finance.near|3515|wrap.near", "DECIMAL": 24},
            {"SYMBOL": "HBTC", "NEAR_ID": "0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near", "MD_ID": "huobi-btc", "DECIMAL": 18},
            {"SYMBOL": "Cheddar", "NEAR_ID": "token.cheddar.near", "MD_ID": "v2.ref-finance.near|2769|wrap.near", "DECIMAL": 24},
            {"SYMBOL": "PEM", "NEAR_ID": "token.pembrock.near", "MD_ID": "v2.ref-finance.near|3449|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "BRRR", "NEAR_ID": "token.burrow.near", "MD_ID": "v2.ref-finance.near|3474|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "ATO", "NEAR_ID": "atocha-token.near", "MD_ID": "v2.ref-finance.near|3519|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "NearX", "NEAR_ID": "nearx.stader-labs.near", "MD_ID": "nearx.stader-labs.near|NA|wrap.near", "DECIMAL": 24},
            {"SYMBOL": "SD", "NEAR_ID": "30d20208d987713f46dfd34ef128bb16c404d10f.factory.bridge.near", "MD_ID": "stader", "DECIMAL": 18},
            {"SYMBOL": "xREF", "NEAR_ID": "xtoken.ref-finance.near", "MD_ID": "xtoken.ref-finance.near|NA|token.v2.ref-finance.near", "DECIMAL": 18},
            {"SYMBOL": "SWEAT", "NEAR_ID": "token.sweat", "MD_ID": "v2.ref-finance.near|3667|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "NearX", "NEAR_ID": "v2-nearx.stader-labs.near", "MD_ID": "v2-nearx.stader-labs.near|NA|wrap.near", "DECIMAL": 24},
            {"SYMBOL": "SEAT", "NEAR_ID": "token.stlb.near", "MD_ID": "v2.ref-finance.near|3714|wrap.near", "DECIMAL": 5},
            {"SYMBOL": "OIN", "NEAR_ID": "9aeb50f542050172359a0e1a25a9933bc8c01259.factory.bridge.near", "MD_ID": "v2.ref-finance.near|3714|wrap.near", "DECIMAL": 8},
            {"SYMBOL": "1MIL", "NEAR_ID": "a4ef4b0b23c1fc81d3f9ecf93510e64f58a4a016.factory.bridge.near", "MD_ID": "v2.ref-finance.near|3714|wrap.near", "DECIMAL": 18},
            {"SYMBOL": "APYS", "NEAR_ID": "apys.token.a11bd.near", "MD_ID": "solana", "DECIMAL": 24},
            {"SYMBOL": "AVRIT", "NEAR_ID": "avrit.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "$ASAC", "NEAR_ID": "coin.asac.near", "MD_ID": "solana", "DECIMAL": 24},
            {"SYMBOL": "CTRL", "NEAR_ID": "ctrl.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "DEIP", "NEAR_ID": "deip-token.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "DISC", "NEAR_ID": "discovol-token.near", "MD_ID": "solana", "DECIMAL": 14},
            {"SYMBOL": "UMINT", "NEAR_ID": "e99de844ef3ef72806cf006224ef3b813e82662f.factory.bridge.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "ELIXIR", "NEAR_ID": "elixir.l2e.near", "MD_ID": "solana", "DECIMAL": 0},
            {"SYMBOL": "CUCUMBER", "NEAR_ID": "farm.berryclub.ek.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "$SPLASH", "NEAR_ID": "ft.soundsplash.near", "MD_ID": "solana", "DECIMAL": 8},
            {"SYMBOL": "ZML", "NEAR_ID": "ft.zomland.near", "MD_ID": "solana", "DECIMAL": 24},
            {"SYMBOL": "TAO", "NEAR_ID": "fusotao-token.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "GEAR", "NEAR_ID": "gear.enleap.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "GEM", "NEAR_ID": "gems.l2e.near", "MD_ID": "solana", "DECIMAL": 4},
            {"SYMBOL": "GOLD", "NEAR_ID": "gold.l2e.near", "MD_ID": "solana", "DECIMAL": 0},
            {"SYMBOL": "HAK", "NEAR_ID": "hak.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "HBSC", "NEAR_ID": "hbsc.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "HRT", "NEAR_ID": "hrt.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "Indulgency", "NEAR_ID": "indulgency.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "MERITOCRACY", "NEAR_ID": "meritocracy.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "MIKA", "NEAR_ID": "mika.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "ELECTRIK", "NEAR_ID": "ndn.electrik.near", "MD_ID": "solana", "DECIMAL": 2},
            {"SYMBOL": "NEARKAT", "NEAR_ID": "nearkat.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "Net", "NEAR_ID": "net.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "NVISION", "NEAR_ID": "nvision.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "RIMJOB", "NEAR_ID": "rimjob.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "SOCIALMEET", "NEAR_ID": "socialmeet.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "CHICA", "NEAR_ID": "token.bocachica_mars.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "JUMBO", "NEAR_ID": "token.jumbo_exchange.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "SHRM", "NEAR_ID": "token.shrm.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "UNET", "NEAR_ID": "uniqueone-appchain-token.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "USN", "NEAR_ID": "usn", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "POTATO", "NEAR_ID": "v1.dacha-finance.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "YNP", "NEAR_ID": "ynp.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "LGBT", "NEAR_ID": "lgbt.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "Ralfusha", "NEAR_ID": "ralfusha.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "REKT", "NEAR_ID": "rekt.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "TER", "NEAR_ID": "ter.tkn.near", "MD_ID": "solana", "DECIMAL": 10},
            {"SYMBOL": "DUCK", "NEAR_ID": "duck.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "NEXP", "NEAR_ID": "nexp.near", "MD_ID": "solana", "DECIMAL": 4},
            {"SYMBOL": "PADTHAI", "NEAR_ID": "padthai.near", "MD_ID": "solana", "DECIMAL": 8},
            {"SYMBOL": "WHALES", "NEAR_ID": "whales.tkn.near", "MD_ID": "solana", "DECIMAL": 4},
            {"SYMBOL": "NVP", "NEAR_ID": "nvp.tkn.near", "MD_ID": "solana", "DECIMAL": 18},
            {"SYMBOL": "USDt", "NEAR_ID": "usdt.tether-token.near", "MD_ID": "solana", "DECIMAL": 6},
        ],
    }
    MARKET_URL = "api.coingecko.com"


if __name__ == '__main__':
    print(type(Cfg))
    print(type(Cfg.TOKENS))
    print(type(Cfg.NETWORK_ID), Cfg.NETWORK_ID)
    print(Cfg.NETWORK["TESTNET"]["NEAR_RPC_URL"])