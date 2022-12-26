import sys
sys.path.append('../')
import pymysql
from loguru import logger
from near_multinode_rpc_provider import MultiNodeJsonProviderError, MultiNodeJsonProvider
import json
import time
from hash2random import random


def get_db_connect():
    conn = pymysql.connect(
        host="173.255.213.66",
        port=3306,
        user="ref",
        passwd="Hd2n7TKFej@C",
        db="ref_dcl_mainnet")
    return conn


def get_account_list(network_id):

    db_conn = get_db_connect()
    sql = "select owner_id,min(`timestamp`) as operation_time from t_liquidity_added group by owner_id order by " \
          "operation_time"
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return verify_account_assets(network_id, rows)
    except Exception as e:
        logger.info(e)
    finally:
        cursor.close()


def verify_account_assets(network_id, rows):
    try:
        draw_account_list = []
        conn = MultiNodeJsonProvider(network_id)
        for row in rows:
            account_id = row["owner_id"]
            ret = conn.view_call("dcl.ref-labs.near", "list_liquidities",
                                 ('{"account_id": "%s"}' % account_id).encode(encoding='utf-8'))
            b = "".join([chr(x) for x in ret["result"]])
            account_assets = json.loads(b)
            if len(account_assets) > 0:
                logger.info("1:{}", account_id)
                draw_account_list.append(row["owner_id"])
            else:
                logger.info("2:{}", account_id)
        return draw_account_list
    except MultiNodeJsonProviderError as e:
        logger.info("RPC Error: ", e)
    except Exception as e:
        logger.info("Error: ", e)


def draw(draw_account_list, random_seqs):
    award_account_list = []
    if len(random_seqs) >= 10:
        for i in range(0, 10):
            award_account = draw_account_list[random_seqs[i] - 1]
            award_account_list.append(award_account)
    # not_award_account_list = list(set(draw_account_list).difference(set(award_account_list)))
    for account in award_account_list:
        draw_account_list.remove(account)
    logger.info("award_account_list:{}", json.dumps(award_account_list))
    logger.info("not_award_account_list:{}", json.dumps(draw_account_list))


logger.add("christmas_activities_2022.log")
if __name__ == '__main__':
    logger.info("#########MAINNET###########")
    # if len(sys.argv) == 2:
    #     network_id = str(sys.argv[1]).upper()
    #     if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
    #         start_time = int(time.time())
    #         draw_accounts = get_account_list(network_id)
    #         logger.info("draw_accounts:{}", json.dumps(draw_accounts))
    #         logger.info("draw_account size:{}", len(draw_accounts))
    #         random_seq_list = random(len(draw_accounts))
    #         logger.info("random_seq_list:{}", json.dumps(random_seq_list))
    #         draw(draw_accounts, random_seq_list)
    #         end_time = int(time.time())
    #         logger.info("get_account_list consuming time:{}", end_time - start_time)
    #     else:
    #         print("Error, network_id should be MAINNET, TESTNET or DEVNET")
    #         exit(1)
    # else:
    #     print("Error, must put NETWORK_ID as arg")
    #     exit(1)
    start_time = int(time.time())
    draw_accounts = ["juaner.near", "tony_stark.near",
                     "f0e4509557e25c4fa99591e71f24365778d72b80bcf71834279a72d71df94be6",
                     "ae03d71382e8621650adfb5706ca430676d9756893b08c1efeae37c92024ef1a", "amyliang1.near",
                     "duducat.near", "cudam321.near", "juaner1218.near", "hh00.near", "flyflyfly.near",
                     "khanhtrandt.near", "bfbaf8f5f5b4aa4c66808a977a97cacba47c66e20be83379188dcf727637fb5d",
                     "tunapress.near", "jingjinhuang2.near", "jingjinhuang.near", "jingjinhuang1.near", "verina.near",
                     "truths.near", "86d6c4b72c95f1089880369ecb26e0a8550a97f7864f6909210a10774c866cf9",
                     "mastrophot.near", "bigwayhnt.near", "boyqag.near", "manolitofinance69.near", "haloshi.near",
                     "test_near.near", "truongthanhtung.near",
                     "31ad8a4fa1085cfb1605a189037762a496fb588fb3fce4ffa536adf09763ce90", "alterrush.near",
                     "a4cf63bc7673642a6fd361f40d82197d6960003432b8ab82687f61a3c0c93306", "block_crypto13.near",
                     "moneting.near", "e1ab3190b033cbadd5b100a52fb0272e9dd37c06dff1439f7c2a1e0e45d0d5b7",
                     "billiondollars.near", "siexp.near",
                     "1a81f82d86d51f985abd515e46d619030a97fbe24cadda168d7418296687a409", "haismse54.near",
                     "izumilabs.near", "jiang_wei.near", "hoathienphong.near",
                     "48c6873f1ba0dd07c560fb4ac05cf2fd2afd568c650666d5eb7955a2b7a584fb", "maomartin.near", "gpbec.near",
                     "rosario.near", "kalibaenterprises.near",
                     "884e0aa90212fb91dc0b718987c5c5fc140cf5694316645f6716d5aaa2ef0a9a",
                     "22122faf7f5b2b7a54b0f45284c6db1e5c040785019d7980478868a1d0ffa38c", "makonak.near", "mp55hk.near",
                     "e0213c4aa5c2cbe1bc5dc3c576fba56331da0e177c70823b44e70ea94cacaa93",
                     "574ed39f0191daf7af799e5d0ff984564a898dfa37a8743f7b824a9b1a03cd9d", "ben777.near", "herolis.near",
                     "c4913fa8790eb8b206ac33d79ab0ffa28b1a31cec8fb4502f2bc77540052bb73", "tverzh.near", "aasun2.near",
                     "trandoan1.near", "white116rus.near", "chuyenbannon.near", "cryptomann.near", "lexisko.near",
                     "middlender.near", "rolers.near", "profit.near",
                     "1cc233b7c39b988d65e4b63e709d0dac6092f258d4609ce019c463905d449f1d", "jaafar-lightency.near",
                     "unmarshal.near", "htanhzrother.near", "imiroslav.near", "minicooper.near", "0rz.near",
                     "lioha.near", "luluca_l.near", "d65153bea9ac9061fa086af5c1e8699900c0e7de9d4bca91dc882b2c32964b84",
                     "puchen.near", "1ce2567f7f49cb34cea72179223ec7fc4ba91077da3e01364d0c729dfbe26467", "igndv.near",
                     "a4c4a25b129adb8cda1dde035722e5f9559d65f72317c90e58920d296672be86",
                     "7b6dd1a6ef17c313d2e41701c2bfb93b1c934d1631c0a1f1b227cc0900d0c40e",
                     "9f6e3a3f8805f47424e7428d2443c1e692f210a7d2f9d27fa94f7e14ddeb233b", "stakenow.near",
                     "yunusemre.near", "tugba.near", "yns.near", "kriptoraptor.near", "madn.near", "fluxer.near",
                     "k1k1m0ra.near", "nguyentuantth2013.near", "jfevantuan.near", "deichpenner.near", "nazelisa.near",
                     "a3b52c13208325849f4163a716363e7fd191741a657d43158717616f40bd9b71", "gokhanerbatu.near",
                     "tatarin23.near", "farpost.near", "cronus.near", "zklim.near", "bagadefente.near", "shefcoin.near",
                     "steamroid.near", "geekzik.near", "tnma.near", "chappie.near", "sugihcrypto.near", "huunv90.near",
                     "jonathankehauinvest.near", "7f08265f3ab13ba349eaaa218757c688a307b768b53599a7dce1bd1da76dd41a",
                     "jackin90.near", "arica.near", "e677822bdf097ca82a9589323636936ad6d0083bb72bddf331fc41adef1a20f7",
                     "ec9c8f1b06fad76cd7d534a76c83a575bd14e024225413b888705725b95cb653", "async.near",
                     "2bc6e8cae8b176b5ee8d489261e0893b402e127e14d396877ba5ac6448dea009",
                     "af3f73861cba7143599145c3c728eb5d2d331c570c4b980836466424ce28df5b",
                     "7eab60d92177fe3fe234fa3a1cb66762aa71516c8dca5d08d06545ad3acf43a7", "sehir.near",
                     "e19d8d594ca951929d0cc75b5f52b31ecdc5960aaedf949ad2a0b3391a26c39e", "colico.near", "jackou12.near",
                     "cogitat.near", "khanhdev.near", "khanhwrk.near", "khanhweb3.near", "luong0709.near",
                     "khanhdefi.near", "e6730eb59d2e640051518fbfeae4a5b4708859d3055607654e98a068fdb21907",
                     "mako3388.near", "adb0276ceba0969f89d541e485dc6dc50ad61e8c3d7834a4153764edec53afec",
                     "34ab0b8c03ad9d68995d4476080b3a18a34277898c0483fb4001aecdaf5c4c2c", "georgemarlow.near",
                     "d29dd2e7b75e209c7cc90ee34832a70b0df10de14b1cbf6eda72f6766aad138c",
                     "bdfd41d94fb5e61999c5fa4f18ccee8cb520f2286695e6aa75750b1c673f971b", "davidnvg1511.near",
                     "e81caf0874f6e7ac04c1571a49233d447a7cf6047d02833fd7280b2d3edb2473",
                     "da69e2721e84fde1ad8bb3a10d98b874a06d0131bfcf4f8254f4f7a7cbf94bd2", "sailinon.near"]
    logger.info("draw_accounts:{}", json.dumps(draw_accounts))
    logger.info("draw_account size:{}", len(draw_accounts))
    random_seq_list = random(len(draw_accounts))
    logger.info("random_seq_list:{}", json.dumps(random_seq_list))
    draw(draw_accounts, random_seq_list)
    end_time = int(time.time())
    logger.info("get_account_list consuming time:{}", end_time - start_time)
