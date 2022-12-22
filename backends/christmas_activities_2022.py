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
            award_account = draw_account_list[random_seqs[i]]
            award_account_list.append(award_account)
    # not_award_account_list = list(set(draw_account_list).difference(set(award_account_list)))
    for account in award_account_list:
        draw_account_list.remove(account)
    logger.info("award_account_list:{}", json.dumps(award_account_list))
    logger.info("not_award_account_list:{}", json.dumps(draw_account_list))


logger.add("christmas_activities_2022.log")
if __name__ == '__main__':
    logger.info("#########MAINNET###########")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            start_time = int(time.time())
            draw_accounts = get_account_list(network_id)
            logger.info("draw_accounts:{}", json.dumps(draw_accounts))
            logger.info("draw_account size:{}", len(draw_accounts))
            random_seq_list = random(len(draw_accounts))
            logger.info("random_seq_list:{}", json.dumps(random_seq_list))
            draw(draw_accounts, random_seq_list)
            end_time = int(time.time())
            logger.info("get_account_list consuming time:{}", end_time - start_time)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
