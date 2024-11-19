import decimal
import pymysql
import json
from datetime import datetime as datatime
from loguru import logger
from config import Cfg


class Encoder(json.JSONEncoder):
    """
    Handle special data types, such as decimal and time types
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)

        if isinstance(o, datatime):
            return o.strftime("%Y-%m-%d %H:%M:%S")

        super(Encoder, self).default(o)


def format_percentage(one, two):
    p = 0
    try:
        p = int(two) / int(one)
    except Exception as e:
        logger.error("format percentage error:{}", e)
    return '%.2f' % p


def get_db_connect(network_id):
    conn = pymysql.connect(
        host=Cfg.NETWORK[network_id]["DB_HOST"],
        port=int(Cfg.NETWORK[network_id]["DB_PORT"]),
        user=Cfg.NETWORK[network_id]["DB_UID"],
        passwd=Cfg.NETWORK[network_id]["DB_PWD"],
        db=Cfg.NETWORK[network_id]["DB_DSN"])
    return conn


def add_near_lake_latest_actions(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_latest_actions(timestamp, tx_id, receiver_account_id, method_name, args, " \
          "deposit, status, predecessor_account_id, receiver_id, receipt_id, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["timestamp"], data["tx_id"], data["receiver_account_id"], data["method_name"],
                                data["args"], data["deposit"], data["status"], data["predecessor_account_id"],
                                data["receiver_id"], data["receipt_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert near lake latest actions log to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_limit_order_log(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_limit_order (type, tx_id, block_id, receipt_id, order_id, " \
          "owner_id, pool_id, point, sell_token, original_amount, original_deposit_amount, `timestamp`, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["type"], data["tx_id"], data["block_id"], data["receipt_id"], data["order_id"],
                                data["owner_id"], data["pool_id"], data["point"], data["sell_token"],
                                data["original_amount"], data["original_deposit_amount"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert near_lake_limit_order to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_limit_order_swap_log(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_limit_order (type, tx_id, block_id, receipt_id, token_in, " \
          "token_out, pool_id, point, amount_in, amount_out, " \
          "owner_id, `timestamp`, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["type"], data["tx_id"], data["block_id"], data["receipt_id"], data["token_in"],
                                data["token_out"], data["pool_id"], data["point"], data["amount_in"],
                                data["amount_out"],
                                data["owner_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert near_lake_limit_order swap to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_burrow_event_log(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into burrow_event_log(event, account_id, amount, token_id, receipt_id, block_id, predecessor_id, " \
          "liquidation_account_id, collateral_sum, repaid_sum, booster_amount, duration, x_booster_amount," \
          "total_booster_amount, total_x_booster_amount, `timestamp`, create_time, position, args) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,%s)"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["event"], data["account_id"], data["amount"], data["token_id"], data["receipt_id"],
                                data["block_id"], data["predecessor_id"], data["liquidation_account_id"],
                                data["collateral_sum"], data["repaid_sum"], data["booster_amount"], data["duration"],
                                data["x_booster_amount"], data["total_booster_amount"], data["total_x_booster_amount"],
                                data["timestamp"], data["position"], data["args"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert burrow_event_log to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_swap_log(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_swap_log(block_hash, block_id, predecessor_id, receiver_id, sender_id, amount, " \
          "`force`, pool_id, token_in, token_out, amount_in, min_amount_out, swap_in, swap_out, `timestamp`, " \
          "create_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["block_hash"], data["block_id"], data["predecessor_id"], data["receiver_id"],
                                data["sender_id"], data["amount"], data["force"], data["pool_id"], data["token_in"],
                                data["token_out"], data["amount_in"], data["min_amount_out"],
                                data["swap_in"], data["swap_out"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert near_lake_swap_log to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_swap(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(swapper, token_in, token_out, amount_in, amount_out, tx_id, " \
          "block_id, timestamp, args, predecessor_id, receiver_id, pool_id, protocol_fee_amounts, total_fee_amounts, " \
          "create_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["swapper"], data["token_in"], data["token_out"], data["amount_in"],
                                data["amount_out"], data["tx_id"], data["block_id"], data["timestamp"],
                                data["args"], data["predecessor_id"], data["receiver_id"],
                                data["pool_id"], data["protocol_fee_amounts"], data["total_fee_amounts"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert swap log to db error:{}", e)
        logger.error("insert swap log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_swap_desire(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(swapper, token_in, token_out, amount_in, amount_out, tx_id, " \
          "block_id, timestamp, args, predecessor_id, receiver_id, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["swapper"], data["token_in"], data["token_out"], data["amount_in"],
                                data["amount_out"], data["tx_id"], data["block_id"], data["timestamp"],
                                data["args"], data["predecessor_id"], data["receiver_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert swap_desire log to db error:{}", e)
        logger.error("insert swap_desire log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_liquidity_added(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(event_method, lpt_id, merge_lpt_ids, owner_id, pool_id, left_point, " \
          "right_point, added_amount, cur_amount, paid_token_x, paid_token_y, tx_id, block_id, timestamp, " \
          "args, predecessor_id, receiver_id, claim_fee_token_x, claim_fee_token_y, merge_token_x, merge_token_y, " \
          "remove_token_x, remove_token_y, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["event_method"], data["lpt_id"], data["merge_lpt_ids"], data["owner_id"],
                                data["pool_id"], data["left_point"], data["right_point"], data["added_amount"],
                                data["cur_amount"], data["paid_token_x"], data["paid_token_y"], data["tx_id"],
                                data["block_id"], data["timestamp"], data["args"], data["predecessor_id"],
                                data["receiver_id"], data["claim_fee_token_x"], data["claim_fee_token_y"],
                                data["merge_token_x"], data["merge_token_y"], data["remove_token_x"],
                                data["remove_token_y"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert liquidity_added log to db error:{}", e)
        logger.error("insert liquidity_added log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_liquidity_removed(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(event_method, lpt_id, owner_id, pool_id, left_point, right_point, " \
          "removed_amount, cur_amount, refund_token_x, refund_token_y, tx_id, block_id, timestamp, args, " \
          "predecessor_id, receiver_id, claim_fee_token_x, claim_fee_token_y, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["event_method"], data["lpt_id"], data["owner_id"], data["pool_id"],
                                data["left_point"], data["right_point"], data["removed_amount"], data["cur_amount"],
                                data["refund_token_x"], data["refund_token_y"], data["tx_id"], data["block_id"],
                                data["timestamp"], data["args"], data["predecessor_id"], data["receiver_id"],
                                data["claim_fee_token_x"], data["claim_fee_token_y"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert liquidity_removed log to db error:{}", e)
        logger.error("insert liquidity_removed log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_lostfound(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(user, token, amount, locked, tx_id, " \
          "block_id, timestamp, args, predecessor_id, receiver_id, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["user"], data["token"], data["amount"], data["locked"],
                                data["tx_id"], data["block_id"], data["timestamp"],
                                data["args"], data["predecessor_id"], data["receiver_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert lostfound log to db error:{}", e)
        logger.error("insert lostfound log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_order_added(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(order_id, created_at, owner_id, pool_id, point, sell_token, " \
          "buy_token, original_amount, original_deposit_amount, swap_earn_amount, " \
          "tx_id, block_id, timestamp, args, predecessor_id, receiver_id, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["order_id"], data["created_at"], data["owner_id"], data["pool_id"],
                                data["point"], data["sell_token"], data["buy_token"], data["original_amount"],
                                data["original_deposit_amount"], data["swap_earn_amount"],
                                data["tx_id"], data["block_id"], data["timestamp"],
                                data["args"], data["predecessor_id"], data["receiver_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert order_added log to db error:{}", e)
        logger.error("insert order_added log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_order_cancelled(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(order_id, created_at, cancel_at, owner_id, pool_id, point, sell_token," \
          "buy_token, request_cancel_amount, actual_cancel_amount, original_amount, cancel_amount, remain_amount, " \
          "bought_amount, tx_id, block_id, timestamp, args, predecessor_id, receiver_id, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["order_id"], data["created_at"], data["cancel_at"], data["owner_id"],
                                data["pool_id"],
                                data["point"], data["sell_token"], data["buy_token"], data["request_cancel_amount"],
                                data["actual_cancel_amount"], data["original_amount"], data["cancel_amount"],
                                data["remain_amount"], data["bought_amount"], data["tx_id"], data["block_id"],
                                data["timestamp"], data["args"], data["predecessor_id"], data["receiver_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert order_cancelled log to db error:{}", e)
        logger.error("insert order_cancelled log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_order_completed(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(event_method, order_id, created_at, completed_at, owner_id, pool_id, " \
          "point, sell_token, buy_token, original_amount, original_deposit_amount, swap_earn_amount, cancel_amount, " \
          "bought_amount, cancel_amount_this_time, bought_amount_this_time, tx_id, block_id, " \
          "timestamp, args, predecessor_id, receiver_id, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["event_method"], data["order_id"], data["created_at"], data["completed_at"],
                                data["owner_id"], data["pool_id"], data["point"], data["sell_token"], data["buy_token"],
                                data["original_amount"], data["original_deposit_amount"], data["swap_earn_amount"],
                                data["cancel_amount"], data["bought_amount"], data["cancel_amount_this_time"],
                                data["bought_amount_this_time"], data["tx_id"], data["block_id"], data["timestamp"],
                                data["args"], data["predecessor_id"], data["receiver_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert order_completed log to db error:{}", e)
        logger.error("insert order_completed log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_claim_charged_fee(data_list, table_suffix, network):
    db_conn = get_db_connect(network)

    sql = "insert into t_"+str(table_suffix)+"(user, pool_id, amount_x, amount_y, tx_id, " \
          "block_id, timestamp, args, predecessor_id, receiver_id, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["user"], data["pool_id"], data["amount_x"], data["amount_y"],
                                data["tx_id"], data["block_id"], data["timestamp"],
                                data["args"], data["predecessor_id"], data["receiver_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        logger.error("insert claim_charged_fee log to db error:{}", e)
        logger.error("insert claim_charged_fee log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_account_not_registered_logs(data_list, network):
    db_conn = get_db_connect(network)

    sql = "insert into account_not_registered_logs(block_id, tx_id, token_id, sender_id, amount, status, " \
          "timestamp, create_time, update_time, block, receiver_id, type, log) " \
          "values(%s,%s,%s,%s,%s,1,%s,now(),now(),%s,%s,%s,%s)"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["block_id"], data["tx_id"], data["token_id"], data["sender_id"], data["amount"],
                                data["timestamp"], data["block"], data["receiver_id"], data["type"], data["log"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert account log to db error:{}", e)
        logger.error("insert account log to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


def add_liquidity_pools(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_liquidity_pools(pool_id, account_id, receipt_id, create_time) " \
          "values(%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["pool_id"], data["account_id"], data["receipt_id"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        print("insert liquidity pools log to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_liquidity_log(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_liquidity_log(block_hash, block_id, predecessor_id, receiver_id, method_name, " \
          "pool_id, shares, amounts, token_in, token_out, amount_in, amount_out, log, `timestamp`, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["block_hash"], data["block_id"], data["predecessor_id"], data["receiver_id"],
                                data["method_name"], data["pool_id"], data["shares"], data["amounts"],
                                data["token_in"], data["token_out"], data["amount_in"], data["amount_out"],
                                data["log"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert liquidity log to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_xref_log(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_xref_log(block_hash, block_id, sender_id, amount_in, amount_out, predecessor_id, " \
          "receiver_id, ratio, `timestamp`, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["block_hash"], data["block_id"], data["sender_id"], data["amount_in"],
                                data["amount_out"], data["predecessor_id"], data["receiver_id"],
                                format_percentage(data["amount_in"], data["amount_out"]), data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert xref log to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_farm_log(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_farm_log(block_hash, block_id, predecessor_id, receiver_id, token_id, sender_id, " \
          "msg, event, farmer_id, seed_id, amount, increased_power, duration, `timestamp`, create_time) " \
          "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["block_hash"], data["block_id"], data["predecessor_id"], data["receiver_id"],
                                data["token_id"], data["sender_id"], data["msg"], data["event"], data["farmer_id"],
                                data["seed_id"], data["amount"], data["increased_power"],
                                data["duration"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert farm log to db error:{}", e)
    finally:
        cursor.close()
        db_conn.close()


def add_withdraw_reward_data(data_list, network_id):
    db_conn = get_db_connect(network_id)

    sql = "insert into near_lake_withdraw_reward(account_id, amount, token, receipt_id, block_id, timestamp, " \
          "create_time) values(%s,%s,%s,%s,%s,%s, now())"

    insert_data = []
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        for data in data_list:
            insert_data.append((data["account_id"], data["amount"], data["token"], data["receipt_id"],
                                data["block_id"], data["timestamp"]))

        cursor.executemany(sql, insert_data)
        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        logger.error("insert withdraw reward log to db error:{}", e)
        logger.error("insert withdraw reward to db insert_data:{}", insert_data)
    finally:
        cursor.close()
        db_conn.close()


if __name__ == '__main__':
    logger.info("#########MAINNET###########")
