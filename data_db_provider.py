import decimal
import pymysql
import json
from datetime import datetime
import time
from data_config import Cfg


class Encoder(json.JSONEncoder):
    """
    Handle special data types, such as decimal and time types
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)

        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")

        super(Encoder, self).default(o)


def get_data_db_connect():
    network_id = "MAINNET"
    conn = pymysql.connect(
        host=Cfg.NETWORK[network_id]["DATA_DB_HOST"],
        port=int(Cfg.NETWORK[network_id]["DATA_DB_PORT"]),
        user=Cfg.NETWORK[network_id]["DATA_DB_UID"],
        passwd=Cfg.NETWORK[network_id]["DATA_DB_PWD"],
        db=Cfg.NETWORK[network_id]["DATA_DB_DSN"])
    return conn


def get_token_ratio_swap_data():
    # Get current timestamp
    # now = int(time.time())
    # before_time = now - (1 * 24 * 60 * 60)
    token_decimal = {}
    res_list = []
    for token in Cfg.TOKENS["MAINNET"]:
        token_decimal[token["NEAR_ID"]] = token["DECIMAL"]
    db_conn = get_data_db_connect()
    sql = "select token_in,token_out,swap_in,swap_out,`timestamp` from near_lake_swap_log group by token_in," \
          "token_out order by `timestamp` desc"
    # par = (contract_address, before_time)
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            res_data = {
                "token_in": row["token_in"],
                "token_out": row["token_out"],
                "ratio": ""
            }
            if row["token_in"] in token_decimal:
                dis = int("1" + "0" * token_decimal[row["token_in"]])
                row["swap_in"] = int(row["swap_in"]) / dis
            if row["token_out"] in token_decimal:
                dis = int("1" + "0" * token_decimal[row["token_out"]])
                row["swap_out"] = int(row["swap_out"]) / dis
            res_data["ratio"] = format_percentage(float(row["swap_in"]), float(row["swap_out"]))
            res_list.append(res_data)
        return res_list
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()


def get_swap_count_data(start_time, end_time):
    start_time = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    start_time_timestamp = int(time.mktime(start_time) * 1000000000)
    end_time_timestamp = int(time.mktime(end_time) * 1000000000)
    swap_count = 0
    db_conn = get_data_db_connect()
    sql = "select count(*) as count from near_lake_swap_log where `timestamp` >= %s and `timestamp` <= %s"
    par = (start_time_timestamp, end_time_timestamp)
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql, par)
        row = cursor.fetchone()
        if row is not None:
            swap_count = row["count"]
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()
    return swap_count


def get_swap_count_by_account_data(start_time, end_time):
    start_time = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    start_time_timestamp = int(time.mktime(start_time) * 1000000000)
    end_time_timestamp = int(time.mktime(end_time) * 1000000000)
    swap_count = 0
    db_conn = get_data_db_connect()
    sql = "select count(*) as count from (select count(*) as count1, sender_id from near_lake_swap_log " \
          "where `timestamp` >= %s and `timestamp` <= %s group by sender_id) as cc"
    par = (start_time_timestamp, end_time_timestamp)
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql, par)
        row = cursor.fetchone()
        if row is not None:
            swap_count = row["count"]
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()
    return swap_count


def get_swap_count_by_pool_data(start_time, end_time):
    start_time = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    start_time_timestamp = int(time.mktime(start_time) * 1000000000)
    end_time_timestamp = int(time.mktime(end_time) * 1000000000)
    swap_data_list = []
    db_conn = get_data_db_connect()
    sql = "select count(*) as count, pool_id from near_lake_swap_log " \
          "where `timestamp` >= %s and `timestamp` <= %s group by pool_id"
    par = (start_time_timestamp, end_time_timestamp)
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql, par)
        rows = cursor.fetchall()
        for row in rows:
            swap_data = {
                "pool_id": row["pool_id"],
                "count": row["count"]
            }
            swap_data_list.append(swap_data)
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()
    return swap_data_list


def get_add_liquidity_count_data(start_time, end_time):
    return get_liquidity_count_data(start_time, end_time, "add")


def get_remove_liquidity_count_data(start_time, end_time):
    return get_liquidity_count_data(start_time, end_time, "remove")


def get_liquidity_count_data(start_time, end_time, method):
    start_time = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    start_time_timestamp = int(time.mktime(start_time) * 1000000000)
    end_time_timestamp = int(time.mktime(end_time) * 1000000000)
    add_liquidity_count = 0
    db_conn = get_data_db_connect()
    if method == "add":
        sql = "select count(*) as count from near_lake_liquidity_log where `timestamp` >= %s " \
              "and `timestamp` <= %s and (method_name = 'add_liquidity' or method_name = 'add_stable_liquidity')"
    else:
        sql = "select count(*) as count from near_lake_liquidity_log where `timestamp` >= %s " \
              "and `timestamp` <= %s and (method_name = 'remove_liquidity' or method_name = 'remove_liquidity_by_tokens')"
    par = (start_time_timestamp, end_time_timestamp)
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql, par)
        row = cursor.fetchone()
        if row is not None:
            add_liquidity_count = row["count"]
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()
    return add_liquidity_count


def get_add_liquidity_count_by_pool_data(start_time, end_time):
    return get_liquidity_count_by_pool_data(start_time, end_time, "add")


def get_remove_liquidity_count_by_pool_data(start_time, end_time):
    return get_liquidity_count_by_pool_data(start_time, end_time, "remove")


def get_liquidity_count_by_pool_data(start_time, end_time, method):
    start_time = time.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = time.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    start_time_timestamp = int(time.mktime(start_time) * 1000000000)
    end_time_timestamp = int(time.mktime(end_time) * 1000000000)
    liquidity_data_list = []
    db_conn = get_data_db_connect()
    if method == "add":
        sql = "select count(*) as count, pool_id from near_lake_liquidity_log where `timestamp` >= %s " \
              "and `timestamp` <= %s and (method_name = 'add_liquidity' or method_name = 'add_stable_liquidity')  group by pool_id"
    else:
        sql = "select count(*) as count, pool_id from near_lake_liquidity_log where `timestamp` >= %s " \
              "and `timestamp` <= %s and (method_name = 'remove_liquidity' or method_name = 'remove_liquidity_by_tokens')  group by pool_id"
    par = (start_time_timestamp, end_time_timestamp)
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql, par)
        rows = cursor.fetchall()
        for row in rows:
            liquidity_data = {
                "pool_id": row["pool_id"],
                "count": row["count"]
            }
            liquidity_data_list.append(liquidity_data)
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()
    return liquidity_data_list


def format_percentage(one, two):
    if 0 == one or 0 == two:
        p = 0
    else:
        p = 100 * one / two
    return '%.2f' % p


if __name__ == '__main__':
    print("#########MAINNET###########")
    # clear_token_price()
    # print(get_token_ratio_swap_data())
    print(get_swap_count_data("2022-10-23 23:59:55", "2022-10-23 23:59:59"))
