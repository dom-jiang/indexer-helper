import decimal
import pymysql
import json
from datetime import datetime as datatime
from config import Cfg
import time
from redis_provider import RedisProvider, list_history_token_price
import datetime


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


def get_db_connect(network_id: str):
    conn = pymysql.connect(
        host=Cfg.NETWORK[network_id]["DB_HOST"], 
        port=int(Cfg.NETWORK[network_id]["DB_PORT"]), 
        user=Cfg.NETWORK[network_id]["DB_UID"], 
        passwd=Cfg.NETWORK[network_id]["DB_PWD"], 
        db=Cfg.NETWORK[network_id]["DB_DSN"])
    return conn


def get_history_token_price(id_list: list) -> list:
    """
    Batch query historical price
    """
    """because 'usn' Special treatment require,'use 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' 
    Price of, Record whether the input parameter is passed in 'usn'，If there is an incoming 'usn', But no incoming 
    'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', Then the return parameter only needs to be 
    returned 'usn', Do not return 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', If both have 
    incoming,It is necessary to return the price information of two at the same time,usn_flag 1 means no incoming 'usn'
    2 means that it is passed in at the same time 'usn和dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
    ,3 means that only 'usn',No incoming 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' """
    usn_flag = 1
    # Special treatment of USN to determine whether USN is included in the input parameter
    if "usn" in id_list:
        if "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near" in id_list:
            usn_flag = 2
        else:
            usn_flag = 3
            id_list = ['dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' if i == 'usn' else i for i in
                       id_list]

    ret = []
    history_token_prices = list_history_token_price(Cfg.NETWORK_ID, id_list)
    for token_price in history_token_prices:
        if not token_price is None:
            float_ratio = format_percentage(float(token_price['price']), float(token_price['history_price']))
            if "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near" in token_price['contract_address']:
                if 2 == usn_flag:
                    new_usn = {
                        "price": token_price['price'],
                        "decimal": 18,
                        "symbol": "USN",
                        "float_ratio": float_ratio,
                        "timestamp": token_price['datetime'],
                        "contract_address": "usn"
                    }
                    ret.append(new_usn)
                elif 3 == usn_flag:
                    token_price['contract_address'] = "usn"
                    token_price['symbol'] = "USN"
                    token_price['decimal'] = 18
            token_price['float_ratio'] = float_ratio
            ret.append(token_price)
    return ret


def add_history_token_price(contract_address, symbol, price, decimals, network_id):
    """
    Write the token price to the MySQL database
    """
    for token in Cfg.TOKENS[Cfg.NETWORK_ID]:
        if token["NEAR_ID"] in contract_address:
            symbol = token["SYMBOL"]

    # Get current timestamp
    now = int(time.time())
    before_time = now - (1 * 24 * 60 * 60)
    db_conn = get_db_connect(Cfg.NETWORK_ID)
    sql = "insert into mk_history_token_price(contract_address, symbol, price, `decimal`, create_time, update_time, " \
          "`status`, `timestamp`) values(%s,%s,%s,%s, now(), now(), 1, %s) "
    par = (contract_address, symbol, price, decimals, now)
    # Query the price records 24 hours ago according to the token
    sql2 = "SELECT price FROM mk_history_token_price where contract_address = %s and `timestamp` < " \
           "%s order by from_unixtime(`timestamp`, '%%Y-%%m-%%d %%H:%%i:%%s') desc limit 1"
    par2 = (contract_address, before_time)
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        cursor.execute(sql, par)
        # Submit to database for execution
        db_conn.commit()

        cursor.execute(sql2, par2)
        old_rows = cursor.fetchone()
        old_price = price
        if old_rows is not None:
            old_price = old_rows["price"]

        history_token = {
            "price": price,
            "history_price": old_price,
            "symbol": symbol,
            "datetime": now,
            "contract_address": contract_address,
            "decimal": decimals
        }
        redis_conn = RedisProvider()
        redis_conn.begin_pipe()
        redis_conn.add_history_token_price(network_id, contract_address, json.dumps(history_token, cls=Encoder))
        redis_conn.end_pipe()
        redis_conn.close()

    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()


def format_percentage(new, old):
    if new == 0:
        p = 0
    elif old == 0:
        p = 100
    else:
        p = 100 * (new - old) / old
    return '%.2f' % p


def clear_token_price():
    now = int(time.time())
    before_time = now - (7*24*60*60)
    print("seven days ago time:", before_time)
    conn = get_db_connect(Cfg.NETWORK_ID)
    sql = "delete from mk_history_token_price where `timestamp` < %s"
    cursor = conn.cursor()
    try:
        cursor.execute(sql, before_time)
        # Submit to database for execution
        conn.commit()
    except Exception as e:
        # Rollback on error
        conn.rollback()
        print(e)
    finally:
        cursor.close()


def summary_hourly_price():
    db_conn = get_db_connect(Cfg.NETWORK_ID)
    sql = "select mh.symbol, mh.contract_address, " \
          "DATE_FORMAT(from_unixtime(mh.`timestamp`, '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d %H') as time, " \
          "max(mh.price) as high_price, min(mh.price) as low_price, " \
          "(select price from mk_history_token_price mt " \
          "where mt.contract_address = mh.contract_address and mt.`timestamp` = min(mh.`timestamp`)) as start_price, " \
          "(select price from mk_history_token_price mp " \
          "where mp.contract_address = mh.contract_address and mp.`timestamp` = max(mh.`timestamp`)) as end_price " \
          "from mk_history_token_price mh where mh.`status` = 1 group by contract_address,time "
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()

    sql1 = "insert into token_price_report(symbol, contract_address, time, `status`, start_price, high_price, " \
           "low_price, end_price, float_ratio) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    rows_length = len(rows)
    now = int(time.time())
    data = []
    try:
        for index in range(rows_length):
            data.append((rows[index]["symbol"], rows[index]["contract_address"], rows[index]["time"], 1,
                         rows[index]["start_price"], rows[index]["high_price"], rows[index]["low_price"],
                         rows[index]["end_price"], 1))
            if (index != 0 and index % 10 == 0) or index == rows_length-1:
                cursor.executemany(sql1, data)
                db_conn.commit()
                data = []

        sql2 = "update mk_history_token_price set `status` = 2 where `status` = 1 and `timestamp` < %s" % now
        cursor.execute(sql2)
        # Submit to database for execution
        db_conn.commit()
    except Exception as e:
        # Rollback on error
        db_conn.rollback()
        print(e)
    finally:
        cursor.close()


def price_report(network_id):
    now_time = int(time.time())
    handle_price_report_hour(network_id, now_time)
    handle_price_report_week(network_id, now_time)
    handle_price_report_month(network_id, now_time)
    handle_price_report_year(network_id, now_time)


def handle_price_report_hour(network_id, now_time):
    date_time = now_time - (1 * 24 * 60 * 60)
    db_conn = get_db_connect(Cfg.NETWORK_ID)
    sql_h = "select symbol,contract_address,time,`status`,start_price,high_price,low_price,end_price,float_ratio " \
            "from token_price_report where time > from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
            "group by contract_address,time " % date_time
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql_h)
    rows_h = cursor.fetchall()

    history_time = date_time - (1 * 24 * 60 * 60)
    sql_h_history = "select symbol,contract_address,time,`status`,start_price,high_price,low_price,end_price," \
                    "float_ratio from token_price_report where time >= from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
                    "and time < from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s')" % (history_time, date_time)
    cursor.execute(sql_h_history)
    rows_h_history = cursor.fetchall()

    cursor.close()

    token_list_h = {}
    for row in rows_h:
        for row_history in rows_h_history:
            old_time = (row_history["time"] + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            if row_history["contract_address"] in row["contract_address"] and old_time in str(row["time"]):
                row["float_ratio"] = format_percentage(float(row['start_price']), float(row_history['start_price']))
        if row["contract_address"] in token_list_h.keys():
            token_list_h[row["contract_address"]].append(row)
        else:
            token_list_h[row["contract_address"]] = [row]
    for key_h, values in token_list_h.items():
        key_h = key_h + "_h"
        print("key:", key_h)
        add_price_report_to_redis(network_id, key_h, values)


def handle_price_report_week(network_id, now_time):
    date_time = now_time - (7 * 24 * 60 * 60)
    db_conn = get_db_connect(Cfg.NETWORK_ID)
    sql_w = "select symbol,contract_address,`status`,max(high_price) as high_price,min(low_price) as low_price," \
            "float_ratio,DATE_FORMAT(concat(date(time), ' ',floor(HOUR(time)/8)*8),'%%Y-%%m-%%d %%H') as date_time," \
            "time, (select start_price from token_price_report mt " \
            "where mt.contract_address = tpr.contract_address and mt.time = min(tpr.time) group by mt.time) " \
            "as start_price, (select end_price from token_price_report mp " \
            "where mp.contract_address = tpr.contract_address and mp.time = max(tpr.time) group by mp.time) " \
            "as end_price from token_price_report tpr where time >= from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
            "group by contract_address,date_time" % date_time
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql_w)
    rows_w = cursor.fetchall()

    history_time = date_time - (7 * 24 * 60 * 60)
    sql_w_history = "select symbol,contract_address,`status`,max(high_price) as high_price,min(low_price) as low_price," \
                    "float_ratio,DATE_FORMAT(concat(date(time), ' ',floor(HOUR(time)/8)*8),'%%Y-%%m-%%d %%H') as date_time," \
                    "time, (select start_price from token_price_report mt " \
                    "where mt.contract_address = tpr.contract_address and mt.time = min(tpr.time) group by mt.time) " \
                    "as start_price, (select end_price from token_price_report mp " \
                    "where mp.contract_address = tpr.contract_address and mp.time = max(tpr.time) group by mp.time) as " \
                    "end_price from token_price_report tpr where time >= from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
                    "and time < from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
                    "group by contract_address,date_time" % (history_time, date_time)
    cursor.execute(sql_w_history)
    rows_w_history = cursor.fetchall()

    cursor.close()

    token_list_w = {}
    for row in rows_w:
        row["time"] = row["date_time"]
        for row_history in rows_w_history:
            old_time = (row_history["time"] + datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            if row_history["contract_address"] in row["contract_address"] and old_time in str(row["time"]):
                row["float_ratio"] = format_percentage(float(row['start_price']), float(row_history['start_price']))
        if row["contract_address"] in token_list_w.keys():
            token_list_w[row["contract_address"]].append(row)
        else:
            token_list_w[row["contract_address"]] = [row]
    for key_w, values in token_list_w.items():
        key_w = key_w + "_w"
        print("key:", key_w)
        add_price_report_to_redis(network_id, key_w, values)


def handle_price_report_month(network_id, now_time):
    date_time = now_time - (30 * 24 * 60 * 60)
    db_conn = get_db_connect(Cfg.NETWORK_ID)
    sql_m = "select symbol,contract_address,`status`,max(high_price) as high_price,min(low_price) as low_price," \
            "float_ratio,DATE_FORMAT(time, '%%Y-%%m-%%d') as date_time,time," \
            "(select start_price from token_price_report mt " \
            "where mt.contract_address = tpr.contract_address and mt.time = min(tpr.time) group by mt.time) " \
            "as start_price, (select end_price from token_price_report mp " \
            "where mp.contract_address = tpr.contract_address and mp.time = max(tpr.time) group by mp.time) " \
            "as end_price from token_price_report tpr where time > from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
            "group by contract_address,date_time" % date_time
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql_m)
    rows_m = cursor.fetchall()

    history_time = date_time - (30 * 24 * 60 * 60)
    sql_m_history = "select symbol,contract_address,`status`,max(high_price) as high_price," \
                    "min(low_price) as low_price, float_ratio,DATE_FORMAT(time, '%%Y-%%m-%%d') as date_time,time," \
                    "(select start_price from token_price_report mt " \
                    "where mt.contract_address = tpr.contract_address and mt.time = min(tpr.time) group by mt.time) " \
                    "as start_price, (select end_price from token_price_report mp " \
                    "where mp.contract_address = tpr.contract_address and mp.time = max(tpr.time) group by mp.time) as" \
                    "end_price from token_price_report tpr where time >= from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
                    "and time < from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
                    "group by contract_address,date_time" % (history_time, date_time)
    cursor.execute(sql_m_history)
    rows_m_history = cursor.fetchall()

    cursor.close()

    token_list_m = {}
    for row in rows_m:
        row["time"] = row["date_time"]
        for row_history in rows_m_history:
            old_time = (row_history["time"] + datetime.timedelta(days=30)).strftime("%Y-%m-%d")
            if row_history["contract_address"] in row["contract_address"] and old_time in str(row["time"]):
                row["float_ratio"] = format_percentage(float(row['start_price']), float(row_history['start_price']))
        if row["contract_address"] in token_list_m.keys():
            token_list_m[row["contract_address"]].append(row)
        else:
            token_list_m[row["contract_address"]] = [row]
    for key_m, values in token_list_m.items():
        key_m = key_m + "_m"
        print("key:", key_m)
        add_price_report_to_redis(network_id, key_m, values)


def handle_price_report_year(network_id, now_time):
    date_time = now_time - (365 * 24 * 60 * 60)
    db_conn = get_db_connect(Cfg.NETWORK_ID)
    sql_y = "select symbol,contract_address,`status`,max(high_price) as high_price,min(low_price) as low_price," \
            "float_ratio,DATE_FORMAT(concat(YEAR(time),'-',MONTH(time),'-',floor(DAY(time) / 15) * 15),'%%Y-%%m-%%d')" \
            " AS date_time,time,(select start_price from token_price_report mt " \
            "where mt.contract_address = tpr.contract_address and mt.time = min(tpr.time) group by mt.time) " \
            "as start_price, (select end_price from token_price_report mp " \
            "where mp.contract_address = tpr.contract_address and mp.time = max(tpr.time) group by mp.time) " \
            "as end_price from token_price_report tpr where time >= from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
            "group by contract_address,date_time" % date_time
    cursor = db_conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor.execute(sql_y)
    rows_y = cursor.fetchall()

    history_time = date_time - (365 * 24 * 60 * 60)
    sql_y_history = "select symbol,contract_address,`status`,max(high_price) as high_price,min(low_price) as low_price," \
                    "float_ratio,DATE_FORMAT(concat(YEAR(time),'-',MONTH(time),'-',floor(DAY(time) / 15) * 15),'%%Y-%%m-%%d') " \
                    "AS date_time,time,(select start_price from token_price_report mt " \
                    "where mt.contract_address = tpr.contract_address and mt.time = min(tpr.time) group by mt.time) " \
                    "as start_price, (select end_price from token_price_report mp " \
                    "where mp.contract_address = tpr.contract_address and mp.time = max(tpr.time) group by mp.time) as" \
                    "end_price from token_price_report tpr where time >= from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
                    "and time < from_unixtime(%s, '%%Y-%%m-%%d %%H:%%i:%%s') " \
                    "group by contract_address,date_time" % (history_time, date_time)
    cursor.execute(sql_y_history)
    rows_y_history = cursor.fetchall()

    cursor.close()

    token_list_y = {}
    for row in rows_y:
        row["time"] = row["date_time"]
        for row_history in rows_y_history:
            old_time = (row_history["time"] + datetime.timedelta(days=365)).strftime("%Y-%m")
            if row_history["contract_address"] in row["contract_address"] and old_time in str(row["time"]):
                row["float_ratio"] = format_percentage(float(row['start_price']), float(row_history['start_price']))
        if row["contract_address"] in token_list_y.keys():
            token_list_y[row["contract_address"]].append(row)
        else:
            token_list_y[row["contract_address"]] = [row]
    for key_y, values in token_list_y.items():
        key_y = key_y + "_y"
        print("key:", key_y)
        add_price_report_to_redis(network_id, key_y, values)


def add_price_report_to_redis(network_id, key, values):
    redis_conn = RedisProvider()
    redis_conn.begin_pipe()
    redis_conn.add_token_price_report(network_id, key, json.dumps(values, cls=Encoder))
    redis_conn.end_pipe()
    redis_conn.close()

if __name__ == '__main__':
    print("#########MAINNET###########")
    # clear_token_price()
    # add_history_token_price("ref.fakes.testnet", "ref2", 1.003, 18, "MAINNET")
    # summary_hourly_price()
    price_report("TESTNET")
