import sys
import time
sys.path.append('../')
from db_provider import get_conversion_token_log, get_conversion_token_day_data_index_number, add_conversion_token_day_data


def handel_rhea_data(network_id):
    timestamp = int(time.time())
    index_number = get_conversion_token_day_data_index_number(network_id)
    week_data_map = {}
    start_id = 0
    for i in range(10000):
        conversion_log_list = get_conversion_token_log(network_id, start_id)
        if len(conversion_log_list) < 1:
            print("get_conversion_token_log end")
            break
        else:
            first_id = conversion_log_list[0]["id"]
            print("first_id:", first_id)
            last_id = conversion_log_list[-1]["id"]
            print("last_id:", last_id)
            for conversion_log in conversion_log_list:
                start_id = conversion_log["id"]
                end_time_ms = conversion_log["end_time_ms"]
                if end_time_ms is None or end_time_ms == "":
                    locking_duration = 0
                else:
                    lock_time = int(conversion_log["end_time_ms"]) - int(conversion_log["start_time_ms"])
                    locking_duration = int(lock_time / 86400000 / 7)
                if locking_duration in week_data_map:
                    locking_duration_data_list = week_data_map[locking_duration]
                    locking_duration_data_list.append(conversion_log)
                    week_data_map[locking_duration] = locking_duration_data_list
                else:
                    week_data_map[locking_duration] = [conversion_log]
    conversion_token_day_data_list = []
    for week, week_data_list in week_data_map.items():
        account_data_map = {}
        target_amount_map = {}
        for week_data in week_data_list:
            account_id = week_data["account_id"]
            source_token_id = week_data["source_token_id"]
            conversion_type = week_data["event"]
            source_amount = week_data["source_amount"]
            target_amount = week_data["target_amount"]
            if source_amount is None or source_amount == "":
                source_amount = 0
            if target_amount is None or target_amount == "":
                target_amount = 0
            account_token_data_map = {}
            if source_token_id in account_data_map:
                account_token_data_map = account_data_map[source_token_id]
            account_map_key = account_id + "$" + conversion_type
            target_amount_map_key = account_map_key + source_token_id
            if account_map_key in account_token_data_map:
                account_token_data_map[account_map_key] = account_token_data_map[account_map_key] + int(source_amount)
                target_amount_map[target_amount_map_key] = target_amount_map[target_amount_map_key] + int(target_amount)
            else:
                account_token_data_map[account_map_key] = int(source_amount)
                target_amount_map[target_amount_map_key] = int(target_amount)
            account_data_map[source_token_id] = account_token_data_map
        for token_id, token_account_data in account_data_map.items():
            sorted_data = sorted(token_account_data.items(), key=lambda x: x[1], reverse=True)
            rank = 1
            for key, value in sorted_data:
                key_list = key.split("$")
                if key_list[1] == "create_conversion":
                    data_type = "Lock"
                else:
                    data_type = "unLock"
                conversion_token_day_data = {"token_id": token_id, "account_id": key_list[0], "balance": value, "rank": rank, "locking_duration": week, "type": data_type, "target_amount": target_amount_map[key + token_id]}
                # if rank > 100:
                #     break
                rank = rank + 1
                conversion_token_day_data_list.append(conversion_token_day_data)
    add_conversion_token_day_data(network_id, conversion_token_day_data_list, index_number + 1, timestamp)


if __name__ == '__main__':
    print("start rhea data task")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            handel_rhea_data(network_id)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("end rhea data task")
