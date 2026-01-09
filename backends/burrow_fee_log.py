import sys
import requests
import json
import time
import os
from datetime import datetime, timedelta
sys.path.append('../')
from redis_provider import RedisProvider, get_burrow_total_fee, get_burrow_total_revenue, get_cross_chain_total_fee, get_cross_chain_total_revenue
from db_provider import get_burrow_fee_log_data, update_burrow_fee_log_data, get_burrow_fee_log_24h_data
from config import Cfg
from near_multinode_rpc_provider import MultiNodeJsonProvider


# Rhea Intents 交易处理相关函数
RHEA_FEE_RECIPIENT = 'rhea-ccdfm.sputnik-dao.near'
INTENTS_API_BASE_URL = 'https://explorer.near-intents.org/api/v0'
TOKENS_METADATA_URL = 'https://1click.chaindefuser.com/v0/tokens'
INTENTS_JSON_FILE = 'rhea_intents_txs.json'
LAST_DEPOSIT_ADDRESS_FILE = 'rhea_intents_last_deposit_address.txt'
REVENUE_PERCENTAGE = 1


def handel_burrow_fee_log(network_id):
    log_id_list = []
    price_url = "https://api.ref.finance/list-token-price"
    token_price_data = requests.get(price_url).text
    token_price_data = json.loads(token_price_data)
    config_url = "https://api.burrow.finance/get_assets_paged_detailed"
    token_config_ret = requests.get(config_url).text
    token_config_ret = json.loads(token_config_ret)
    if token_config_ret["code"] == "0":
        token_config_data = {}
        for token_config in token_config_ret["data"]:
            token_config_data[token_config["token_id"]] = token_config["config"]["extra_decimals"]
        burrow_fee_log_list = get_burrow_fee_log_data(network_id)
        burrow_total_fee = get_burrow_total_fee()
        if burrow_total_fee is None:
            burrow_total_fee = 0
        else:
            burrow_total_fee = float(burrow_total_fee)
        burrow_total_revenue = get_burrow_total_revenue()
        if burrow_total_revenue is None:
            burrow_total_revenue = 0
        else:
            burrow_total_revenue = float(burrow_total_revenue)
        for burrow_fee_log in burrow_fee_log_list:
            token_id = burrow_fee_log["token_id"]
            if token_id not in token_price_data:
                print("token not in token_price_data:", token_id)
                continue
            if token_id not in token_config_data:
                print("token not in token_config_ret:", token_id)
                continue
            usd_token_decimal = token_price_data[token_id]["decimal"] + token_config_data[token_id]
            burrow_total_fee += (int(burrow_fee_log["interest"]) / int("1" + "0" * usd_token_decimal)) * float(token_price_data[token_id]["price"])
            burrow_total_revenue += ((int(burrow_fee_log["prot_fee"]) + int(burrow_fee_log["reserved"])) / int("1" + "0" * usd_token_decimal)) * float(token_price_data[token_id]["price"])
            log_id_list.append(burrow_fee_log["id"])

        conn = RedisProvider()
        conn.begin_pipe()
        conn.add_burrow_total_fee(burrow_total_fee)
        conn.add_burrow_total_revenue(burrow_total_revenue)
        conn.end_pipe()
        conn.close()
        if len(log_id_list) > 0:
            update_burrow_fee_log_data(network_id, log_id_list)


def handel_burrow_fee_log_24h(network_id):
    log_id_list = []
    price_url = "https://api.ref.finance/list-token-price"
    token_price_data = requests.get(price_url).text
    token_price_data = json.loads(token_price_data)
    config_url = "https://api.burrow.finance/get_assets_paged_detailed"
    token_config_ret = requests.get(config_url).text
    token_config_ret = json.loads(token_config_ret)
    if token_config_ret["code"] == "0":
        token_config_data = {}
        for token_config in token_config_ret["data"]:
            token_config_data[token_config["token_id"]] = token_config["config"]["extra_decimals"]
        now_time = int(time.time())
        old_time = (now_time - (24 * 60 * 60)) * 1000000000
        burrow_fee_log_list = get_burrow_fee_log_24h_data(network_id, old_time)
        burrow_total_fee = 0.0
        burrow_total_revenue = 0.0
        for burrow_fee_log in burrow_fee_log_list:
            token_id = burrow_fee_log["token_id"]
            if token_id not in token_price_data:
                print("token not in token_price_data:", token_id)
                continue
            if token_id not in token_config_data:
                print("token not in token_config_ret:", token_id)
                continue
            usd_token_decimal = token_price_data[token_id]["decimal"] + token_config_data[token_id]
            burrow_total_fee += (int(burrow_fee_log["interest"]) / int("1" + "0" * usd_token_decimal)) * float(token_price_data[token_id]["price"])
            burrow_total_revenue += ((int(burrow_fee_log["prot_fee"]) + int(burrow_fee_log["reserved"])) / int("1" + "0" * usd_token_decimal)) * float(token_price_data[token_id]["price"])
            log_id_list.append(burrow_fee_log["id"])

        conn = RedisProvider()
        conn.begin_pipe()
        conn.add_burrow_total_fee(burrow_total_fee)
        conn.add_burrow_total_revenue(burrow_total_revenue)
        conn.end_pipe()
        conn.close()
        if len(log_id_list) > 0:
            update_burrow_fee_log_data(network_id, log_id_list)


def handel_cross_chain_fee(network_id, incremental_fee=0.0, incremental_revenue=0.0, is_first_run=False):
    """
    处理 cross chain fee，保存到 Redis，同时计算24小时收益数据

    Args:
        network_id: 网络 ID
        incremental_fee: 本次增量费用（USD）
        incremental_revenue: 本次增量收入（USD）
        is_first_run: 是否是首次运行（全量数据）
    """
    current_timestamp = int(time.time())
    
    # 1. 处理历史总收益（原有逻辑）
    if is_first_run:
        # 首次运行：从 JSON 文件读取所有数据计算 total
        if os.path.exists(INTENTS_JSON_FILE):
            try:
                with open(INTENTS_JSON_FILE, 'r', encoding='utf-8') as f:
                    all_txs = json.load(f)

                # 计算全量费用和收入
                total_fee = 0.0
                for tx in all_txs:
                    if tx.get('status') == 'SUCCESS' and tx.get('fee_amount_usd'):
                        total_fee += float(tx['fee_amount_usd'])

                total_revenue = total_fee * REVENUE_PERCENTAGE

                print(
                    f"First run: calculated total_fee=${total_fee:.2f}, total_revenue=${total_revenue:.2f} from {len(all_txs)} transactions")
            except Exception as e:
                print(f"Error reading JSON file for first run: {e}")
                total_fee = incremental_fee
                total_revenue = incremental_revenue
        else:
            # JSON 文件不存在，使用增量值
            total_fee = incremental_fee
            total_revenue = incremental_revenue
    else:
        # 后续运行：从 Redis 读取现有值，加上增量
        existing_fee = get_cross_chain_total_fee()
        existing_revenue = get_cross_chain_total_revenue()

        if existing_fee is None:
            existing_fee = 0.0
        else:
            existing_fee = float(existing_fee)

        if existing_revenue is None:
            existing_revenue = 0.0
        else:
            existing_revenue = float(existing_revenue)

        total_fee = existing_fee + incremental_fee
        total_revenue = existing_revenue + incremental_revenue

        print(
            f"Incremental update: existing_fee=${existing_fee:.2f}, incremental_fee=${incremental_fee:.2f}, new_total_fee=${total_fee:.2f}")
        print(
            f"Incremental update: existing_revenue=${existing_revenue:.2f}, incremental_revenue=${incremental_revenue:.2f}, new_total_revenue=${total_revenue:.2f}")

    # 2. 处理24小时收益数据（直接计算过去24小时的交易）
    redis_conn = RedisProvider()
    
    # 计算24小时前的时间戳（直接用当前时间减去24小时）
    cutoff_timestamp = current_timestamp - 24 * 3600
    
    # 从 JSON 文件筛选24小时内的交易
    fee_24h = 0.0
    revenue_24h = 0.0
    
    if os.path.exists(INTENTS_JSON_FILE):
        try:
            with open(INTENTS_JSON_FILE, 'r', encoding='utf-8') as f:
                all_txs = json.load(f)
            
            # 筛选过去24小时内的交易（基于 created_at_timestamp）
            txs_24h = []
            for tx in all_txs:
                tx_timestamp = int(tx.get('created_at_timestamp', 0))
                if tx_timestamp >= cutoff_timestamp and tx.get('status') == 'SUCCESS' and tx.get('fee_amount_usd'):
                    txs_24h.append(tx)
            
            # 计算24小时内的费用和收入
            for tx in txs_24h:
                fee_24h += float(tx['fee_amount_usd'])
            
            revenue_24h = fee_24h * REVENUE_PERCENTAGE
            
            print(f"24h data calculated: fee_24h=${fee_24h:.2f}, revenue_24h=${revenue_24h:.2f}, "
                  f"txs_count={len(txs_24h)}, cutoff_timestamp={cutoff_timestamp}")
            
        except Exception as e:
            print(f"Error reading JSON file for 24h calculation: {e}")
            fee_24h = 0.0
            revenue_24h = 0.0
    else:
        # JSON 文件不存在
        print("JSON file not found for 24h calculation")
        fee_24h = 0.0
        revenue_24h = 0.0
    
    # 3. 使用 pipeline 批量写入所有数据到 Redis
    redis_conn.begin_pipe()
    
    # 保存历史总收益（用于其他接口）
    redis_conn.add_cross_chain_total_fee(str(total_fee))
    redis_conn.add_cross_chain_total_revenue(str(total_revenue))
    
    # 保存24小时收益
    redis_conn.pipe.set("CROSS_CHAIN_TOTAL_FEE_24H", str(fee_24h))
    redis_conn.pipe.set("CROSS_CHAIN_TOTAL_REVENUE_24H", str(revenue_24h))
    
    redis_conn.end_pipe()
    redis_conn.close()

    print(
        f"Saved to Redis: cross_chain_total_fee=${total_fee:.2f}, cross_chain_total_revenue=${total_revenue:.2f}, "
        f"cross_chain_fee_24h=${fee_24h:.2f}, cross_chain_revenue_24h=${revenue_24h:.2f}")


def delay(seconds):
    """延迟函数"""
    time.sleep(seconds)


def get_intents_tokens_metadata():
    """获取 Intents tokens 元数据"""
    try:
        response = requests.get(TOKENS_METADATA_URL, headers={"Accept": "*/*"}, timeout=30)
        response.raise_for_status()
        tokens_metadata = response.json()
        print(f"Tokens metadata loaded successfully, contain: {len(tokens_metadata)} records")
        return tokens_metadata
    except Exception as e:
        print(f"Error fetching Intents tokens metadata: {e}")
        return None


def normalize_symbol(symbol):
    """标准化 token symbol（WNEAR -> NEAR, WETH -> ETH, WBTC -> BTC）"""
    symbol_upper = symbol.upper()
    if symbol_upper == 'WNEAR':
        return 'NEAR'
    elif symbol_upper == 'WETH':
        return 'ETH'
    elif symbol_upper == 'WBTC':
        return 'BTC'
    return symbol


def determine_tx_type(origin_symbol, origin_blockchain, dest_symbol, dest_blockchain):
    """确定交易类型"""
    if origin_symbol == dest_symbol and origin_blockchain == dest_blockchain:
        return 'Transfer'
    elif origin_symbol == dest_symbol and origin_blockchain != dest_blockchain:
        return 'Bridge'
    elif origin_symbol != dest_symbol and origin_blockchain == dest_blockchain:
        return 'Swap'
    elif origin_symbol != dest_symbol and origin_blockchain != dest_blockchain:
        return 'Xswap'
    return 'Unknown'


def process_tx_data(tx, tokens_metadata):
    """处理单笔交易数据"""
    # 检查是否有费用支付给 Rhea
    if not tx.get('appFees') or len(tx['appFees']) == 0:
        return None

    app_fee = tx['appFees'][0]
    if (app_fee.get('recipient') != RHEA_FEE_RECIPIENT or
            app_fee.get('fee') is None or
            app_fee.get('fee') < 0):
        return None

    # 获取 token metadata
    origin_asset = tx.get('originAsset', '')
    destination_asset = tx.get('destinationAsset', '')

    origin_metadata = next((t for t in tokens_metadata if t.get('assetId') == origin_asset), None)
    dest_metadata = next((t for t in tokens_metadata if t.get('assetId') == destination_asset), None)

    if not origin_metadata or not dest_metadata:
        print(f"Missing metadata for tx: {tx.get('depositAddress')}")
        return None

    origin_symbol = normalize_symbol(origin_metadata.get('symbol', ''))
    dest_symbol = normalize_symbol(dest_metadata.get('symbol', ''))
    origin_blockchain = origin_metadata.get('blockchain', '')
    dest_blockchain = dest_metadata.get('blockchain', '')

    tx_type = determine_tx_type(origin_symbol, origin_blockchain, dest_symbol, dest_blockchain)

    # 计算费用（基于 amountInUsd，fee_percent 是基点单位）
    fee_percent = app_fee.get('fee', 0)
    amount_in_usd = float(tx.get('amountInUsd', 0))
    fee_amount_usd = amount_in_usd * (fee_percent / 10000) if amount_in_usd > 0 else 0

    return {
        'created_at_timestamp': tx.get('createdAtTimestamp'),
        'status': tx.get('status'),
        'tx_type': tx_type,
        'trader': tx.get('refundTo'),
        'origin_asset_blockchain': origin_blockchain,
        'origin_asset_symbol': origin_symbol,
        'amount_in_formatted': tx.get('amountInFormatted'),
        'amount_in_usd': tx.get('amountInUsd'),
        'destination_asset_blockchain': dest_blockchain,
        'destination_asset_symbol': dest_symbol,
        'amount_out_formatted': tx.get('amountOutFormatted'),
        'amount_out_usd': tx.get('amountOutUsd'),
        'fee_recipient': app_fee.get('recipient'),
        'fee_percent': fee_percent,
        'fee_amount_usd': fee_amount_usd,
        'deposit_address': tx.get('depositAddress')
    }


def get_last_deposit_address():
    """从文件读取上次的最后一个 deposit address"""
    if os.path.exists(LAST_DEPOSIT_ADDRESS_FILE):
        try:
            with open(LAST_DEPOSIT_ADDRESS_FILE, 'r') as f:
                return f.read().strip()
        except Exception as e:
            print(f"Error reading last deposit address: {e}")
    return None


def save_last_deposit_address(deposit_address):
    """保存最后一个 deposit address 到文件"""
    try:
        with open(LAST_DEPOSIT_ADDRESS_FILE, 'w') as f:
            f.write(deposit_address)
    except Exception as e:
        print(f"Error saving last deposit address: {e}")


def get_txs_in_time_range(intents_api_key, days=2):
    """获取指定天数内的交易（用于重新解析状态可能变化的交易）"""
    txs_list = []
    cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    start_filter_options = f"?numberOfTransactions=1000&referral=rhea&statuses=SUCCESS,FAILED,REFUNDED&direction=prev"
    last_deposit_address = ''
    need_next_run = True
    filter_options = start_filter_options

    while need_next_run:
        delay(5)  # API 要求至少 5 秒延迟

        if last_deposit_address:
            filter_options = f"{start_filter_options}&lastDepositAddress={last_deposit_address}"

        try:
            api_url = f"{INTENTS_API_BASE_URL}/transactions{filter_options}"
            response = requests.get(
                api_url,
                headers={
                    "Authorization": f"Bearer {intents_api_key}",
                    "Accept": "*/*"
                },
                timeout=30
            )
            response.raise_for_status()
            txs_batch = response.json()

            if not txs_batch:
                break

            # 检查是否还有更早的交易需要获取
            oldest_tx_timestamp = min(tx.get('createdAtTimestamp', 0) for tx in txs_batch)
            if oldest_tx_timestamp < cutoff_timestamp:
                # 只保留在时间范围内的交易
                txs_list.extend([tx for tx in txs_batch if tx.get('createdAtTimestamp', 0) >= cutoff_timestamp])
                break

            txs_list.extend(txs_batch)

            if len(txs_batch) == 1000:
                last_deposit_address = txs_batch[0].get('depositAddress')
                print(f"Fetched 1000 TXs, continuing...")
            else:
                need_next_run = False

        except Exception as e:
            print(f"Error fetching Intents transactions: {e}")
            break

    print(f"Fetched {len(txs_list)} transactions from last {days} days")
    return txs_list


def fetch_new_txs(intents_api_key, start_from_address=None):
    """获取新交易（从指定的 deposit address 开始，或从最新开始）"""
    txs_list = []

    if start_from_address:
        start_filter_options = f"?numberOfTransactions=1000&referral=rhea&statuses=SUCCESS,FAILED,REFUNDED&direction=prev&lastDepositAddress={start_from_address}"
    else:
        start_filter_options = f"?numberOfTransactions=1000&referral=rhea&statuses=SUCCESS,FAILED,REFUNDED&direction=prev"

    last_deposit_address = start_from_address
    need_next_run = True
    filter_options = start_filter_options

    while need_next_run:
        delay(5)  # API 要求至少 5 秒延迟

        if last_deposit_address and last_deposit_address != start_from_address:
            filter_options = f"{start_filter_options.split('&lastDepositAddress')[0]}&lastDepositAddress={last_deposit_address}"

        try:
            api_url = f"{INTENTS_API_BASE_URL}/transactions{filter_options}"
            response = requests.get(
                api_url,
                headers={
                    "Authorization": f"Bearer {intents_api_key}",
                    "Accept": "*/*"
                },
                timeout=30
            )
            response.raise_for_status()
            txs_batch = response.json()

            if not txs_batch:
                break

            txs_list.extend(txs_batch)

            if len(txs_batch) == 1000:
                last_deposit_address = txs_batch[0].get('depositAddress')
                print(f"Fetched 1000 TXs, continuing...")
            else:
                need_next_run = False
                if txs_batch:
                    last_deposit_address = txs_batch[0].get('depositAddress')

        except Exception as e:
            print(f"Error fetching Intents transactions: {e}")
            break

    print(f"Fetched {len(txs_list)} new transactions")
    return txs_list, last_deposit_address


def save_to_json(data, append_mode=False):
    """保存数据到 JSON 文件"""
    if not data:
        print("No data to save")
        return

    file_exists = os.path.exists(INTENTS_JSON_FILE)

    try:
        if append_mode and file_exists:
            # 追加模式：读取现有数据，合并新数据
            with open(INTENTS_JSON_FILE, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)

            # 使用 deposit_address 作为唯一键，去重并更新
            existing_dict = {tx['deposit_address']: tx for tx in existing_data}
            for tx in data:
                existing_dict[tx['deposit_address']] = tx

            all_data = list(existing_dict.values())
            # 按时间戳排序
            all_data.sort(key=lambda x: int(x.get('created_at_timestamp', 0)))
        else:
            # 首次写入或覆盖模式
            all_data = data
            # 按时间戳排序
            all_data.sort(key=lambda x: int(x.get('created_at_timestamp', 0)))

        with open(INTENTS_JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)

        print(f"Saved {len(all_data)} transactions to {INTENTS_JSON_FILE}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")


def calculate_fee_statistics(txs_data):
    """计算费用统计信息"""
    total_fee = 0.0
    total_revenue = 0.0
    success_count = 0

    for tx in txs_data:
        if tx.get('status') == 'SUCCESS' and tx.get('fee_amount_usd'):
            fee_amount = float(tx['fee_amount_usd'])
            total_fee += fee_amount
            success_count += 1

    total_revenue = total_fee * REVENUE_PERCENTAGE

    return {
        'total_fee': total_fee,
        'total_revenue': total_revenue,
        'success_count': success_count,
        'total_count': len(txs_data)
    }


def handel_rhea_intents_txs(intents_api_key, reparse_days=2, append_mode=True, network_id=None):
    """
    处理 Rhea Intents 交易数据

    Args:
        intents_api_key: Intents API key
        reparse_days: 重新解析最近几天的交易（默认2天）
        append_mode: 是否使用追加模式（从上次的地址继续）
        network_id: 网络 ID（用于保存费用统计）
    """
    start_time = time.time()
    print("Starting Rhea Intents transactions parsing")

    # 1. 获取 tokens metadata
    tokens_metadata = get_intents_tokens_metadata()
    if not tokens_metadata:
        print("Failed to get tokens metadata, exiting")
        return

    all_txs_to_save = []
    last_deposit_address = None

    # 2. 追加模式：获取新交易
    if append_mode:
        last_deposit_address = get_last_deposit_address()
        if last_deposit_address:
            print(f"Append mode: starting from last deposit address: {last_deposit_address}")
            new_txs, new_last_address = fetch_new_txs(intents_api_key, last_deposit_address)
            if new_txs:
                processed_new_txs = []
                for tx in new_txs:
                    processed_tx = process_tx_data(tx, tokens_metadata)
                    if processed_tx:
                        processed_new_txs.append(processed_tx)

                if processed_new_txs:
                    all_txs_to_save.extend(processed_new_txs)
                    save_to_json(processed_new_txs, append_mode=True)
                    # 保存最新的地址（用于下次追加）
                    if new_last_address:
                        save_last_deposit_address(new_last_address)
                    elif processed_new_txs:
                        # 如果没有新地址，使用最后一条交易的地址
                        save_last_deposit_address(processed_new_txs[-1]['deposit_address'])
                    print(f"Added {len(processed_new_txs)} new transactions")
            else:
                print("No new transactions found")
        else:
            print("No last deposit address found, will do full parse")
            append_mode = False

    # 3. 如果非追加模式，进行全量解析
    if not append_mode:
        print("Full parse mode: fetching all transactions")
        all_txs, last_address = fetch_new_txs(intents_api_key, None)
        if all_txs:
            processed_all_txs = []
            for tx in all_txs:
                processed_tx = process_tx_data(tx, tokens_metadata)
                if processed_tx:
                    processed_all_txs.append(processed_tx)

            if processed_all_txs:
                # 按时间戳排序（从旧到新）
                processed_all_txs.sort(key=lambda x: x['created_at_timestamp'])
                save_to_json(processed_all_txs, append_mode=False)
                if last_address:
                    save_last_deposit_address(last_address)
                elif processed_all_txs:
                    save_last_deposit_address(processed_all_txs[-1]['deposit_address'])
                print(f"Saved {len(processed_all_txs)} transactions (full parse)")
                all_txs_to_save = processed_all_txs

    # 4. 重新解析最近 N 天的交易（因为状态可能变化）
    print(f"Reparsing transactions from last {reparse_days} days")
    recent_txs = get_txs_in_time_range(intents_api_key, days=reparse_days)

    if recent_txs:
        # 创建 deposit_address 到交易数据的映射（用于去重和更新）
        recent_txs_map = {}
        for tx in recent_txs:
            processed_tx = process_tx_data(tx, tokens_metadata)
            if processed_tx:
                deposit_addr = processed_tx['deposit_address']
                # 如果已存在，保留最新的（状态可能已更新）
                if deposit_addr not in recent_txs_map or processed_tx['created_at_timestamp'] > \
                        recent_txs_map[deposit_addr]['created_at_timestamp']:
                    recent_txs_map[deposit_addr] = processed_tx

        recent_txs_list = list(recent_txs_map.values())

        if recent_txs_list:
            # 如果 JSON 文件存在，更新已存在的记录（因为状态可能已变化）
            if os.path.exists(INTENTS_JSON_FILE):
                # 读取现有 JSON
                existing_txs = {}
                try:
                    with open(INTENTS_JSON_FILE, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        for row in existing_data:
                            existing_txs[row['deposit_address']] = row
                except Exception as e:
                    print(f"Error reading existing JSON: {e}")
                    existing_txs = {}

                # 更新已存在的记录（状态可能已变化）
                updated_count = 0
                for tx in recent_txs_list:
                    if tx['deposit_address'] in existing_txs:
                        existing_txs[tx['deposit_address']] = tx
                        updated_count += 1
                    else:
                        # 新记录（不在现有 JSON 中）
                        existing_txs[tx['deposit_address']] = tx

                # 重新写入 JSON（按时间戳排序）
                all_txs = list(existing_txs.values())
                all_txs.sort(key=lambda x: int(x.get('created_at_timestamp', 0)))
                save_to_json(all_txs, append_mode=False)
                print(
                    f"Updated {updated_count} existing transactions from last {reparse_days} days, total: {len(all_txs)}")
            else:
                # JSON 文件不存在：直接保存
                recent_txs_list.sort(key=lambda x: x['created_at_timestamp'])
                save_to_json(recent_txs_list, append_mode=False)
                print(f"Saved {len(recent_txs_list)} transactions from last {reparse_days} days")

    # 5. 计算统计信息（基于本次处理的交易）
    stats_data = []
    if 'recent_txs_list' in locals() and recent_txs_list:
        stats_data = recent_txs_list
    elif all_txs_to_save:
        stats_data = all_txs_to_save

    if stats_data:
        stats = calculate_fee_statistics(stats_data)
        print(
            f"Statistics (this run) - Total Fee: ${stats['total_fee']:.2f}, Total Revenue: ${stats['total_revenue']:.2f}, "
            f"Success TXs: {stats['success_count']}/{stats['total_count']}")

    # 6. 计算全量统计信息（从 JSON 文件读取所有数据）
    incremental_fee = 0.0
    incremental_revenue = 0.0
    is_first_run = not os.path.exists(INTENTS_JSON_FILE)

    # 计算本次新增交易的费用（只统计新增的，不统计重新解析的）
    if all_txs_to_save:
        new_txs_stats = calculate_fee_statistics(all_txs_to_save)
        incremental_fee = new_txs_stats['total_fee']
        incremental_revenue = new_txs_stats['total_revenue']
        print(f"New transactions statistics - Fee: ${incremental_fee:.2f}, Revenue: ${incremental_revenue:.2f}, "
                    f"Success TXs: {new_txs_stats['success_count']}/{new_txs_stats['total_count']}")

    if os.path.exists(INTENTS_JSON_FILE):
        try:
            with open(INTENTS_JSON_FILE, 'r', encoding='utf-8') as f:
                all_json_txs = json.load(f)

            if all_json_txs:
                total_stats = calculate_fee_statistics(all_json_txs)
                print(
                    f"Statistics (all time) - Total Fee: ${total_stats['total_fee']:.2f}, Total Revenue: ${total_stats['total_revenue']:.2f}, "
                    f"Success TXs: {total_stats['success_count']}/{total_stats['total_count']}")
        except Exception as e:
            print(f"Error calculating total statistics: {e}")
    else:
        # JSON 文件不存在，使用本次处理的统计作为全量
        if stats_data:
            total_stats = calculate_fee_statistics(stats_data)
            print(
                f"Statistics (first run) - Total Fee: ${total_stats['total_fee']:.2f}, Total Revenue: ${total_stats['total_revenue']:.2f}, "
                f"Success TXs: {total_stats['success_count']}/{total_stats['total_count']}")
            is_first_run = True

    # 7. 保存费用统计到 Redis
    if network_id:
        handel_cross_chain_fee(network_id, incremental_fee, incremental_revenue, is_first_run)

    # 8. 打印执行时间
    elapsed_time = time.time() - start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)
    print(
        f"Rhea Intents transactions parsing completed. Total execution time: {hours}h {minutes}m {seconds}s ({elapsed_time:.2f} seconds)")


def handel_lst_fee(network_id):
    """
    处理 LST fee，按照方案2实现：
    - 存储数量（不乘以价格）
    - 计算24小时增长数量后乘以当前价格得到24小时收益
    - 每10分钟执行一次，正确维护24小时前的数量
    """
    try:
        # 1. 获取当前数量（不乘以价格）
        conn = MultiNodeJsonProvider(network_id)
        ret = conn.view_call("lst.rhealab.near", "get_account", '{"account_id": "ref-finance.sputnik-dao.near"}'.encode(encoding='utf-8'))
        json_str = "".join([chr(x) for x in ret["result"]])
        account_data = json.loads(json_str)
        print("ref-finance.sputnik-dao.near account_data:", account_data)
        
        # 计算当前数量（扣除初始值300000）
        current_quantity = int(account_data["staked_balance"]) / 10**24 - 300000
        current_timestamp = int(time.time())
        
        # 2. 获取当前价格（用于计算历史总值和24小时收益）
        price_url = "https://api.ref.finance/get-token-price?token_id=lst.rhealab.near"
        token_price_data = requests.get(price_url).text
        token_price_data = json.loads(token_price_data)
        current_price = float(token_price_data["price"])
        
        # 3. 计算历史总收益（用于其他接口）
        total_fee = current_quantity * current_price
        
        # 4. 使用历史快照计算24小时收益（使用 ZSET 存储历史快照）
        redis_conn = RedisProvider()
        
        # 计算24小时前的时间戳
        cutoff_timestamp = current_timestamp - 24 * 3600
        
        # 5. 先查询24小时前的快照（在记录当前快照之前查询，避免查询到刚记录的快照）
        # 查找最接近24小时前的快照，允许±10分钟误差
        search_start = cutoff_timestamp - 600  # 24小时前 - 10分钟
        search_end = cutoff_timestamp + 600    # 24小时前 + 10分钟
        
        snapshots = redis_conn.r.zrangebyscore("LST_QUANTITY_SNAPSHOTS", search_start, search_end, start=0, num=1, withscores=True)
        
        if snapshots and len(snapshots) > 0:
            # 找到最接近24小时前的快照
            quantity_24h_ago_str, snapshot_timestamp = snapshots[0]
            quantity_24h_ago = float(quantity_24h_ago_str)
            actual_time_diff = current_timestamp - snapshot_timestamp
            
            # 计算24小时增长数量
            delta_quantity = current_quantity - quantity_24h_ago
            fee_24h = delta_quantity * current_price
            
            print(f"24h data calculated: delta_quantity={delta_quantity:.2f}, fee_24h=${fee_24h:.2f}, "
                  f"snapshot_timestamp={int(snapshot_timestamp)}, actual_time_diff={actual_time_diff/3600:.2f}h")
        else:
            # 没有找到24小时前的快照（首次运行或数据不足）
            print("No 24h ago snapshot found, setting fee_24h=0 (first run or insufficient data)")
            fee_24h = 0.0
        
        # 6. 使用 pipeline 批量写入所有数据到 Redis
        redis_conn.begin_pipe()
        
        # 保存历史总收益（用于其他接口）
        redis_conn.add_lst_total_fee(str(total_fee))
        redis_conn.add_lst_total_revenue(str(total_fee))
        
        # 保存24小时收益
        redis_conn.pipe.set("LST_TOTAL_FEE_24H", str(fee_24h))
        
        # 记录当前快照到 ZSET（使用时间戳作为 score，数量作为 value）
        # ZSET key: LST_QUANTITY_SNAPSHOTS, score: timestamp, value: quantity
        redis_conn.pipe.zadd("LST_QUANTITY_SNAPSHOTS", {str(current_quantity): current_timestamp})
        
        # 清理7天前的历史快照（节省存储空间）
        # 只保留最近7天的数据，7天前的数据对24小时计算已经没有意义
        cleanup_timestamp = current_timestamp - 7 * 24 * 3600
        redis_conn.pipe.zremrangebyscore("LST_QUANTITY_SNAPSHOTS", 0, cleanup_timestamp)
        
        redis_conn.end_pipe()
        redis_conn.close()
        
        print(f"LST fee updated: current_quantity={current_quantity:.2f}, current_price=${current_price:.2f}, "
              f"total_fee=${total_fee:.2f}, fee_24h=${fee_24h:.2f}")
        
    except Exception as e:
        print(f"Error in handel_lst_fee: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    print("start burrow fee log task")
    if len(sys.argv) == 2:
        network_id = str(sys.argv[1]).upper()
        if network_id in ["MAINNET", "TESTNET", "DEVNET"]:
            # handel_burrow_fee_log(network_id)
            handel_burrow_fee_log_24h(network_id)
            handel_lst_fee(network_id)
            intents_api_key = Cfg.INTENTS_API_KEY
            print("intents_api_key:", intents_api_key)
            handel_rhea_intents_txs(intents_api_key, reparse_days=2, append_mode=True, network_id=network_id)
        else:
            print("Error, network_id should be MAINNET, TESTNET or DEVNET")
            exit(1)
    else:
        print("Error, must put NETWORK_ID as arg")
        exit(1)
    print("end burrow fee log task")
