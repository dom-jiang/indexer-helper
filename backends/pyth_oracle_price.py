import json
import requests
import time
import sys
from loguru import logger
sys.path.append('../')
from db_provider import get_db_connect


# Token ID 到 Symbol 的映射
TOKEN_ID_MAP = {
    "ded2a0d2624278a32c56725397cc98b24ddb83d8c4d2ce108b1fc44b1d8de22b": "RHEA",
    "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace": "ETH",
    "c415de8d2eba7db216527dff4b60e8f3a5311c740dadb233e13e12547e226750": "NEAR",
    "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d": "SOL",
    "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43": "BTC"
}

# Pyth API URL
PYTH_API_URL = "https://hermes.pyth.network/v2/updates/price/latest"


def create_pyth_price_table(network_id):
    """创建 pyth 价格数据表"""
    db_conn = get_db_connect(network_id)
    cursor = db_conn.cursor()
    try:
        sql = """
        CREATE TABLE IF NOT EXISTS pyth_oracle_price (
            id INT AUTO_INCREMENT PRIMARY KEY,
            token_id VARCHAR(64) NOT NULL COMMENT 'Token ID',
            symbol VARCHAR(20) NOT NULL COMMENT 'Token Symbol',
            price DECIMAL(30, 8) NOT NULL COMMENT 'Price',
            conf DECIMAL(20, 8) DEFAULT NULL COMMENT 'Confidence',
            expo INT DEFAULT NULL COMMENT 'Exponent',
            publish_time BIGINT DEFAULT NULL COMMENT 'Publish Time',
            ema_price DECIMAL(30, 8) DEFAULT NULL COMMENT 'EMA Price',
            ema_conf DECIMAL(20, 8) DEFAULT NULL COMMENT 'EMA Confidence',
            slot BIGINT DEFAULT NULL COMMENT 'Slot',
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Created Time',
            INDEX idx_token_id (token_id),
            INDEX idx_symbol (symbol),
            INDEX idx_publish_time (publish_time),
            INDEX idx_created_time (created_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Pyth Oracle Price Data'
        """
        cursor.execute(sql)
        db_conn.commit()
        logger.info("Table pyth_oracle_price created or already exists")
    except Exception as e:
        db_conn.rollback()
        logger.error(f"Error creating table: {e}")
        raise e
    finally:
        cursor.close()
        db_conn.close()


def fetch_pyth_price_data():
    """从 Pyth API 获取价格数据"""
    try:
        # 构建请求 URL，包含所有需要的 token IDs
        token_ids = list(TOKEN_ID_MAP.keys())
        ids_param = "&".join([f"ids%5B%5D={token_id}" for token_id in token_ids])
        url = f"{PYTH_API_URL}?{ids_param}"
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        ret_data = response.text
        tokens_data = json.loads(ret_data)
        return tokens_data
    except Exception as e:
        logger.error(f"Error fetching pyth price data: {e}")
        return None


def save_pyth_price_data(network_id, price_data):
    """保存价格数据到数据库"""
    if not price_data or "parsed" not in price_data:
        logger.warning("No parsed data in response")
        return
    
    db_conn = get_db_connect(network_id)
    cursor = db_conn.cursor()
    
    try:
        parsed_data = price_data["parsed"]
        insert_count = 0
        
        for item in parsed_data:
            token_id = item.get("id", "")
            if token_id not in TOKEN_ID_MAP:
                continue
            
            symbol = TOKEN_ID_MAP[token_id]
            price_info = item.get("price", {})
            ema_price_info = item.get("ema_price", {})
            metadata = item.get("metadata", {})
            
            price = price_info.get("price", "0")
            conf = price_info.get("conf", "0")
            expo = price_info.get("expo", -8)
            publish_time = price_info.get("publish_time", 0)
            
            ema_price = ema_price_info.get("price", "0") if ema_price_info else "0"
            ema_conf = ema_price_info.get("conf", "0") if ema_price_info else "0"
            slot = metadata.get("slot", 0) if metadata else 0
            
            # 计算实际价格：price * 10^expo
            try:
                actual_price = float(price) * (10 ** int(expo))
                actual_conf = float(conf) * (10 ** int(expo)) if conf else 0
                actual_ema_price = float(ema_price) * (10 ** int(expo)) if ema_price else 0
                actual_ema_conf = float(ema_conf) * (10 ** int(expo)) if ema_conf else 0
            except (ValueError, TypeError) as e:
                logger.warning(f"Error calculating price for {symbol}: {e}")
                continue
            
            sql = """
            INSERT INTO pyth_oracle_price 
            (token_id, symbol, price, conf, expo, publish_time, ema_price, ema_conf, slot)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(sql, (
                token_id,
                symbol,
                actual_price,
                actual_conf,
                expo,
                publish_time,
                actual_ema_price,
                actual_ema_conf,
                slot
            ))
            insert_count += 1
        
        db_conn.commit()
        logger.info(f"Successfully saved {insert_count} price records")
    except Exception as e:
        db_conn.rollback()
        logger.error(f"Error saving price data: {e}")
        raise e
    finally:
        cursor.close()
        db_conn.close()


def run_pyth_price_fetcher(network_id="MAINNET", interval=20):
    """运行价格获取任务，每 interval 秒执行一次"""
    logger.info(f"Starting Pyth price fetcher for {network_id}, interval: {interval}s")
    
    # 创建表
    create_pyth_price_table(network_id)
    
    while True:
        try:
            logger.info("Fetching pyth price data...")
            price_data = fetch_pyth_price_data()
            
            if price_data:
                save_pyth_price_data(network_id, price_data)
            else:
                logger.warning("Failed to fetch price data")
            
            logger.info(f"Waiting {interval} seconds before next fetch...")
            time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Pyth price fetcher stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in price fetcher loop: {e}")
            logger.info(f"Waiting {interval} seconds before retry...")
            time.sleep(interval)


if __name__ == "__main__":
    # 默认每20秒执行一次
    run_pyth_price_fetcher(interval=20)
