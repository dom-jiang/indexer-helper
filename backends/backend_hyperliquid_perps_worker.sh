#!/bin/sh

pid=`ps -ef | grep "hyperliquid_perps_worker.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

date >> backend_hyperliquid_perps_worker.log

if [ ! ${pid} ]; then
        echo "No stale worker process." >> backend_hyperliquid_perps_worker.log
else
        kill -s 9 ${pid}
        echo "Warning: killed previous hyperliquid_perps_worker." >> backend_hyperliquid_perps_worker.log
fi
/usr/local/bin/python hyperliquid_perps_worker.py MAINNET >> backend_hyperliquid_perps_worker.log
echo 'OK'
