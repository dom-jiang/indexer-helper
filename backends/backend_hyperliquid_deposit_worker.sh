#!/bin/sh

pid=`ps -ef | grep "hyperliquid_deposit_worker.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_hyperliquid_deposit_worker.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No stale worker process." >> backend_hyperliquid_deposit_worker.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: killed previous hyperliquid_deposit_worker." >> backend_hyperliquid_deposit_worker.log
fi
/usr/local/bin/python hyperliquid_deposit_worker.py MAINNET >> backend_hyperliquid_deposit_worker.log
echo 'OK'
