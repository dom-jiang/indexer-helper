#!/bin/sh

pid=`ps -ef | grep "top_pools.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_top_pools.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_top_pools.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_top_pools.log
fi
python3 top_pools.py MAINNET >> backend_top_pools.log
echo 'OK'