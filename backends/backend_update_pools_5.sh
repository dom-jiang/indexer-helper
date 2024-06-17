#!/bin/sh

pid=`ps -ef | grep "update_pools.py MAINNET 5000" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_update_pools.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_update_pools.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_update_pools.log
fi
/usr/local/bin/python update_pools.py MAINNET 5000 >> backend_update_pools.log
echo 'OK'