#!/bin/sh

pid=`ps -ef | grep "burrow_fee_log.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_burrow_fee_log.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_burrow_fee_log.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_burrow_fee_log.log
fi
/usr/local/bin/python burrow_fee_log.py MAINNET >> backend_burrow_fee_log.log
echo 'OK'
