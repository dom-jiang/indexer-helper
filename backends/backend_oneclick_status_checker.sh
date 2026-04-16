#!/bin/sh

pid=`ps -ef | grep "oneclick_status_checker.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

date >> backend_oneclick_status_checker.log

if [ ! ${pid} ]; then
        echo "No backend process rubbish to clean." >> backend_oneclick_status_checker.log
else
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_oneclick_status_checker.log
fi
/usr/local/bin/python token_price.py MAINNET >> backend_token_price.log
echo 'OK'
