#!/bin/sh

pid=`ps -ef | grep "near_intents_status_checker.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_near_intents_status_checker.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_near_intents_status_checker.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_near_intents_status_checker.log
fi
/usr/local/bin/python near_intents_status_checker.py MAINNET >> backend_near_intents_status_checker.log
echo 'OK'
