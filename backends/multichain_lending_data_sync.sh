#!/bin/sh

pid=`ps -ef | grep "multichain_lending_data_sync.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_multichain_lending_data_sync.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_multichain_lending_data_sync.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_multichain_lending_data_sync.log
fi
/usr/local/bin/python multichain_lending_data_sync.py MAINNET >> backend_multichain_lending_data_sync.log
echo 'OK'