#!/bin/sh

pid=`ps -ef | grep "update_metadata.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_update_metadata.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_update_metadata.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_update_metadata.log
fi
/usr/local/bin/python update_metadata.py MAINNET >> backend_update_metadata.log
echo 'OK'