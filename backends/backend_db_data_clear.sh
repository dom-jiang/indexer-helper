#!/bin/sh

pid=`ps -ef | grep "db_data_clear.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "indexer-helper/backends"

# echo ${pid}
date >> backend_db_data_clear.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_db_data_clear.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_db_data_clear.log
fi
/usr/local/bin/python db_data_clear.py MAINNET >> backend_db_data_clear.log
echo 'OK'