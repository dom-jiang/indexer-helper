#!/bin/sh

pid=`ps -ef | grep "dcl_points.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_dcl_points.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_dcl_points.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_dcl_points.log
fi
/usr/local/bin/python dcl_points.py MAINNET >> backend_dcl_points.log
echo 'OK'
