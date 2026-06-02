#!/bin/sh

pid=`ps -ef | grep "backend_swap_status_checker.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

date >> backend_swap_status_checker.log

if [ ! ${pid} ]; then
        echo "No stale worker process." >> backend_swap_status_checker.log
else
        kill -s 9 ${pid}
        echo "Warning: killed previous backend_swap_status_checker." >> backend_swap_status_checker.log
fi
/usr/local/bin/python backend_swap_status_checker.py MAINNET >> backend_swap_status_checker.log
echo 'OK'
