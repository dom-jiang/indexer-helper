#!/bin/sh

pid=`ps -ef | grep "apy_report_collector.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

date >> backend_apy_report_collector.log

if [ ! ${pid} ]; then
        echo "No stale worker process." >> backend_apy_report_collector.log
else
        kill -s 9 ${pid}
        echo "Warning: killed previous apy_report_collector." >> backend_apy_report_collector.log
fi
/usr/local/bin/python apy_report_collector.py MAINNET >> backend_apy_report_collector.log
echo 'OK'
