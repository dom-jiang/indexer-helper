#!/bin/sh

pid=`ps -ef | grep "mca_withdraw_job.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_mca_withdraw_job.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_mca_withdraw_job.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_mca_withdraw_job.log
fi
/usr/local/bin/python mca_withdraw_job.py MAINNET >> backend_mca_withdraw_job.log
echo 'OK'
