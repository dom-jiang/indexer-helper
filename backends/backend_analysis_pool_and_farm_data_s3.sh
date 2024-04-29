#!/bin/sh

pid=`ps -ef | grep "analysis_pool_and_farm_data_s3.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_analysis_pool_and_farm_data_s3.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_analysis_pool_and_farm_data_s3.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_analysis_pool_and_farm_data_s3.log
fi
/usr/local/bin/python analysis_pool_and_farm_data_s3.py MAINNET >> backend_analysis_pool_and_farm_data_s3.log
echo 'OK'