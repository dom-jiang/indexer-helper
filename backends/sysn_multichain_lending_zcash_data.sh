#!/bin/sh

pid=`ps -ef | grep "sysn_multichain_lending_zcash_data.py MAINNET" | grep -v grep | /usr/bin/awk '{print $2}'`

cd "/indexer-helper/backends"

# echo ${pid}
date >> backend_sysn_multichain_lending_zcash_data.log

if [ ! ${pid} ]; then
        # echo "is null"
        echo "No backend process rubbish to clean." >> backend_sysn_multichain_lending_zcash_data.log
else
        # echo "not null"
        kill -s 9 ${pid}
        echo "Warning: clean backend process of last round." >> backend_sysn_multichain_lending_zcash_data.log
fi
/usr/local/bin/python sysn_multichain_lending_zcash_data.py MAINNET >> backend_sysn_multichain_lending_zcash_data.log
echo 'OK'