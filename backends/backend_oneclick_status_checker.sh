#!/bin/sh
#
# Cron wrapper: run oneclick status poll only when no worker is already running.
# Polling logic: last 70 minutes + batch limit — see backends/oneclick_status_checker.py
# and db_provider.get_pending_oneclick_orders.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$SCRIPT_DIR/backend_oneclick_status_checker.log"

# Match the python command line cron uses ([o] avoids matching grep itself).
existing_pid="$(ps -ef | grep '[o]neclick_status_checker.py MAINNET' | awk 'NR==1 {print $2}')"

if [ -n "$existing_pid" ]; then
	date >>"$LOG"
	echo "Skip: oneclick_status_checker.py already running (pid=$existing_pid)" >>"$LOG"
	exit 0
fi

cd "$SCRIPT_DIR" || exit 1
date >>"$LOG"

/usr/local/bin/python oneclick_status_checker.py MAINNET >>"$LOG"
echo OK >>"$LOG"
