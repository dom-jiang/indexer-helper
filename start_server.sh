#!/bin/sh
. ./venv/bin/activate
exec gunicorn -c gunicorn.conf.py -w 4 --threads 8 --worker-class gthread --timeout 30 -b 0.0.0.0:8000 app:app