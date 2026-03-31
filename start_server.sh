#!/bin/sh
. ./venv/bin/activate
gunicorn -w 4 --threads 8 --worker-class gthread --timeout 30 -b 0.0.0.0:8000 app:app