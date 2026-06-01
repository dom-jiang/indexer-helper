"""
Gunicorn hooks for indexer-helper API.

TRXX pending-order polling must run in exactly one worker when using
``-w N``. Importing app.py in every worker must NOT start the scheduler;
``post_fork`` calls ``start_trxx_scheduler_once()`` instead.
"""

import logging

_log = logging.getLogger("gunicorn.error")


def post_fork(server, worker):
    try:
        from trxx_utils import start_trxx_scheduler_once

        if start_trxx_scheduler_once():
            _log.info("TRXX scheduler started in worker pid=%s", worker.pid)
    except Exception as e:
        _log.warning("TRXX scheduler failed to start in worker pid=%s: %s", worker.pid, e)
