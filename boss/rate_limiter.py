"""
Redis-based rate limiter for swap API endpoints.

Two dimensions:
  - Per-minute: sliding window per endpoint_group (quote / build)
  - Per-month: counter shared across all endpoint groups, resets on UTC month boundary
"""

import time
from datetime import datetime, timezone

import redis
from loguru import logger
from cachetools import TTLCache

from config import Cfg
from boss.models import get_rate_limits

_rate_limit_cache = TTLCache(maxsize=500, ttl=30)

_redis_pool = redis.ConnectionPool(
    host=Cfg.REDIS["REDIS_HOST"],
    port=int(Cfg.REDIS["REDIS_PORT"]),
    decode_responses=True,
    db=3,
)


def _get_redis():
    return redis.StrictRedis(connection_pool=_redis_pool)


def _current_utc_month_key() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year}{now.month:02d}"


def _seconds_until_next_minute() -> int:
    now = time.time()
    return 60 - int(now % 60) + 1


def _seconds_until_next_month() -> int:
    now = datetime.now(timezone.utc)
    if now.month == 12:
        next_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        next_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return int((next_month - now).total_seconds()) + 1


def get_cached_rate_limits(app_id: str, get_db_conn_func) -> dict:
    """
    Returns rate limit config as:
    {
      "quote": {"per_minute": 60, "per_month": 300000},
      "build": {"per_minute": 30, "per_month": 300000},
    }
    """
    cache_key = f"rl:{app_id}"
    cached = _rate_limit_cache.get(cache_key)
    if cached:
        return cached

    conn = get_db_conn_func()
    try:
        configs = get_rate_limits(conn, app_id)
    finally:
        conn.close()

    result = {}
    for cfg in configs:
        result[cfg["endpoint_group"]] = {
            "per_minute": cfg["per_minute"],
            "per_month": cfg["per_month"],
        }

    if not result:
        result = {
            "quote": {"per_minute": 60, "per_month": 300000},
            "build": {"per_minute": 30, "per_month": 300000},
        }

    _rate_limit_cache[cache_key] = result
    return result


def check_rate_limit(app_id: str, endpoint_group: str, get_db_conn_func) -> dict:
    """
    Check and increment rate limit counters.

    Returns:
      {"allowed": True/False, "reason": "...", "minute_remaining": N, "month_remaining": N}
    """
    limits = get_cached_rate_limits(app_id, get_db_conn_func)
    group_limits = limits.get(endpoint_group, limits.get("all", {"per_minute": 60, "per_month": 300000}))
    per_minute = group_limits["per_minute"]
    per_month = group_limits["per_month"]

    r = _get_redis()

    current_minute = int(time.time()) // 60
    minute_key = f"rl:min:{app_id}:{endpoint_group}:{current_minute}"
    month_key_suffix = _current_utc_month_key()
    month_key = f"rl:mon:{app_id}:{month_key_suffix}"

    pipe = r.pipeline()
    pipe.get(minute_key)
    pipe.get(month_key)
    minute_count_str, month_count_str = pipe.execute()

    minute_count = int(minute_count_str) if minute_count_str else 0
    month_count = int(month_count_str) if month_count_str else 0

    if minute_count >= per_minute:
        r.close()
        return {
            "allowed": False,
            "reason": f"Rate limit exceeded: {per_minute} requests/minute for {endpoint_group}",
            "minute_remaining": 0,
            "month_remaining": max(0, per_month - month_count),
        }

    if month_count >= per_month:
        r.close()
        return {
            "allowed": False,
            "reason": f"Monthly quota exceeded: {per_month} requests/month",
            "minute_remaining": max(0, per_minute - minute_count),
            "month_remaining": 0,
        }

    pipe = r.pipeline()
    pipe.incr(minute_key)
    pipe.expire(minute_key, _seconds_until_next_minute())
    pipe.incr(month_key)
    pipe.expire(month_key, _seconds_until_next_month())
    pipe.execute()
    r.close()

    return {
        "allowed": True,
        "reason": "",
        "minute_remaining": max(0, per_minute - minute_count - 1),
        "month_remaining": max(0, per_month - month_count - 1),
    }


def get_usage_stats(app_id: str) -> dict:
    """Get current usage stats for an app_id."""
    r = _get_redis()

    current_minute = int(time.time()) // 60
    month_key_suffix = _current_utc_month_key()

    quote_min_key = f"rl:min:{app_id}:quote:{current_minute}"
    build_min_key = f"rl:min:{app_id}:build:{current_minute}"
    month_key = f"rl:mon:{app_id}:{month_key_suffix}"

    pipe = r.pipeline()
    pipe.get(quote_min_key)
    pipe.get(build_min_key)
    pipe.get(month_key)
    results = pipe.execute()
    r.close()

    return {
        "quote_this_minute": int(results[0]) if results[0] else 0,
        "build_this_minute": int(results[1]) if results[1] else 0,
        "total_this_month": int(results[2]) if results[2] else 0,
        "month": month_key_suffix,
    }


def invalidate_rate_limit_cache(app_id: str):
    """Clear cached rate limits after config change."""
    cache_key = f"rl:{app_id}"
    _rate_limit_cache.pop(cache_key, None)
