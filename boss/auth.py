"""
JWT authentication for the Boss system.

Flow:
  1. User registers -> gets user account
  2. User creates API token -> gets app_id + app_key + app_secret
  3. Client generates JWT: HS256 signed with app_secret, sub=app_id
  4. Client sends JWT in Authorization header for /api/swap/* calls
  5. Middleware validates JWT signature and checks token status
"""

import time
from functools import wraps
from typing import Optional

import jwt
from flask import request, jsonify, g
from loguru import logger
from cachetools import TTLCache

from boss.models import get_api_token_by_app_id, authenticate_user, get_user_by_id

_token_cache = TTLCache(maxsize=1000, ttl=60)


def generate_jwt(app_id: str, app_secret: str, expires_in: int = 86400) -> str:
    now = int(time.time())
    payload = {
        "sub": app_id,
        "iat": now,
        "exp": now + expires_in,
    }
    return jwt.encode(payload, app_secret, algorithm="HS256")


def decode_jwt(token_str: str, app_secret: str) -> dict | None:
    try:
        return jwt.decode(token_str, app_secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def validate_swap_jwt(get_db_conn_func) -> Optional[dict]:
    """
    Validate JWT from Authorization header for swap API calls.
    Returns the api_token record on success, or None.
    Sets g.app_id on success.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None

    token_str = auth_header[7:].strip()
    if not token_str:
        return None

    try:
        unverified = jwt.decode(token_str, options={"verify_signature": False})
        app_id = unverified.get("sub")
        if not app_id:
            return None
    except jwt.InvalidTokenError:
        return None

    cache_key = f"token:{app_id}"
    api_token = _token_cache.get(cache_key)
    if not api_token:
        conn = get_db_conn_func()
        try:
            api_token = get_api_token_by_app_id(conn, app_id)
        finally:
            conn.close()
        if api_token:
            _token_cache[cache_key] = api_token

    if not api_token or api_token.get("status") != 1:
        return None

    payload = decode_jwt(token_str, api_token["app_secret"])
    if not payload:
        return None

    g.app_id = app_id
    g.api_token = api_token
    return api_token


def generate_boss_session_token(user_id: int, role: str, secret: str, expires_in: int = 86400) -> str:
    """Generate session JWT for Boss UI login."""
    now = int(time.time())
    payload = {
        "user_id": user_id,
        "role": role,
        "iat": now,
        "exp": now + expires_in,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def decode_boss_session_token(token_str: str, secret: str) -> dict | None:
    try:
        return jwt.decode(token_str, secret, algorithms=["HS256"])
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None


def boss_login_required(secret: str):
    """Decorator to require Boss session JWT."""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            auth_header = request.headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                return jsonify({"code": 401, "msg": "Authorization required"}), 401
            token_str = auth_header[7:].strip()
            payload = decode_boss_session_token(token_str, secret)
            if not payload:
                return jsonify({"code": 401, "msg": "Invalid or expired token"}), 401
            g.boss_user_id = payload["user_id"]
            g.boss_role = payload.get("role", "user")
            return f(*args, **kwargs)
        return wrapper
    return decorator


def boss_admin_required(secret: str):
    """Decorator to require Boss admin session JWT."""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            auth_header = request.headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                return jsonify({"code": 401, "msg": "Authorization required"}), 401
            token_str = auth_header[7:].strip()
            payload = decode_boss_session_token(token_str, secret)
            if not payload:
                return jsonify({"code": 401, "msg": "Invalid or expired token"}), 401
            if payload.get("role") != "admin":
                return jsonify({"code": 403, "msg": "Admin access required"}), 403
            g.boss_user_id = payload["user_id"]
            g.boss_role = payload.get("role", "user")
            return f(*args, **kwargs)
        return wrapper
    return decorator
