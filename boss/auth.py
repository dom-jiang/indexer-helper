"""
JWT authentication for the Boss system.

Flow:
  1. User registers -> gets user account
  2. User creates API token -> gets app_id + app_key + app_secret
  3. Client generates JWT: HS256 signed with app_secret, sub=app_id
  4. Client sends JWT in Authorization header for /api/swap/* calls
  5. Middleware validates JWT signature and checks token status
"""

import secrets
import time
from functools import wraps
from typing import Any, Dict, Optional, Tuple

import jwt
from flask import request, jsonify, g
from loguru import logger
from cachetools import TTLCache

from boss.models import get_api_token_by_app_id, authenticate_user, get_user_by_id

_token_cache = TTLCache(maxsize=1000, ttl=60)

_boss_get_db_conn = None


def init_boss_auth(get_db_conn_func):
    """Register DB accessor for session guards (active user checks)."""
    global _boss_get_db_conn
    _boss_get_db_conn = get_db_conn_func


def invalidate_api_token_cache(app_id: str) -> None:
    if app_id:
        _token_cache.pop(f"token:{app_id}", None)


def _boss_user_is_active(user_id: int) -> bool:
    if not _boss_get_db_conn:
        return True
    conn = _boss_get_db_conn()
    try:
        user = get_user_by_id(conn, user_id)
    finally:
        conn.close()
    return bool(user and int(user.get("status") or 0) == 1)


def generate_jwt(app_id: str, app_secret: str, expires_in: int = 86400) -> str:
    """Legacy helper with expiry (Boss session / old callers). Prefer generate_swap_jwt."""
    now = int(time.time())
    payload = {
        "sub": app_id,
        "iat": now,
        "exp": now + expires_in,
    }
    return jwt.encode(payload, app_secret, algorithm="HS256")


def generate_swap_jwt(app_id: str, app_secret: str) -> str:
    """API JWT — no expiry; validity is DB-backed (single active token per API key)."""
    now = int(time.time())
    payload = {
        "sub": app_id,
        "iat": now,
        "jti": secrets.token_hex(16),
    }
    return jwt.encode(payload, app_secret, algorithm="HS256")


def decode_jwt(token_str: str, app_secret: str) -> dict | None:
    try:
        return jwt.decode(token_str, app_secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def decode_swap_jwt(token_str: str, app_secret: str) -> dict | None:
    try:
        return jwt.decode(
            token_str,
            app_secret,
            algorithms=["HS256"],
            options={"verify_exp": False},
        )
    except jwt.InvalidTokenError:
        return None


def _swap_jwt_error(reason: str, msg: str) -> Dict[str, str]:
    return {"reason": reason, "msg": msg}


def validate_swap_jwt(get_db_conn_func) -> Tuple[Optional[dict], Optional[Dict[str, str]]]:
    """
    Validate JWT from Authorization header for swap API calls.

    Returns:
      (api_token, None) on success — sets g.app_id and g.api_token.
      (None, {"reason": "<code>", "msg": "<detail>"}) on failure.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header:
        return None, _swap_jwt_error(
            "missing_authorization",
            "Authorization header is required (format: Bearer <jwt>)",
        )
    if not auth_header.startswith("Bearer "):
        return None, _swap_jwt_error(
            "invalid_authorization_scheme",
            "Authorization header must use Bearer scheme (Bearer <jwt>)",
        )

    token_str = auth_header[7:].strip()
    if not token_str:
        return None, _swap_jwt_error(
            "empty_token",
            "Bearer token is empty",
        )

    try:
        unverified = jwt.decode(token_str, options={"verify_signature": False})
    except jwt.InvalidTokenError:
        return None, _swap_jwt_error(
            "malformed_token",
            "JWT is malformed or cannot be parsed",
        )

    app_id = unverified.get("sub")
    if not app_id:
        return None, _swap_jwt_error(
            "missing_subject",
            "JWT payload is missing subject (sub)",
        )
    app_id = str(app_id).strip()

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

    if not api_token:
        return None, _swap_jwt_error(
            "unknown_app_id",
            f"API key not found for app_id {app_id}",
        )

    if int(api_token.get("status") or 0) != 1:
        return None, _swap_jwt_error(
            "api_key_disabled",
            f"API key is disabled for app_id {app_id}",
        )

    payload = decode_swap_jwt(token_str, api_token["app_secret"])
    if not payload:
        return None, _swap_jwt_error(
            "invalid_signature",
            "JWT signature is invalid (wrong app_secret, tampered token, or API key was reset)",
        )

    stored = str(api_token.get("swap_jwt") or "").strip()
    if not stored:
        return None, _swap_jwt_error(
            "jwt_not_issued",
            "No active API JWT on file; create or regenerate JWT in Boss dashboard",
        )
    if token_str != stored:
        return None, _swap_jwt_error(
            "jwt_revoked",
            "JWT is not the current active token (regenerate JWT or reset API key invalidates the previous JWT)",
        )

    g.app_id = app_id
    g.api_token = api_token
    return api_token, None


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
            if not _boss_user_is_active(int(payload["user_id"])):
                return jsonify({"code": -1, "msg": "Account is disabled"}), 403
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
            if not _boss_user_is_active(int(payload["user_id"])):
                return jsonify({"code": -1, "msg": "Account is disabled"}), 403
            g.boss_user_id = payload["user_id"]
            g.boss_role = payload.get("role", "user")
            return f(*args, **kwargs)
        return wrapper
    return decorator
