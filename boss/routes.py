"""
Boss system Flask routes.

User-facing:
  POST /boss/send-code             (email verification code)
  POST /boss/register
  POST /boss/login
  GET  /boss/me
  POST /boss/api-tokens           (create key)
  GET  /boss/api-tokens           (list my keys)
  GET  /boss/api-tokens/<id>      (detail)
  POST /boss/api-tokens/<id>/reset-key
  POST /boss/api-tokens/<id>/generate-jwt
  GET  /boss/api-tokens/<id>/usage

Admin:
  GET  /boss/admin/users
  PUT  /boss/admin/users/<id>
  GET  /boss/admin/tokens
  PUT  /boss/admin/tokens/<id>
  GET  /boss/admin/tokens/<app_id>/rate-limits
  PUT  /boss/admin/tokens/<app_id>/rate-limits
"""

import hashlib
import random
import secrets
from flask import Blueprint, request, jsonify, g
from loguru import logger

import redis as redis_lib
from boss.models import (
    create_user, authenticate_user, get_user_by_id,
    list_users, update_user,
    create_api_token, list_api_tokens_by_user, list_all_api_tokens,
    get_api_token_detail, update_api_token, reset_api_key,
    get_rate_limits, upsert_rate_limit, get_api_token_by_app_id,
    init_boss_tables,
)
from boss.auth import (
    generate_jwt, generate_boss_session_token, boss_login_required, boss_admin_required,
)
from boss.rate_limiter import get_usage_stats, invalidate_rate_limit_cache
from boss.email_utils import send_verification_code
from config import Cfg

_aes_key = getattr(Cfg, "CRYPTO_AES_KEY", "") or ""
BOSS_SESSION_SECRET = hashlib.sha256(f"boss_session_{_aes_key}".encode()).hexdigest()

boss_bp = Blueprint("boss", __name__, url_prefix="/boss")

_get_db_conn = None

_boss_redis_pool = redis_lib.ConnectionPool(
    host=Cfg.REDIS["REDIS_HOST"],
    port=int(Cfg.REDIS["REDIS_PORT"]),
    decode_responses=True,
    db=3,
)


def _boss_redis():
    return redis_lib.StrictRedis(connection_pool=_boss_redis_pool)


def init_boss_routes(app, get_db_conn_func):
    """Register boss blueprint and initialize tables."""
    global _get_db_conn
    _get_db_conn = get_db_conn_func
    app.register_blueprint(boss_bp)

    try:
        conn = get_db_conn_func()
        init_boss_tables(conn)
        conn.close()
        logger.info("Boss tables initialized")
    except Exception as e:
        logger.warning(f"Boss table init skipped: {e}")


def _conn():
    return _get_db_conn()


# ── Public: Config / Email verification / Register / Login ─

@boss_bp.route("/config", methods=["GET"])
def boss_config():
    """Public endpoint: return frontend-relevant config flags."""
    return jsonify({
        "code": 0,
        "data": {
            "emailVerify": bool(Cfg.BOSS_EMAIL_VERIFY),
        },
    })


@boss_bp.route("/send-code", methods=["POST"])
def send_code():
    if not Cfg.BOSS_EMAIL_VERIFY:
        return jsonify({"code": -1, "msg": "Email verification is disabled"})

    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip().lower()

    if not email or "@" not in email:
        return jsonify({"code": -1, "msg": "Valid email required"})

    r = _boss_redis()
    lock_key = f"boss:email_code_lock:{email}"
    if r.get(lock_key):
        return jsonify({"code": -1, "msg": "Please wait 60 seconds before requesting a new code"})

    code = f"{random.randint(0, 999999):06d}"
    ok = send_verification_code(email, code)
    if not ok:
        return jsonify({"code": -1, "msg": "Failed to send verification email, please try again later"})

    code_key = f"boss:email_code:{email}"
    r.set(code_key, code, ex=300)
    r.set(lock_key, "1", ex=60)

    return jsonify({"code": 0, "msg": "Verification code sent"})


@boss_bp.route("/register", methods=["POST"])
def register():
    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""

    if not email or "@" not in email:
        return jsonify({"code": -1, "msg": "Valid email required"})
    if len(password) < 6:
        return jsonify({"code": -1, "msg": "Password must be at least 6 characters"})

    if Cfg.BOSS_EMAIL_VERIFY:
        code = (body.get("code") or "").strip()
        if not code:
            return jsonify({"code": -1, "msg": "Verification code required"})
        r = _boss_redis()
        code_key = f"boss:email_code:{email}"
        stored_code = r.get(code_key)
        if not stored_code or stored_code != code:
            return jsonify({"code": -1, "msg": "Invalid or expired verification code"})

    conn = _conn()
    try:
        user = create_user(conn, email, password)
    finally:
        conn.close()

    if not user:
        return jsonify({"code": -1, "msg": "Email already registered"})

    if Cfg.BOSS_EMAIL_VERIFY:
        r = _boss_redis()
        r.delete(f"boss:email_code:{email}")

    token = generate_boss_session_token(user["id"], user["role"], BOSS_SESSION_SECRET)
    return jsonify({"code": 0, "msg": "success", "data": {"user": user, "token": token}})


@boss_bp.route("/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""

    conn = _conn()
    try:
        user = authenticate_user(conn, email, password)
    finally:
        conn.close()

    if not user:
        return jsonify({"code": -1, "msg": "Invalid email or password"})

    token = generate_boss_session_token(user["id"], user["role"], BOSS_SESSION_SECRET)
    return jsonify({"code": 0, "msg": "success", "data": {"user": user, "token": token}})


@boss_bp.route("/me", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def me():
    conn = _conn()
    try:
        user = get_user_by_id(conn, g.boss_user_id)
    finally:
        conn.close()
    if not user:
        return jsonify({"code": -1, "msg": "User not found"})
    return jsonify({"code": 0, "msg": "success", "data": user})


# ── User: API Token management ───────────────────────────

@boss_bp.route("/api-tokens", methods=["POST"])
@boss_login_required(BOSS_SESSION_SECRET)
def create_token():
    body = request.get_json(silent=True) or {}
    app_name = body.get("appName", "")
    refund_address = (body.get("refundAddress") or "").strip()
    app_fee_raw = body.get("appFee")

    app_fee = 0.0
    if app_fee_raw is not None and app_fee_raw != "":
        try:
            app_fee = float(app_fee_raw)
        except (ValueError, TypeError):
            return jsonify({"code": -1, "msg": "appFee must be a number"})
        if app_fee < 1 or app_fee > 10:
            return jsonify({"code": -1, "msg": "appFee must be between 1 and 10 (percent)"})

    conn = _conn()
    try:
        token = create_api_token(conn, g.boss_user_id, app_name, refund_address, app_fee)
    finally:
        conn.close()

    expires_in = 86400 * 30
    jwt_token = generate_jwt(token["app_id"], token["app_secret"], expires_in=expires_in)
    token.pop("app_secret", None)
    token["jwt"] = jwt_token
    token["jwt_expires_in"] = expires_in
    return jsonify({"code": 0, "msg": "success", "data": token})


@boss_bp.route("/api-tokens", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def list_my_tokens():
    conn = _conn()
    try:
        tokens = list_api_tokens_by_user(conn, g.boss_user_id)
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success", "data": tokens})


@boss_bp.route("/api-tokens/<int:token_id>", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def get_token_detail(token_id):
    conn = _conn()
    try:
        token = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
        if token:
            token["rate_limits"] = get_rate_limits(conn, token["app_id"])
    finally:
        conn.close()
    if not token:
        return jsonify({"code": -1, "msg": "Token not found"})

    token.pop("app_secret", None)
    token.pop("app_key", None)
    return jsonify({"code": 0, "msg": "success", "data": token})


@boss_bp.route("/api-tokens/<int:token_id>", methods=["PUT"])
@boss_login_required(BOSS_SESSION_SECRET)
def update_my_token(token_id):
    body = request.get_json(silent=True) or {}
    conn = _conn()
    try:
        token = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
        if not token:
            return jsonify({"code": -1, "msg": "Token not found"})

        updates = {}
        if "refundAddress" in body:
            updates["refund_address"] = (body["refundAddress"] or "").strip()
        if "appFee" in body:
            try:
                fee = float(body["appFee"])
            except (ValueError, TypeError):
                return jsonify({"code": -1, "msg": "appFee must be a number"})
            if fee != 0 and (fee < 1 or fee > 10):
                return jsonify({"code": -1, "msg": "appFee must be between 1 and 10 (percent), or 0 to disable"})
            updates["app_fee"] = fee

        if updates:
            update_api_token(conn, token_id, **updates)
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success"})


@boss_bp.route("/api-tokens/<int:token_id>/reset-key", methods=["POST"])
@boss_login_required(BOSS_SESSION_SECRET)
def reset_token_key(token_id):
    conn = _conn()
    try:
        token = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
        if not token:
            return jsonify({"code": -1, "msg": "Token not found"})
        reset_api_key(conn, token_id)
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success"})


@boss_bp.route("/api-tokens/<int:token_id>/generate-jwt", methods=["POST"])
@boss_login_required(BOSS_SESSION_SECRET)
def generate_api_jwt(token_id):
    body = request.get_json(silent=True) or {}
    expires_in = body.get("expiresIn", 86400 * 30)

    conn = _conn()
    try:
        token = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
    finally:
        conn.close()

    if not token:
        return jsonify({"code": -1, "msg": "Token not found"})

    jwt_token = generate_jwt(token["app_id"], token["app_secret"], expires_in=int(expires_in))
    return jsonify({"code": 0, "msg": "success", "data": {"jwt": jwt_token, "expiresIn": expires_in}})


@boss_bp.route("/api-tokens/<int:token_id>/usage", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def get_token_usage(token_id):
    conn = _conn()
    try:
        token = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
        if token:
            rate_limits = get_rate_limits(conn, token["app_id"])
    finally:
        conn.close()

    if not token:
        return jsonify({"code": -1, "msg": "Token not found"})

    usage = get_usage_stats(token["app_id"])
    limits_map = {}
    for rl in rate_limits:
        limits_map[rl["endpoint_group"]] = {"per_minute": rl["per_minute"], "per_month": rl["per_month"]}

    return jsonify({"code": 0, "msg": "success", "data": {"usage": usage, "limits": limits_map}})


# ── Admin: User management ───────────────────────────────

@boss_bp.route("/admin/users", methods=["GET"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_list_users():
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("pageSize", 20, type=int)
    conn = _conn()
    try:
        result = list_users(conn, page, page_size)
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success", "data": result})


@boss_bp.route("/admin/users/<int:user_id>", methods=["PUT"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_update_user(user_id):
    body = request.get_json(silent=True) or {}
    conn = _conn()
    try:
        update_user(conn, user_id, role=body.get("role"), status=body.get("status"))
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success"})


# ── Admin: Token management ──────────────────────────────

@boss_bp.route("/admin/tokens", methods=["GET"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_list_tokens():
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("pageSize", 20, type=int)
    conn = _conn()
    try:
        result = list_all_api_tokens(conn, page, page_size)
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success", "data": result})


@boss_bp.route("/admin/tokens/<int:token_id>", methods=["PUT"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_update_token(token_id):
    body = request.get_json(silent=True) or {}
    conn = _conn()
    try:
        update_api_token(conn, token_id, app_name=body.get("appName"), status=body.get("status"))
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success"})


@boss_bp.route("/admin/tokens/<app_id>/rate-limits", methods=["GET"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_get_rate_limits(app_id):
    conn = _conn()
    try:
        limits = get_rate_limits(conn, app_id)
    finally:
        conn.close()
    return jsonify({"code": 0, "msg": "success", "data": limits})


@boss_bp.route("/admin/tokens/<app_id>/rate-limits", methods=["PUT"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_set_rate_limits(app_id):
    body = request.get_json(silent=True) or {}
    configs = body.get("configs", [])
    conn = _conn()
    try:
        for cfg in configs:
            endpoint_group = cfg.get("endpointGroup", "all")
            per_minute = cfg.get("perMinute", 60)
            per_month = cfg.get("perMonth", 300000)
            upsert_rate_limit(conn, app_id, endpoint_group, per_minute, per_month)
    finally:
        conn.close()

    invalidate_rate_limit_cache(app_id)
    return jsonify({"code": 0, "msg": "success"})


@boss_bp.route("/admin/tokens/<app_id>/usage", methods=["GET"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_get_token_usage(app_id):
    conn = _conn()
    try:
        limits = get_rate_limits(conn, app_id)
    finally:
        conn.close()

    usage = get_usage_stats(app_id)
    limits_map = {}
    for rl in limits:
        limits_map[rl["endpoint_group"]] = {"per_minute": rl["per_minute"], "per_month": rl["per_month"]}

    return jsonify({"code": 0, "msg": "success", "data": {"usage": usage, "limits": limits_map}})
