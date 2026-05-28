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
    create_user, authenticate_user, login_boss_user, get_user_by_id,
    list_users, update_user,
    create_api_token, list_api_tokens_by_user, list_all_api_tokens,
    get_api_token_detail, get_user_api_token, user_has_api_token,
    update_api_token, reset_api_key, issue_swap_jwt, enrich_token_jwt_meta,
    swap_jwt_issue_count, MAX_SWAP_JWT_ISSUE_COUNT,
    get_rate_limits, upsert_rate_limit, get_api_token_by_app_id,
    init_boss_tables,
)
from boss.auth import (
    generate_jwt,
    generate_swap_jwt,
    generate_boss_session_token,
    boss_login_required,
    boss_admin_required,
    init_boss_auth,
    invalidate_api_token_cache,
)
from boss.rate_limiter import (
    get_usage_stats,
    invalidate_rate_limit_cache,
    rate_limits_as_list,
    resolve_rate_limits,
)
from boss.email_utils import send_verification_code, is_valid_email
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
    init_boss_auth(get_db_conn_func)
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


def _reject_disabled_api_token(token: dict | None):
    if not token:
        return jsonify({"code": -1, "msg": "Token not found"}), 404
    if int(token.get("status") or 0) != 1:
        return jsonify({"code": -1, "msg": "API key is disabled"}), 403
    return None


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

    if not is_valid_email(email):
        return jsonify({"code": -1, "msg": "Invalid email format"})

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

    if not is_valid_email(email):
        return jsonify({"code": -1, "msg": "Invalid email format"})
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
        user, err = login_boss_user(conn, email, password)
    finally:
        conn.close()

    if err == "disabled":
        return jsonify({"code": -1, "msg": "Account is disabled"})
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
    if int(user.get("status") or 0) != 1:
        return jsonify({"code": -1, "msg": "Account is disabled"}), 403
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
        if app_fee < 0 or app_fee > 10:
            return jsonify({"code": -1, "msg": "appFee must be between 0 and 10 (percent)"})

    conn = _conn()
    try:
        if user_has_api_token(conn, g.boss_user_id):
            return jsonify({
                "code": -1,
                "msg": "Each account may only have one API key. Open your existing key to view or regenerate the JWT.",
            })
        token = create_api_token(conn, g.boss_user_id, app_name, refund_address, app_fee)
        jwt_token = generate_swap_jwt(token["app_id"], token["app_secret"])
        ok, err = issue_swap_jwt(conn, token["id"], jwt_token)
        if not ok:
            return jsonify({"code": -1, "msg": err or "Failed to store JWT"})
        invalidate_api_token_cache(token["app_id"])
        refreshed = get_api_token_detail(conn, token["id"], user_id=g.boss_user_id)
        out = enrich_token_jwt_meta(refreshed)
    finally:
        conn.close()

    return jsonify({"code": 0, "msg": "success", "data": out})


@boss_bp.route("/api-tokens", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def list_my_tokens():
    conn = _conn()
    try:
        tokens = [
            enrich_token_jwt_meta(t)
            for t in list_api_tokens_by_user(conn, g.boss_user_id)
        ]
        tokens = [t for t in tokens if t]
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
            token["rate_limits"] = rate_limits_as_list(get_rate_limits(conn, token["app_id"]))
    finally:
        conn.close()
    if not token:
        return jsonify({"code": -1, "msg": "Token not found"})

    return jsonify({"code": 0, "msg": "success", "data": enrich_token_jwt_meta(token)})


@boss_bp.route("/api-tokens/<int:token_id>", methods=["PUT"])
@boss_login_required(BOSS_SESSION_SECRET)
def update_my_token(token_id):
    body = request.get_json(silent=True) or {}
    conn = _conn()
    try:
        token = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
        blocked = _reject_disabled_api_token(token)
        if blocked:
            return blocked

        updates = {}
        if "refundAddress" in body:
            updates["refund_address"] = (body["refundAddress"] or "").strip()
        if "appFee" in body:
            try:
                fee = float(body["appFee"])
            except (ValueError, TypeError):
                return jsonify({"code": -1, "msg": "appFee must be a number"})
            if fee < 0 or fee > 10:
                return jsonify({"code": -1, "msg": "appFee must be between 0 and 10 (percent)"})
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
        blocked = _reject_disabled_api_token(token)
        if blocked:
            return blocked
        reset_api_key(conn, token_id)
        invalidate_api_token_cache(token.get("app_id"))
    finally:
        conn.close()
    return jsonify({
        "code": 0,
        "msg": "success",
        "data": {"note": "Secret rotated and stored JWT cleared. Regenerate JWT to use Swap API again."},
    })


@boss_bp.route("/api-tokens/<int:token_id>/generate-jwt", methods=["POST"])
@boss_login_required(BOSS_SESSION_SECRET)
def generate_api_jwt(token_id):
    conn = _conn()
    try:
        token = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
        if not token:
            return jsonify({"code": -1, "msg": "Token not found"}), 404
        blocked = _reject_disabled_api_token(token)
        if blocked:
            return blocked

        if swap_jwt_issue_count(token) >= MAX_SWAP_JWT_ISSUE_COUNT:
            return jsonify({
                "code": -1,
                "msg": (
                    f"JWT can only be issued {MAX_SWAP_JWT_ISSUE_COUNT} times for this API key "
                    "(including the initial issue on key creation)"
                ),
            })

        jwt_token = generate_swap_jwt(token["app_id"], token["app_secret"])
        ok, err = issue_swap_jwt(conn, token["id"], jwt_token)
        if not ok:
            return jsonify({"code": -1, "msg": err or "Failed to store JWT"})
        invalidate_api_token_cache(token["app_id"])
        refreshed = get_api_token_detail(conn, token_id, user_id=g.boss_user_id)
    finally:
        conn.close()

    return jsonify({
        "code": 0,
        "msg": "success",
        "data": enrich_token_jwt_meta(refreshed),
    })


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
    limits_map = resolve_rate_limits(rate_limits)

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
    updates = {}
    if "role" in body and body.get("role") is not None:
        updates["role"] = body.get("role")
    if "status" in body:
        try:
            updates["status"] = int(body["status"])
        except (TypeError, ValueError):
            return jsonify({"code": -1, "msg": "status must be 0 or 1"})
    if not updates:
        return jsonify({"code": -1, "msg": "No fields to update"})

    conn = _conn()
    try:
        ok = update_user(conn, user_id, **updates)
        if not ok:
            return jsonify({"code": -1, "msg": "Update failed"})
        user = get_user_by_id(conn, user_id)
    finally:
        conn.close()
    if not user:
        return jsonify({"code": -1, "msg": "User not found"})
    return jsonify({"code": 0, "msg": "success", "data": user})


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
        before = get_api_token_detail(conn, token_id)
        update_api_token(conn, token_id, app_name=body.get("appName"), status=body.get("status"))
        after = get_api_token_detail(conn, token_id)
        app_id = (after or before or {}).get("app_id")
        if app_id and (
            body.get("status") is not None
            or (before and after and before.get("status") != after.get("status"))
        ):
            invalidate_api_token_cache(str(app_id))
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
    return jsonify({"code": 0, "msg": "success", "data": rate_limits_as_list(limits)})


@boss_bp.route("/admin/tokens/<app_id>/rate-limits", methods=["PUT"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_set_rate_limits(app_id):
    body = request.get_json(silent=True) or {}
    configs = body.get("configs", [])
    if not configs:
        return jsonify({"code": -1, "msg": "configs is required"})

    conn = _conn()
    try:
        token = get_api_token_by_app_id(conn, app_id)
        if not token:
            return jsonify({"code": -1, "msg": "API token not found for this app_id"})

        for cfg in configs:
            endpoint_group = (cfg.get("endpointGroup") or "").strip()
            if endpoint_group not in ("quote", "build", "all"):
                return jsonify({"code": -1, "msg": f"Invalid endpointGroup: {endpoint_group}"})
            try:
                per_minute = int(cfg.get("perMinute", 60))
                per_month = int(cfg.get("perMonth", 300000))
            except (TypeError, ValueError):
                return jsonify({"code": -1, "msg": "perMinute and perMonth must be integers"})
            if per_minute < 1 or per_month < 1:
                return jsonify({"code": -1, "msg": "perMinute and perMonth must be >= 1"})
            upsert_rate_limit(conn, app_id, endpoint_group, per_minute, per_month)

        limits = get_rate_limits(conn, app_id)
    finally:
        conn.close()

    invalidate_rate_limit_cache(app_id)
    return jsonify({"code": 0, "msg": "success", "data": rate_limits_as_list(limits)})


@boss_bp.route("/admin/tokens/<app_id>/usage", methods=["GET"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_get_token_usage(app_id):
    conn = _conn()
    try:
        limits = get_rate_limits(conn, app_id)
    finally:
        conn.close()

    usage = get_usage_stats(app_id)
    limits_map = resolve_rate_limits(limits)

    return jsonify({"code": 0, "msg": "success", "data": {"usage": usage, "limits": limits_map}})
