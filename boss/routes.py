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
import json
import random
import secrets
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, g
from loguru import logger

import redis as redis_lib
from boss.models import (
    create_user, authenticate_user, login_boss_user, get_user_by_id, get_user_by_email,
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
    DEFAULT_RATE_LIMITS,
    get_usage_stats,
    invalidate_rate_limit_cache,
    rate_limits_as_list,
    resolve_rate_limits,
)
from boss.email_utils import send_verification_code, is_valid_email
from config import Cfg
from redis_provider import get_chain_tokens_with_prices
from nearintents_utils import CHAIN_TO_1CLICK
from message import send_message
from db_provider import (
    query_boss_fee_balance_by_app,
    insert_boss_fee_withdraw_request,
    query_boss_fee_withdraw_requests,
    get_boss_fee_withdraw_request_by_id,
    update_boss_fee_withdraw_request,
)

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

    conn = _conn()
    try:
        if get_user_by_email(conn, email):
            return jsonify({"code": -1, "msg": "Email already registered"})
    finally:
        conn.close()

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


def _resolve_withdraw_min_usd() -> Decimal:
    raw = getattr(Cfg, "BOSS_FEE_WITHDRAW_MIN_USD", "10")
    try:
        val = Decimal(str(raw))
        if val < 0:
            return Decimal("10")
        return val
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("10")


_WITHDRAW_MIN_USD = _resolve_withdraw_min_usd()


def _asset_to_near_contract(asset: str) -> str:
    a = str(asset or "").strip()
    if a.lower().startswith("nep141:"):
        return a[7:]
    return a


def _safe_decimal(v, default="0") -> Decimal:
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def _pow10(decimals: int) -> Decimal:
    if decimals <= 0:
        return Decimal("1")
    return Decimal(10) ** decimals


def _near_price_map() -> dict:
    data = get_chain_tokens_with_prices("near", max_age_seconds=600) or {}
    return {str(k).lower(): v for k, v in data.items()} if isinstance(data, dict) else {}


def _format_fee_balance_rows(rows: list) -> list:
    prices = _near_price_map()
    out = []
    for row in rows or []:
        asset = str(row.get("fee_token_asset") or "")
        symbol = str(row.get("fee_token_symbol") or "")
        decimals = int(row.get("fee_token_decimals") or 0)
        accrued = int(_safe_decimal(row.get("total_accrued"), "0"))
        locked = int(_safe_decimal(row.get("total_locked"), "0"))
        available = max(0, int(_safe_decimal(row.get("total_available"), "0")))
        contract = _asset_to_near_contract(asset).lower()
        token_info = prices.get(contract) or {}
        price = _safe_decimal((token_info or {}).get("price"), "0")
        amount_hr = (Decimal(available) / _pow10(decimals)) if available > 0 else Decimal("0")
        usd = amount_hr * price if price > 0 else Decimal("0")
        out.append(
            {
                "asset": asset,
                "symbol": symbol,
                "decimals": decimals,
                "totalAccrued": str(accrued),
                "totalLocked": str(locked),
                "totalAvailable": str(available),
                "availableUsd": str(usd.quantize(Decimal("0.000001")) if usd > 0 else Decimal("0")),
                "canWithdraw": bool(available > 0 and usd >= _WITHDRAW_MIN_USD),
            }
        )
    return out


def _normalize_withdraw_rows(rows: list) -> list:
    out = []
    for row in rows or []:
        item = dict(row)
        for k in ("created_at", "updated_at", "reviewed_at"):
            if item.get(k) is not None:
                item[k] = str(item.get(k))
        if isinstance(item.get("status_response"), str):
            try:
                item["status_response"] = json.loads(item["status_response"])
            except Exception:
                pass
        out.append(item)
    return out


def _user_app_token_or_none():
    conn = _conn()
    try:
        return get_user_api_token(conn, g.boss_user_id)
    finally:
        conn.close()


def _notify_boss_withdraw(event_type: str, content: dict) -> None:
    try:
        payload = dict(content or {})
        payload.setdefault("source", "stats")
        send_message(event_type, payload)
    except Exception as e:
        logger.warning(f"boss withdraw notify failed: {e}")


@boss_bp.route("/fee/balances", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def fee_balances():
    token = _user_app_token_or_none()
    if not token:
        return jsonify({"code": 0, "msg": "success", "data": {"appId": "", "tokens": []}})
    app_id = str(token.get("app_id") or "")
    rows = query_boss_fee_balance_by_app(Cfg.NETWORK_ID, app_id)
    return jsonify({"code": 0, "msg": "success", "data": {"appId": app_id, "tokens": _format_fee_balance_rows(rows)}})


@boss_bp.route("/fee/withdraw-options", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def fee_withdraw_options():
    chains = sorted(set(CHAIN_TO_1CLICK.values()))
    return jsonify({"code": 0, "msg": "success", "data": {"chains": chains, "minUsd": str(_WITHDRAW_MIN_USD)}})


@boss_bp.route("/fee/withdraw-requests", methods=["POST"])
@boss_login_required(BOSS_SESSION_SECRET)
def create_fee_withdraw_request():
    body = request.get_json(silent=True) or {}
    token_asset = str(body.get("tokenAsset") or body.get("asset") or "").strip()
    amount = str(body.get("amount") or "").strip()
    to_chain = str(body.get("toChain") or "").strip()
    to_address = str(body.get("toAddress") or body.get("recipient") or "").strip()
    if not token_asset or not amount or not to_chain or not to_address:
        return jsonify({"code": -1, "msg": "tokenAsset, amount, toChain, toAddress are required"})
    try:
        amount_int = int(amount)
    except ValueError:
        return jsonify({"code": -1, "msg": "amount must be integer smallest-unit string"})
    if amount_int <= 0:
        return jsonify({"code": -1, "msg": "amount must be > 0"})

    token = _user_app_token_or_none()
    if not token:
        return jsonify({"code": -1, "msg": "Please create API token first"})
    app_id = str(token.get("app_id") or "")
    rows = query_boss_fee_balance_by_app(Cfg.NETWORK_ID, app_id)
    formatted = _format_fee_balance_rows(rows)
    selected = next((x for x in formatted if str(x.get("asset")) == token_asset), None)
    if not selected:
        return jsonify({"code": -1, "msg": "tokenAsset not found in your fee balances"})
    available = int(selected.get("totalAvailable") or "0")
    if amount_int > available:
        return jsonify({"code": -1, "msg": "amount exceeds available balance"})

    decimals = int(selected.get("decimals") or 0)
    prices = _near_price_map()
    contract = _asset_to_near_contract(token_asset).lower()
    price = _safe_decimal((prices.get(contract) or {}).get("price"), "0")
    req_usd = (Decimal(amount_int) / _pow10(decimals)) * price if price > 0 else Decimal("0")
    if req_usd < _WITHDRAW_MIN_USD:
        return jsonify({"code": -1, "msg": f"Withdrawal amount must be >= {_WITHDRAW_MIN_USD} USD by current price"})

    req_id = insert_boss_fee_withdraw_request(
        Cfg.NETWORK_ID,
        app_id=app_id,
        boss_user_id=g.boss_user_id,
        fee_token_asset=token_asset,
        fee_token_symbol=selected.get("symbol"),
        fee_token_decimals=decimals,
        amount=str(amount_int),
        amount_usd=str(req_usd.quantize(Decimal("0.000001"))),
        to_chain=to_chain,
        to_address=to_address,
    )
    _notify_boss_withdraw(
        "alert",
        {
            "event": "withdraw_apply",
            "app_id": app_id,
            "boss_user_id": g.boss_user_id,
            "request_id": req_id,
            "token_asset": token_asset,
            "amount_smallest": str(amount_int),
            "to_chain": to_chain,
            "to_address": to_address,
            "amount_usd": str(req_usd.quantize(Decimal("0.000001"))),
        },
    )
    return jsonify({"code": 0, "msg": "success", "data": {"id": req_id, "status": "PENDING"}})


@boss_bp.route("/fee/withdraw-requests", methods=["GET"])
@boss_login_required(BOSS_SESSION_SECRET)
def list_fee_withdraw_requests():
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("pageSize", 20, type=int)
    status = (request.args.get("status") or "").strip().upper() or None
    token = _user_app_token_or_none()
    if not token:
        return jsonify({"code": 0, "msg": "success", "data": {"list": [], "total": 0, "page": page, "pageSize": page_size}})
    app_id = str(token.get("app_id") or "")
    rows, total = query_boss_fee_withdraw_requests(
        Cfg.NETWORK_ID,
        app_id=app_id,
        status=status,
        page_number=page,
        page_size=page_size,
    )
    return jsonify({"code": 0, "msg": "success", "data": {"list": _normalize_withdraw_rows(rows), "total": total, "page": page, "pageSize": page_size}})


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
            group_defaults = DEFAULT_RATE_LIMITS.get(endpoint_group, DEFAULT_RATE_LIMITS["quote"])
            try:
                per_minute = int(cfg.get("perMinute", group_defaults["per_minute"]))
                per_month = int(cfg.get("perMonth", group_defaults["per_month"]))
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


@boss_bp.route("/admin/fee/withdraw-requests", methods=["GET"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_list_fee_withdraw_requests():
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("pageSize", 20, type=int)
    status = (request.args.get("status") or "").strip().upper() or None
    rows, total = query_boss_fee_withdraw_requests(
        Cfg.NETWORK_ID,
        app_id=None,
        status=status,
        page_number=page,
        page_size=page_size,
    )
    return jsonify({"code": 0, "msg": "success", "data": {"list": _normalize_withdraw_rows(rows), "total": total, "page": page, "pageSize": page_size}})


@boss_bp.route("/admin/fee/withdraw-requests/<int:request_id>/review", methods=["POST"])
@boss_admin_required(BOSS_SESSION_SECRET)
def admin_review_fee_withdraw_request(request_id):
    body = request.get_json(silent=True) or {}
    action = str(body.get("action") or "").strip().lower()
    note = str(body.get("note") or "").strip()
    if action not in ("approve", "reject"):
        return jsonify({"code": -1, "msg": "action must be approve or reject"})

    row = get_boss_fee_withdraw_request_by_id(Cfg.NETWORK_ID, request_id)
    if not row:
        return jsonify({"code": -1, "msg": "request not found"}), 404
    if str(row.get("status") or "").upper() != "PENDING":
        return jsonify({"code": -1, "msg": "only PENDING request can be reviewed"})

    new_status = "APPROVED" if action == "approve" else "REJECTED"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    update_boss_fee_withdraw_request(
        Cfg.NETWORK_ID,
        request_id,
        status=new_status,
        review_note=note,
        reviewed_by_user_id=g.boss_user_id,
        reviewed_at=now,
    )
    return jsonify({"code": 0, "msg": "success", "data": {"id": request_id, "status": new_status}})
