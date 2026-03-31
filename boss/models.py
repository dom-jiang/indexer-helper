"""
Boss system database models and operations.

Tables:
  - boss_user: user accounts (email/password)
  - boss_api_token: API keys (app_id / app_key)
  - boss_rate_limit_config: per-key rate limit settings
"""

import time
import uuid
import secrets
import hashlib
from datetime import datetime

import bcrypt
import pymysql
from loguru import logger


INIT_SQL = """
CREATE TABLE IF NOT EXISTS `boss_user` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `email` VARCHAR(255) NOT NULL,
  `password_hash` VARCHAR(255) NOT NULL,
  `role` ENUM('user','admin') NOT NULL DEFAULT 'user',
  `status` TINYINT NOT NULL DEFAULT 1 COMMENT '1=active 0=disabled',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `boss_api_token` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `user_id` INT NOT NULL,
  `app_name` VARCHAR(128) NOT NULL DEFAULT '',
  `app_id` VARCHAR(64) NOT NULL,
  `app_key` VARCHAR(128) NOT NULL,
  `app_secret` VARCHAR(128) NOT NULL COMMENT 'HS256 signing secret for JWT',
  `status` TINYINT NOT NULL DEFAULT 1 COMMENT '1=active 0=disabled',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_app_id` (`app_id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `boss_rate_limit_config` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `app_id` VARCHAR(64) NOT NULL,
  `endpoint_group` VARCHAR(32) NOT NULL DEFAULT 'all' COMMENT 'quote / build / all',
  `per_minute` INT NOT NULL DEFAULT 60,
  `per_month` INT NOT NULL DEFAULT 300000,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_app_endpoint` (`app_id`, `endpoint_group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def init_boss_tables(conn):
    """Run DDL to create boss tables if they don't exist."""
    cursor = conn.cursor()
    for statement in INIT_SQL.strip().split(";"):
        statement = statement.strip()
        if statement:
            cursor.execute(statement)
    conn.commit()
    cursor.close()


# ── helpers ──────────────────────────────────────────────

def _gen_app_id() -> str:
    return "ak_" + uuid.uuid4().hex[:16]


def _gen_app_key() -> str:
    return "sk_" + secrets.token_urlsafe(32)


def _gen_app_secret() -> str:
    return secrets.token_urlsafe(48)


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def _check_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode(), password_hash.encode())


# ── user CRUD ────────────────────────────────────────────

def create_user(conn, email: str, password: str, role: str = "user") -> dict:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    pw_hash = _hash_password(password)
    try:
        cursor.execute(
            "INSERT INTO boss_user (email, password_hash, role) VALUES (%s, %s, %s)",
            (email, pw_hash, role),
        )
        conn.commit()
        user_id = cursor.lastrowid
        return {"id": user_id, "email": email, "role": role}
    except pymysql.err.IntegrityError:
        conn.rollback()
        return None
    finally:
        cursor.close()


def authenticate_user(conn, email: str, password: str) -> dict | None:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM boss_user WHERE email = %s AND status = 1", (email,))
    user = cursor.fetchone()
    cursor.close()
    if user and _check_password(password, user["password_hash"]):
        user.pop("password_hash", None)
        return user
    return None


def get_user_by_id(conn, user_id: int) -> dict | None:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT id, email, role, status, created_at, updated_at FROM boss_user WHERE id = %s", (user_id,))
    user = cursor.fetchone()
    cursor.close()
    return user


def list_users(conn, page: int = 1, page_size: int = 20) -> dict:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    offset = (page - 1) * page_size
    cursor.execute("SELECT COUNT(*) AS total FROM boss_user")
    total = cursor.fetchone()["total"]
    cursor.execute(
        "SELECT id, email, role, status, created_at, updated_at FROM boss_user ORDER BY id DESC LIMIT %s OFFSET %s",
        (page_size, offset),
    )
    users = cursor.fetchall()
    cursor.close()
    return {"total": total, "page": page, "page_size": page_size, "list": users}


def update_user(conn, user_id: int, **kwargs) -> bool:
    allowed = {"role", "status"}
    fields = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in fields)
    values = list(fields.values()) + [user_id]
    cursor = conn.cursor()
    cursor.execute(f"UPDATE boss_user SET {set_clause} WHERE id = %s", values)
    conn.commit()
    cursor.close()
    return True


# ── api token CRUD ───────────────────────────────────────

def create_api_token(conn, user_id: int, app_name: str = "") -> dict:
    app_id = _gen_app_id()
    app_key = _gen_app_key()
    app_secret = _gen_app_secret()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        "INSERT INTO boss_api_token (user_id, app_name, app_id, app_key, app_secret) VALUES (%s, %s, %s, %s, %s)",
        (user_id, app_name, app_id, app_key, app_secret),
    )
    conn.commit()
    token_id = cursor.lastrowid

    # Create default rate limits
    cursor.execute(
        "INSERT INTO boss_rate_limit_config (app_id, endpoint_group, per_minute, per_month) VALUES (%s, 'quote', 60, 300000)",
        (app_id,),
    )
    cursor.execute(
        "INSERT INTO boss_rate_limit_config (app_id, endpoint_group, per_minute, per_month) VALUES (%s, 'build', 30, 300000)",
        (app_id,),
    )
    conn.commit()
    cursor.close()
    return {"id": token_id, "user_id": user_id, "app_name": app_name, "app_id": app_id, "app_key": app_key, "app_secret": app_secret}


def list_api_tokens_by_user(conn, user_id: int) -> list:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        "SELECT id, user_id, app_name, app_id, app_key, status, created_at, updated_at FROM boss_api_token WHERE user_id = %s ORDER BY id DESC",
        (user_id,),
    )
    tokens = cursor.fetchall()
    cursor.close()
    return tokens


def list_all_api_tokens(conn, page: int = 1, page_size: int = 20) -> dict:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    offset = (page - 1) * page_size
    cursor.execute("SELECT COUNT(*) AS total FROM boss_api_token")
    total = cursor.fetchone()["total"]
    cursor.execute(
        """SELECT t.id, t.user_id, u.email, t.app_name, t.app_id, t.app_key, t.status, t.created_at, t.updated_at
           FROM boss_api_token t LEFT JOIN boss_user u ON t.user_id = u.id
           ORDER BY t.id DESC LIMIT %s OFFSET %s""",
        (page_size, offset),
    )
    tokens = cursor.fetchall()
    cursor.close()
    return {"total": total, "page": page, "page_size": page_size, "list": tokens}


def get_api_token_by_app_id(conn, app_id: str) -> dict | None:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM boss_api_token WHERE app_id = %s", (app_id,))
    token = cursor.fetchone()
    cursor.close()
    return token


def get_api_token_detail(conn, token_id: int, user_id: int = None) -> dict | None:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    if user_id:
        cursor.execute("SELECT * FROM boss_api_token WHERE id = %s AND user_id = %s", (token_id, user_id))
    else:
        cursor.execute("SELECT * FROM boss_api_token WHERE id = %s", (token_id,))
    token = cursor.fetchone()
    cursor.close()
    return token


def update_api_token(conn, token_id: int, **kwargs) -> bool:
    allowed = {"app_name", "status"}
    fields = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not fields:
        return False
    set_clause = ", ".join(f"{k} = %s" for k in fields)
    values = list(fields.values()) + [token_id]
    cursor = conn.cursor()
    cursor.execute(f"UPDATE boss_api_token SET {set_clause} WHERE id = %s", values)
    conn.commit()
    cursor.close()
    return True


def reset_api_key(conn, token_id: int) -> dict | None:
    new_key = _gen_app_key()
    new_secret = _gen_app_secret()
    cursor = conn.cursor()
    cursor.execute("UPDATE boss_api_token SET app_key = %s, app_secret = %s WHERE id = %s", (new_key, new_secret, token_id))
    conn.commit()
    cursor.close()
    return {"app_key": new_key, "app_secret": new_secret}


# ── rate limit config ────────────────────────────────────

def get_rate_limits(conn, app_id: str) -> list:
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM boss_rate_limit_config WHERE app_id = %s", (app_id,))
    configs = cursor.fetchall()
    cursor.close()
    return configs


def upsert_rate_limit(conn, app_id: str, endpoint_group: str, per_minute: int, per_month: int) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO boss_rate_limit_config (app_id, endpoint_group, per_minute, per_month)
           VALUES (%s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE per_minute = VALUES(per_minute), per_month = VALUES(per_month)""",
        (app_id, endpoint_group, per_minute, per_month),
    )
    conn.commit()
    cursor.close()
    return True
