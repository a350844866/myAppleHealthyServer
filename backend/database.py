from __future__ import annotations

import os
import threading
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from .config import DB_POOL_MAX_CACHED, DB_POOL_MAX_CONNECTIONS, DB_POOL_MIN_CACHED

try:
    from dbutils.pooled_db import PooledDB
except ImportError:  # pragma: no cover - compatibility fallback
    PooledDB = None

_pool: PooledDB | None = None
_pool_lock = threading.Lock()


def _set_autocommit(conn, autocommit: bool) -> None:
    setter = getattr(conn, "autocommit", None)
    if callable(setter):
        setter(autocommit)
        return

    inner = getattr(conn, "_con", None)
    inner_setter = getattr(inner, "autocommit", None)
    if callable(inner_setter):
        inner_setter(autocommit)
        return

    with conn.cursor() as cur:
        cur.execute("SET autocommit = %s", (1 if autocommit else 0,))


def db_config() -> dict:
    password = os.getenv("HEALTH_DB_PASSWORD", "")
    if not password:
        raise RuntimeError("缺少 HEALTH_DB_PASSWORD 环境变量。")
    return {
        "host": os.getenv("HEALTH_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("HEALTH_DB_PORT", "3306")),
        "user": os.getenv("HEALTH_DB_USER", "root"),
        "password": password,
        "database": os.getenv("HEALTH_DB_NAME", "apple_health"),
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
    }


def get_pool() -> PooledDB:
    global _pool
    if PooledDB is None:
        raise RuntimeError("DBUtils 未安装，当前无法创建连接池。")
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = PooledDB(
                    creator=pymysql,
                    mincached=DB_POOL_MIN_CACHED,
                    maxcached=DB_POOL_MAX_CACHED,
                    maxconnections=DB_POOL_MAX_CONNECTIONS,
                    blocking=True,
                    ping=1,
                    **db_config(),
                )
    return _pool


@contextmanager
def get_db(*, autocommit: bool = True):
    if PooledDB is None:
        conn = pymysql.connect(**db_config())
    else:
        conn = get_pool().connection()
    _set_autocommit(conn, autocommit)
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()
