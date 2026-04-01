from __future__ import annotations

import argparse
import os

import pymysql
from pymysql.cursors import DictCursor

NONCRITICAL_INDEXES = {
    "idx_health_type_date": "ALTER TABLE health_records ADD KEY idx_health_type_date (type, local_date)",
    "idx_health_source_date": "ALTER TABLE health_records ADD KEY idx_health_source_date (source_name, local_date)",
    "idx_health_start": "ALTER TABLE health_records ADD KEY idx_health_start (start_at)",
    "idx_hr_dedup": "ALTER TABLE health_records ADD KEY idx_hr_dedup (type, start_at, end_at, source_name)",
    "idx_hr_type_localdate_value": "ALTER TABLE health_records ADD KEY idx_hr_type_localdate_value (type, local_date, value_num)",
}


def db_config() -> dict:
    password = os.getenv("HEALTH_DB_PASSWORD", "")
    if not password:
        raise SystemExit("缺少 HEALTH_DB_PASSWORD 环境变量。")
    return {
        "host": os.getenv("HEALTH_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("HEALTH_DB_PORT", "3306")),
        "user": os.getenv("HEALTH_DB_USER", "root"),
        "password": password,
        "database": os.getenv("HEALTH_DB_NAME", "apple_health"),
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": True,
    }


def existing_indexes(cur) -> set[str]:
    cur.execute("SHOW INDEX FROM health_records")
    return {row["Key_name"] for row in cur.fetchall()}


def print_status(cur) -> None:
    indexes = existing_indexes(cur)
    print("health_records indexes:")
    for name in sorted(indexes):
        print(f"- {name}")


def drop_indexes(cur) -> None:
    indexes = existing_indexes(cur)
    for name in NONCRITICAL_INDEXES:
        if name not in indexes:
            print(f"跳过 {name}，当前不存在")
            continue
        print(f"删除 {name}")
        cur.execute(f"ALTER TABLE health_records DROP INDEX {name}")


def create_indexes(cur) -> None:
    indexes = existing_indexes(cur)
    for name, statement in NONCRITICAL_INDEXES.items():
        if name in indexes:
            print(f"跳过 {name}，当前已存在")
            continue
        print(f"创建 {name}")
        cur.execute(statement)


def main() -> None:
    parser = argparse.ArgumentParser(description="管理 health_records 非关键索引")
    parser.add_argument("action", choices=("status", "drop", "create"))
    args = parser.parse_args()

    conn = pymysql.connect(**db_config())
    try:
        with conn.cursor() as cur:
            if args.action == "status":
                print_status(cur)
            elif args.action == "drop":
                drop_indexes(cur)
                print_status(cur)
            else:
                create_indexes(cur)
                print_status(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
