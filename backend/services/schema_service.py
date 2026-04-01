from __future__ import annotations

from backend.database import get_db

INGEST_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS ingest_events (
        id                  BIGINT PRIMARY KEY AUTO_INCREMENT,
        device_id           VARCHAR(128) NOT NULL,
        bundle_id           VARCHAR(255) NOT NULL,
        sent_at             DATETIME NOT NULL,
        received_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        item_count          INT NOT NULL DEFAULT 0,
        accepted_count      INT NOT NULL DEFAULT 0,
        deduplicated_count  INT NOT NULL DEFAULT 0,
        status              VARCHAR(32) NOT NULL DEFAULT 'received',
        error_message       VARCHAR(255) NULL,
        payload_json        JSON NOT NULL,
        KEY idx_ingest_events_device (device_id, received_at),
        KEY idx_ingest_events_status (status, received_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS device_sync_state (
        device_id               VARCHAR(128) PRIMARY KEY,
        bundle_id               VARCHAR(255) NOT NULL,
        last_seen_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_sent_at            DATETIME NULL,
        last_sync_at            DATETIME NULL,
        last_sync_status        VARCHAR(32) NOT NULL DEFAULT 'pending',
        last_error_message      VARCHAR(255) NULL,
        last_items_count        INT NOT NULL DEFAULT 0,
        last_accepted_count     INT NOT NULL DEFAULT 0,
        last_deduplicated_count INT NOT NULL DEFAULT 0,
        updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_device_sync_updated (updated_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS device_sync_anchors (
        device_id       VARCHAR(128) NOT NULL,
        record_type     VARCHAR(128) NOT NULL,
        anchor_value    MEDIUMTEXT NOT NULL,
        updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (device_id, record_type),
        CONSTRAINT fk_device_sync_anchors_state FOREIGN KEY (device_id) REFERENCES device_sync_state(device_id) ON DELETE CASCADE
    )
    """,
]


def ensure_ingest_tables(cur) -> None:
    for statement in INGEST_TABLE_STATEMENTS:
        cur.execute(statement)


def ensure_import_status_schema(cur) -> None:
    runtime_columns = {
        "last_progress_at": "ADD COLUMN last_progress_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
        "run_started_records_seen": "ADD COLUMN run_started_records_seen BIGINT NULL",
        "run_started_records_inserted": "ADD COLUMN run_started_records_inserted BIGINT NULL",
        "run_started_at": "ADD COLUMN run_started_at TIMESTAMP NULL DEFAULT NULL",
    }
    for column_name, ddl in runtime_columns.items():
        cur.execute(
            """
            SELECT COUNT(*) AS count
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'import_files'
              AND column_name = %s
            """,
            (column_name,),
        )
        if cur.fetchone()["count"]:
            continue
        cur.execute(f"ALTER TABLE import_files {ddl}")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS import_progress_samples (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            import_file_id BIGINT NOT NULL,
            recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            records_seen BIGINT NOT NULL DEFAULT 0,
            records_inserted BIGINT NOT NULL DEFAULT 0,
            KEY idx_progress_samples_file_time (import_file_id, recorded_at),
            CONSTRAINT fk_progress_samples_file FOREIGN KEY (import_file_id) REFERENCES import_files(id) ON DELETE CASCADE
        )
        """
    )


def ensure_dashboard_ai_schema(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_dashboard_reports (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            snapshot_hash CHAR(64) NOT NULL,
            model VARCHAR(128) NOT NULL,
            title VARCHAR(255) NOT NULL,
            summary TEXT NOT NULL,
            bullets_json JSON NOT NULL,
            watchouts_json JSON NOT NULL,
            next_focus_json JSON NOT NULL,
            confidence VARCHAR(64) NULL,
            usage_json JSON NULL,
            snapshot_json JSON NOT NULL,
            generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_ai_reports_generated (generated_at),
            KEY idx_ai_reports_snapshot_model_time (snapshot_hash, model, generated_at)
        )
        """
    )


def ensure_summary_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS system_summary (
            summary_key VARCHAR(64) PRIMARY KEY,
            summary_json JSON NOT NULL,
            refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_system_summary_refreshed (refreshed_at)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS record_type_stats (
            type VARCHAR(128) PRIMARY KEY,
            record_count BIGINT NOT NULL DEFAULT 0,
            first_date DATE NULL,
            last_date DATE NULL,
            refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_record_type_stats_count (record_count, last_date),
            KEY idx_record_type_stats_refreshed (refreshed_at)
        )
        """
    )


def ensure_runtime_schema() -> None:
    with get_db() as db, db.cursor() as cur:
        ensure_import_status_schema(cur)
        ensure_ingest_tables(cur)
        ensure_dashboard_ai_schema(cur)
        ensure_summary_tables(cur)
