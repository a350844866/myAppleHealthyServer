CREATE TABLE IF NOT EXISTS profile (
    id                TINYINT PRIMARY KEY,
    date_of_birth     DATE NULL,
    biological_sex    VARCHAR(32) NULL,
    blood_type        VARCHAR(32) NULL,
    skin_type         VARCHAR(64) NULL,
    cardio_meds_use   VARCHAR(64) NULL,
    export_date_raw   VARCHAR(64) NULL,
    exported_at       DATETIME NULL,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS import_batches (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    batch_type        VARCHAR(32) NOT NULL,
    status            VARCHAR(32) NOT NULL DEFAULT 'running',
    note              VARCHAR(255) NULL,
    started_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at      TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS import_files (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    batch_id          BIGINT NOT NULL,
    file_path         VARCHAR(255) NOT NULL,
    file_size         BIGINT NULL,
    file_mtime        BIGINT NULL,
    import_type       VARCHAR(32) NOT NULL,
    records_seen      BIGINT NOT NULL DEFAULT 0,
    records_inserted  BIGINT NOT NULL DEFAULT 0,
    run_started_records_seen BIGINT NULL,
    run_started_records_inserted BIGINT NULL,
    run_started_at    TIMESTAMP NULL DEFAULT NULL,
    status            VARCHAR(32) NOT NULL DEFAULT 'running',
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at      TIMESTAMP NULL,
    last_progress_at  TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_import_file_version (file_path, file_mtime, import_type),
    KEY idx_import_files_batch (batch_id),
    CONSTRAINT fk_import_files_batch FOREIGN KEY (batch_id) REFERENCES import_batches(id)
);

CREATE TABLE IF NOT EXISTS import_progress_samples (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    import_file_id    BIGINT NOT NULL,
    recorded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    records_seen      BIGINT NOT NULL DEFAULT 0,
    records_inserted  BIGINT NOT NULL DEFAULT 0,
    KEY idx_progress_samples_file_time (import_file_id, recorded_at),
    CONSTRAINT fk_progress_samples_file FOREIGN KEY (import_file_id) REFERENCES import_files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS health_records (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    record_hash       CHAR(64) NOT NULL,
    type              VARCHAR(128) NOT NULL,
    source_name       VARCHAR(255) NULL,
    source_version    VARCHAR(128) NULL,
    device            TEXT NULL,
    unit              VARCHAR(64) NULL,
    value_text        VARCHAR(255) NULL,
    value_num         DOUBLE NULL,
    creation_at       DATETIME NULL,
    start_at          DATETIME NOT NULL,
    end_at            DATETIME NOT NULL,
    local_date        DATE NOT NULL,
    metadata          JSON NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_health_record_hash (record_hash),
    KEY idx_health_type_date (type, local_date),
    KEY idx_health_source_date (source_name, local_date),
    KEY idx_health_start (start_at)
);

CREATE TABLE IF NOT EXISTS workouts (
    id                        BIGINT PRIMARY KEY AUTO_INCREMENT,
    workout_hash              CHAR(64) NOT NULL,
    activity_type             VARCHAR(128) NOT NULL,
    duration                  DOUBLE NULL,
    duration_unit             VARCHAR(32) NULL,
    total_distance            DOUBLE NULL,
    total_distance_unit       VARCHAR(32) NULL,
    total_energy_burned       DOUBLE NULL,
    total_energy_burned_unit  VARCHAR(32) NULL,
    source_name               VARCHAR(255) NULL,
    source_version            VARCHAR(128) NULL,
    device                    TEXT NULL,
    creation_at               DATETIME NULL,
    start_at                  DATETIME NOT NULL,
    end_at                    DATETIME NOT NULL,
    local_date                DATE NOT NULL,
    route_file                VARCHAR(255) NULL,
    created_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_workout_hash (workout_hash),
    KEY idx_workout_type_date (activity_type, local_date),
    KEY idx_workout_start (start_at)
);

CREATE TABLE IF NOT EXISTS workout_statistics (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    statistic_hash    CHAR(64) NOT NULL,
    workout_id        BIGINT NOT NULL,
    type              VARCHAR(128) NOT NULL,
    start_at          DATETIME NULL,
    end_at            DATETIME NULL,
    average_value     DOUBLE NULL,
    minimum_value     DOUBLE NULL,
    maximum_value     DOUBLE NULL,
    sum_value         DOUBLE NULL,
    unit              VARCHAR(64) NULL,
    UNIQUE KEY uq_workout_stat_hash (statistic_hash),
    KEY idx_workout_statistics_workout (workout_id),
    CONSTRAINT fk_workout_statistics_workout FOREIGN KEY (workout_id) REFERENCES workouts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workout_events (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_hash        CHAR(64) NOT NULL,
    workout_id        BIGINT NOT NULL,
    type              VARCHAR(128) NOT NULL,
    event_at          DATETIME NOT NULL,
    duration          DOUBLE NULL,
    duration_unit     VARCHAR(32) NULL,
    UNIQUE KEY uq_workout_event_hash (event_hash),
    KEY idx_workout_events_workout (workout_id),
    CONSTRAINT fk_workout_events_workout FOREIGN KEY (workout_id) REFERENCES workouts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workout_routes (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    route_hash        CHAR(64) NOT NULL,
    file_path         VARCHAR(255) NOT NULL,
    source_name       VARCHAR(255) NULL,
    source_version    VARCHAR(128) NULL,
    device            TEXT NULL,
    creation_at       DATETIME NULL,
    start_at          DATETIME NULL,
    end_at            DATETIME NULL,
    workout_id        BIGINT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_workout_route_hash (route_hash),
    UNIQUE KEY uq_workout_route_file (file_path),
    KEY idx_workout_routes_workout (workout_id),
    CONSTRAINT fk_workout_routes_workout FOREIGN KEY (workout_id) REFERENCES workouts(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS route_points (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    route_id          BIGINT NOT NULL,
    point_index       INT NOT NULL,
    longitude         DOUBLE NOT NULL,
    latitude          DOUBLE NOT NULL,
    elevation         DOUBLE NULL,
    recorded_at       DATETIME NULL,
    speed             DOUBLE NULL,
    course            DOUBLE NULL,
    h_acc             DOUBLE NULL,
    v_acc             DOUBLE NULL,
    UNIQUE KEY uq_route_point (route_id, point_index),
    KEY idx_route_points_time (recorded_at),
    CONSTRAINT fk_route_points_route FOREIGN KEY (route_id) REFERENCES workout_routes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS activity_summaries (
    id                        BIGINT PRIMARY KEY AUTO_INCREMENT,
    summary_date              DATE NOT NULL,
    active_energy_burned      DOUBLE NULL,
    active_energy_burned_goal DOUBLE NULL,
    active_energy_burned_unit VARCHAR(32) NULL,
    apple_move_time           DOUBLE NULL,
    apple_move_time_goal      DOUBLE NULL,
    apple_exercise_time       DOUBLE NULL,
    apple_exercise_time_goal  DOUBLE NULL,
    apple_stand_hours         DOUBLE NULL,
    apple_stand_hours_goal    DOUBLE NULL,
    updated_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_activity_summary_date (summary_date)
);

CREATE TABLE IF NOT EXISTS ecg_readings (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    ecg_hash          CHAR(64) NOT NULL,
    file_name         VARCHAR(255) NOT NULL,
    record_at         DATETIME NOT NULL,
    classification    VARCHAR(128) NULL,
    symptoms          TEXT NULL,
    software_version  VARCHAR(64) NULL,
    device            VARCHAR(128) NULL,
    sample_rate       INT NULL,
    lead_name         VARCHAR(64) NULL,
    unit              VARCHAR(32) NULL,
    voltage_data      JSON NOT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_ecg_hash (ecg_hash),
    UNIQUE KEY uq_ecg_file_name (file_name),
    KEY idx_ecg_record_at (record_at)
);

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
);

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
);

CREATE TABLE IF NOT EXISTS device_sync_anchors (
    device_id       VARCHAR(128) NOT NULL,
    record_type     VARCHAR(128) NOT NULL,
    anchor_value    MEDIUMTEXT NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (device_id, record_type),
    CONSTRAINT fk_device_sync_anchors_state FOREIGN KEY (device_id) REFERENCES device_sync_state(device_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ai_dashboard_reports (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    snapshot_hash     CHAR(64) NOT NULL,
    model             VARCHAR(128) NOT NULL,
    title             VARCHAR(255) NOT NULL,
    summary           TEXT NOT NULL,
    bullets_json      JSON NOT NULL,
    watchouts_json    JSON NOT NULL,
    next_focus_json   JSON NOT NULL,
    confidence        VARCHAR(64) NULL,
    usage_json        JSON NULL,
    snapshot_json     JSON NOT NULL,
    generated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_ai_reports_generated (generated_at),
    KEY idx_ai_reports_snapshot_model_time (snapshot_hash, model, generated_at)
);
