USE db_monitor;

DROP TABLE IF EXISTS monitor_web_audit_logs;
DROP TABLE IF EXISTS monitor_alert_targets;
DROP TABLE IF EXISTS monitor_alert_policies;
DROP TABLE IF EXISTS monitor_job_locks;
DROP TABLE IF EXISTS monitor_job_runs;
DROP TABLE IF EXISTS monitor_daily_stats;
DROP TABLE IF EXISTS monitor_hourly_lag_stats;

CREATE TABLE monitor_hourly_lag_stats (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  bucket_hour DATETIME NOT NULL,
  server_id BIGINT UNSIGNED NOT NULL,
  server_code VARCHAR(30) NOT NULL,
  lag_max_sec BIGINT NULL,
  lag_avg_sec DECIMAL(12,2) NULL,
  error_count INT NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_hourly_server_bucket (bucket_hour, server_id),
  KEY idx_hourly_server (server_id, bucket_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE monitor_daily_stats (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  stat_date DATE NOT NULL,
  server_id BIGINT UNSIGNED NOT NULL,
  server_code VARCHAR(30) NOT NULL,
  replication_ok_count INT NOT NULL DEFAULT 0,
  replication_warn_count INT NOT NULL DEFAULT 0,
  replication_critical_count INT NOT NULL DEFAULT 0,
  replication_error_count INT NOT NULL DEFAULT 0,
  mismatch_count INT NOT NULL DEFAULT 0,
  incident_opened_count INT NOT NULL DEFAULT 0,
  incident_recovered_count INT NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_daily_server_date (stat_date, server_id),
  KEY idx_daily_server (server_id, stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE monitor_job_runs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  job_name VARCHAR(50) NOT NULL,
  status ENUM('queued','running','success','failed','canceled') NOT NULL DEFAULT 'queued',
  requested_by VARCHAR(100) NOT NULL,
  server_id BIGINT UNSIGNED NULL,
  requested_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  started_at DATETIME(6) NULL,
  finished_at DATETIME(6) NULL,
  progress_percent TINYINT UNSIGNED NOT NULL DEFAULT 0,
  cancel_requested TINYINT(1) NOT NULL DEFAULT 0,
  payload_json LONGTEXT NULL,
  result_json LONGTEXT NULL,
  error_message TEXT NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_job_status_requested (status, requested_at),
  KEY idx_job_name_requested (job_name, requested_at),
  KEY idx_job_server (server_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE monitor_job_locks (
  lock_name VARCHAR(100) NOT NULL,
  run_id BIGINT UNSIGNED NULL,
  acquired_at DATETIME(6) NULL,
  heartbeat_at DATETIME(6) NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (lock_name),
  KEY idx_job_locks_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE monitor_alert_policies (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  policy_name VARCHAR(150) NOT NULL,
  issue_type VARCHAR(50) NOT NULL,
  severity ENUM('INFO','WARNING','CRITICAL') NOT NULL,
  channel VARCHAR(50) NOT NULL DEFAULT 'telegram',
  repeat_minutes INT NOT NULL DEFAULT 30,
  quiet_hours_json TEXT NULL,
  is_enabled TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_alert_policy_name (policy_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE monitor_alert_targets (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  policy_id BIGINT UNSIGNED NOT NULL,
  target_type VARCHAR(50) NOT NULL,
  target_value VARCHAR(255) NOT NULL,
  is_enabled TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_alert_targets_policy (policy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE monitor_web_audit_logs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  actor VARCHAR(100) NOT NULL,
  action VARCHAR(100) NOT NULL,
  object_type VARCHAR(50) NOT NULL,
  object_id BIGINT UNSIGNED NULL,
  details_json LONGTEXT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_web_audit_created (created_at),
  KEY idx_web_audit_actor (actor, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
