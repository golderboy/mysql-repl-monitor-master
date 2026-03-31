CREATE DATABASE IF NOT EXISTS db_monitor
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE db_monitor;

CREATE TABLE IF NOT EXISTS monitor_servers (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  server_code VARCHAR(30) NOT NULL,
  server_name VARCHAR(100) NOT NULL,
  role ENUM('MASTER','SLAVE') NOT NULL,
  host VARCHAR(255) NOT NULL,
  port INT NOT NULL DEFAULT 3306,
  db_name VARCHAR(100) NOT NULL,
  username VARCHAR(100) NOT NULL,
  password_enc TEXT NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  sort_order INT NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_monitor_servers_code (server_code)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_settings (
  setting_key VARCHAR(100) NOT NULL,
  setting_value TEXT NOT NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (setting_key)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_table_watchlist (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  db_name VARCHAR(100) NOT NULL,
  table_name VARCHAR(100) NOT NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  priority INT NOT NULL DEFAULT 100,
  compare_strategy ENUM('pk_tail','date_window','business_counter','custom_sql') NOT NULL DEFAULT 'business_counter',
  pk_column VARCHAR(100) NULL,
  updated_at_column VARCHAR(100) NULL,
  date_column VARCHAR(100) NULL,
  where_clause TEXT NULL,
  tail_rows INT NOT NULL DEFAULT 1000,
  signature_sql_override LONGTEXT NULL,
  note TEXT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_watchlist (db_name, table_name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_check_runs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  run_type ENUM('scheduled','manual','schema','deep') NOT NULL,
  started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  finished_at DATETIME(6) NULL,
  status ENUM('running','success','partial','error') NOT NULL DEFAULT 'running',
  triggered_by ENUM('system','user') NOT NULL DEFAULT 'system',
  trigger_user VARCHAR(100) NULL,
  note TEXT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_check_runs_started (started_at),
  KEY idx_check_runs_status (status)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_schema_runs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  finished_at DATETIME(6) NULL,
  triggered_by VARCHAR(100) NOT NULL,
  target_server_id BIGINT UNSIGNED NOT NULL,
  status ENUM('running','success','partial','error') NOT NULL DEFAULT 'running',
  summary TEXT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_schema_runs_server (target_server_id),
  CONSTRAINT fk_schema_runs_server FOREIGN KEY (target_server_id) REFERENCES monitor_servers(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_deep_compare_runs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  finished_at DATETIME(6) NULL,
  triggered_by VARCHAR(100) NOT NULL,
  server_id BIGINT UNSIGNED NOT NULL,
  db_name VARCHAR(100) NOT NULL,
  table_name VARCHAR(100) NOT NULL,
  pk_column VARCHAR(100) NOT NULL,
  compare_scope VARCHAR(255) NULL,
  chunk_size INT NOT NULL DEFAULT 10000,
  status ENUM('running','success','partial','error') NOT NULL DEFAULT 'running',
  summary TEXT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_deep_runs_server (server_id),
  KEY idx_deep_runs_table (db_name, table_name),
  CONSTRAINT fk_deep_runs_server FOREIGN KEY (server_id) REFERENCES monitor_servers(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_incidents (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  incident_code VARCHAR(50) NOT NULL,
  issue_type VARCHAR(50) NOT NULL,
  severity ENUM('INFO','WARNING','CRITICAL') NOT NULL,
  server_id BIGINT UNSIGNED NULL,
  db_name VARCHAR(100) NULL,
  table_name VARCHAR(100) NULL,
  object_type VARCHAR(50) NULL,
  object_name VARCHAR(255) NULL,
  first_detected_at DATETIME(6) NOT NULL,
  last_detected_at DATETIME(6) NOT NULL,
  recovered_at DATETIME(6) NULL,
  closed_at DATETIME(6) NULL,
  current_status ENUM('OPEN','ACKNOWLEDGED','INVESTIGATING','RECOVERED','CLOSED') NOT NULL DEFAULT 'OPEN',
  occurrence_count INT NOT NULL DEFAULT 1,
  system_summary TEXT NULL,
  root_cause TEXT NULL,
  corrective_action TEXT NULL,
  summary_result TEXT NULL,
  owner VARCHAR(100) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_incident_code (incident_code),
  KEY idx_incident_status (current_status),
  KEY idx_incident_issue (issue_type),
  KEY idx_incident_server (server_id),
  CONSTRAINT fk_incident_server FOREIGN KEY (server_id) REFERENCES monitor_servers(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_incident_notes (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  incident_id BIGINT UNSIGNED NOT NULL,
  note_type ENUM('ROOT_CAUSE','CORRECTIVE_ACTION','SUMMARY','GENERAL') NOT NULL DEFAULT 'GENERAL',
  note_text LONGTEXT NOT NULL,
  created_by VARCHAR(100) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_incident_notes_incident (incident_id),
  CONSTRAINT fk_incident_notes_incident FOREIGN KEY (incident_id) REFERENCES monitor_incidents(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_replication_logs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  check_run_id BIGINT UNSIGNED NOT NULL,
  server_id BIGINT UNSIGNED NOT NULL,
  checked_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  is_connected TINYINT(1) NOT NULL DEFAULT 0,
  slave_io_running VARCHAR(10) NULL,
  slave_sql_running VARCHAR(10) NULL,
  seconds_behind_master BIGINT NULL,
  master_log_file VARCHAR(255) NULL,
  read_master_log_pos BIGINT NULL,
  exec_master_log_pos BIGINT NULL,
  relay_master_log_file VARCHAR(255) NULL,
  last_io_errno INT NULL,
  last_io_error TEXT NULL,
  last_sql_errno INT NULL,
  last_sql_error TEXT NULL,
  sql_running_state VARCHAR(255) NULL,
  health_status ENUM('ok','warn','critical','error') NOT NULL,
  prev_event_hash CHAR(64) NULL,
  event_hash CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_repl_logs_run (check_run_id),
  KEY idx_repl_logs_server_time (server_id, checked_at),
  CONSTRAINT fk_repl_logs_run FOREIGN KEY (check_run_id) REFERENCES monitor_check_runs(id),
  CONSTRAINT fk_repl_logs_server FOREIGN KEY (server_id) REFERENCES monitor_servers(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_table_signature_logs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  check_run_id BIGINT UNSIGNED NOT NULL,
  server_id BIGINT UNSIGNED NOT NULL,
  db_name VARCHAR(100) NOT NULL,
  table_name VARCHAR(100) NOT NULL,
  checked_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  compare_strategy VARCHAR(50) NOT NULL,
  signature_json LONGTEXT NOT NULL,
  signature_hash CHAR(64) NOT NULL,
  result_status ENUM('match','mismatch','error') NOT NULL,
  diff_summary TEXT NULL,
  error_message TEXT NULL,
  prev_event_hash CHAR(64) NULL,
  event_hash CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_sig_logs_run (check_run_id),
  KEY idx_sig_logs_server_table (server_id, db_name, table_name, checked_at),
  CONSTRAINT fk_sig_logs_run FOREIGN KEY (check_run_id) REFERENCES monitor_check_runs(id),
  CONSTRAINT fk_sig_logs_server FOREIGN KEY (server_id) REFERENCES monitor_servers(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_schema_diffs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  schema_run_id BIGINT UNSIGNED NOT NULL,
  server_id BIGINT UNSIGNED NOT NULL,
  db_name VARCHAR(100) NOT NULL,
  table_name VARCHAR(100) NOT NULL,
  diff_type VARCHAR(50) NOT NULL,
  object_type VARCHAR(50) NOT NULL,
  object_name VARCHAR(255) NULL,
  master_value LONGTEXT NULL,
  slave_value LONGTEXT NULL,
  diff_summary TEXT NULL,
  prev_event_hash CHAR(64) NULL,
  event_hash CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_schema_diffs_run (schema_run_id),
  KEY idx_schema_diffs_server_table (server_id, db_name, table_name),
  CONSTRAINT fk_schema_diffs_run FOREIGN KEY (schema_run_id) REFERENCES monitor_schema_runs(id),
  CONSTRAINT fk_schema_diffs_server FOREIGN KEY (server_id) REFERENCES monitor_servers(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_deep_compare_results (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  deep_run_id BIGINT UNSIGNED NOT NULL,
  chunk_no INT NOT NULL,
  pk_start VARCHAR(255) NULL,
  pk_end VARCHAR(255) NULL,
  master_hash CHAR(64) NULL,
  slave_hash CHAR(64) NULL,
  master_count BIGINT NULL,
  slave_count BIGINT NULL,
  result_status ENUM('match','mismatch','error') NOT NULL,
  diff_summary TEXT NULL,
  prev_event_hash CHAR(64) NULL,
  event_hash CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_deep_results_run (deep_run_id),
  KEY idx_deep_results_status (result_status),
  CONSTRAINT fk_deep_results_run FOREIGN KEY (deep_run_id) REFERENCES monitor_deep_compare_runs(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_incident_events (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  incident_id BIGINT UNSIGNED NOT NULL,
  event_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  event_type ENUM('detected','telegram_sent','acknowledged','escalated','recovered','closed','manual_note') NOT NULL,
  old_status VARCHAR(50) NULL,
  new_status VARCHAR(50) NULL,
  message LONGTEXT NULL,
  created_by VARCHAR(100) NOT NULL DEFAULT 'system',
  prev_event_hash CHAR(64) NULL,
  event_hash CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_incident_events_incident_time (incident_id, event_time),
  CONSTRAINT fk_incident_events_incident FOREIGN KEY (incident_id) REFERENCES monitor_incidents(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS monitor_telegram_logs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  incident_id BIGINT UNSIGNED NULL,
  sent_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  chat_id VARCHAR(100) NOT NULL,
  message_text LONGTEXT NOT NULL,
  telegram_message_id VARCHAR(100) NULL,
  send_status ENUM('success','failed') NOT NULL,
  error_message TEXT NULL,
  prev_event_hash CHAR(64) NULL,
  event_hash CHAR(64) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_telegram_logs_sent (sent_at),
  KEY idx_telegram_logs_incident (incident_id),
  CONSTRAINT fk_telegram_logs_incident FOREIGN KEY (incident_id) REFERENCES monitor_incidents(id)
) ENGINE=InnoDB;

DELIMITER $$

CREATE TRIGGER trg_no_update_monitor_replication_logs
BEFORE UPDATE ON monitor_replication_logs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'UPDATE not allowed on evidence table monitor_replication_logs';
END$$

CREATE TRIGGER trg_no_delete_monitor_replication_logs
BEFORE DELETE ON monitor_replication_logs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DELETE not allowed on evidence table monitor_replication_logs';
END$$

CREATE TRIGGER trg_no_update_monitor_table_signature_logs
BEFORE UPDATE ON monitor_table_signature_logs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'UPDATE not allowed on evidence table monitor_table_signature_logs';
END$$

CREATE TRIGGER trg_no_delete_monitor_table_signature_logs
BEFORE DELETE ON monitor_table_signature_logs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DELETE not allowed on evidence table monitor_table_signature_logs';
END$$

CREATE TRIGGER trg_no_update_monitor_schema_diffs
BEFORE UPDATE ON monitor_schema_diffs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'UPDATE not allowed on evidence table monitor_schema_diffs';
END$$

CREATE TRIGGER trg_no_delete_monitor_schema_diffs
BEFORE DELETE ON monitor_schema_diffs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DELETE not allowed on evidence table monitor_schema_diffs';
END$$

CREATE TRIGGER trg_no_update_monitor_deep_compare_results
BEFORE UPDATE ON monitor_deep_compare_results FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'UPDATE not allowed on evidence table monitor_deep_compare_results';
END$$

CREATE TRIGGER trg_no_delete_monitor_deep_compare_results
BEFORE DELETE ON monitor_deep_compare_results FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DELETE not allowed on evidence table monitor_deep_compare_results';
END$$

CREATE TRIGGER trg_no_update_monitor_incident_events
BEFORE UPDATE ON monitor_incident_events FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'UPDATE not allowed on evidence table monitor_incident_events';
END$$

CREATE TRIGGER trg_no_delete_monitor_incident_events
BEFORE DELETE ON monitor_incident_events FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DELETE not allowed on evidence table monitor_incident_events';
END$$

CREATE TRIGGER trg_no_update_monitor_telegram_logs
BEFORE UPDATE ON monitor_telegram_logs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'UPDATE not allowed on evidence table monitor_telegram_logs';
END$$

CREATE TRIGGER trg_no_delete_monitor_telegram_logs
BEFORE DELETE ON monitor_telegram_logs FOR EACH ROW
BEGIN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DELETE not allowed on evidence table monitor_telegram_logs';
END$$

DELIMITER ;
