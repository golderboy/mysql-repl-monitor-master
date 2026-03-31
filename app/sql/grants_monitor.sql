CREATE USER IF NOT EXISTS 'monitor_ro'@'10.10.10.50'
  IDENTIFIED BY 'CHANGE_ME_STRONG_PASSWORD';

GRANT SELECT ON hosxp.ovst TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.vn_stat TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.an_stat TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.patient TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.person TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.vn_stat_log TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.rx_operator_log TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.patient_log TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.opitemrece_log TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.lab_entry_log TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.opitemrece TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.lab_head TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.lab_order TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.ipt TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.ovstdiag TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.visit_pttype TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.ovst_seq TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.person_anc TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.person_epi TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.person_labour TO 'monitor_ro'@'10.10.10.50';
GRANT SELECT ON hosxp.surveil_member TO 'monitor_ro'@'10.10.10.50';
GRANT REPLICA MONITOR ON *.* TO 'monitor_ro'@'10.10.10.50';

CREATE USER IF NOT EXISTS 'mon_app'@'127.0.0.1'
  IDENTIFIED BY 'CHANGE_ME_APP_PASSWORD';
CREATE USER IF NOT EXISTS 'mon_web_admin'@'127.0.0.1'
  IDENTIFIED BY 'CHANGE_ME_WEB_PASSWORD';
CREATE USER IF NOT EXISTS 'mon_backup'@'127.0.0.1'
  IDENTIFIED BY 'CHANGE_ME_BACKUP_PASSWORD';

GRANT SELECT, INSERT ON db_monitor.monitor_replication_logs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT ON db_monitor.monitor_table_signature_logs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT ON db_monitor.monitor_schema_diffs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT ON db_monitor.monitor_deep_compare_results TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT ON db_monitor.monitor_incident_events TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT ON db_monitor.monitor_telegram_logs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_check_runs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_schema_runs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_deep_compare_runs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_incidents TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_incident_notes TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_servers TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_settings TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_table_watchlist TO 'mon_app'@'127.0.0.1';

GRANT SELECT ON db_monitor.* TO 'mon_web_admin'@'127.0.0.1';
GRANT INSERT, UPDATE ON db_monitor.monitor_incidents TO 'mon_web_admin'@'127.0.0.1';
GRANT INSERT, UPDATE ON db_monitor.monitor_incident_notes TO 'mon_web_admin'@'127.0.0.1';
GRANT INSERT, UPDATE ON db_monitor.monitor_settings TO 'mon_web_admin'@'127.0.0.1';
GRANT INSERT, UPDATE ON db_monitor.monitor_table_watchlist TO 'mon_web_admin'@'127.0.0.1';

GRANT SELECT ON db_monitor.* TO 'mon_backup'@'127.0.0.1';
FLUSH PRIVILEGES;
