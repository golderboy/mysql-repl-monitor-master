GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_hourly_lag_stats TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_daily_stats TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_job_runs TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_job_locks TO 'mon_app'@'127.0.0.1';
GRANT SELECT ON db_monitor.monitor_alert_policies TO 'mon_app'@'127.0.0.1';
GRANT SELECT ON db_monitor.monitor_alert_targets TO 'mon_app'@'127.0.0.1';
GRANT INSERT ON db_monitor.monitor_web_audit_logs TO 'mon_app'@'127.0.0.1';

GRANT SELECT ON db_monitor.monitor_hourly_lag_stats TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT ON db_monitor.monitor_daily_stats TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_job_runs TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_alert_policies TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_alert_targets TO 'mon_web_admin'@'127.0.0.1';
GRANT INSERT ON db_monitor.monitor_web_audit_logs TO 'mon_web_admin'@'127.0.0.1';

FLUSH PRIVILEGES;
