USE db_monitor;

GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_report_exports TO 'mon_app'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_report_exports TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT ON db_monitor.monitor_report_exports TO 'mon_backup'@'127.0.0.1';
