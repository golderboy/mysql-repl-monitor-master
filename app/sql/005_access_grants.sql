GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_roles TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_permissions TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE, DELETE ON db_monitor.monitor_role_permissions TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_users TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE, DELETE ON db_monitor.monitor_user_roles TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT, UPDATE ON db_monitor.monitor_config_change_requests TO 'mon_web_admin'@'127.0.0.1';
GRANT SELECT, INSERT ON db_monitor.monitor_web_audit_logs TO 'mon_web_admin'@'127.0.0.1';
FLUSH PRIVILEGES;
