USE db_monitor;

ALTER TABLE monitor_users
  ADD COLUMN IF NOT EXISTS failed_login_count INT NOT NULL DEFAULT 0 AFTER require_password_change,
  ADD COLUMN IF NOT EXISTS locked_until DATETIME(6) NULL AFTER failed_login_count,
  ADD COLUMN IF NOT EXISTS password_changed_at DATETIME(6) NULL AFTER locked_until;

ALTER TABLE monitor_users
  ADD KEY IF NOT EXISTS idx_monitor_users_locked_until (locked_until),
  ADD KEY IF NOT EXISTS idx_monitor_users_is_active (is_active, locked_until);

ALTER TABLE monitor_web_audit_logs
  ADD KEY IF NOT EXISTS idx_mwal_actor_created (actor, created_at),
  ADD KEY IF NOT EXISTS idx_mwal_action_created (action, created_at);
