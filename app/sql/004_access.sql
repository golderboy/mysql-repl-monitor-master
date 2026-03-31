USE db_monitor;

CREATE TABLE IF NOT EXISTS monitor_roles (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  role_code VARCHAR(50) NOT NULL,
  role_name VARCHAR(100) NOT NULL,
  role_description TEXT NULL,
  is_system TINYINT(1) NOT NULL DEFAULT 1,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_monitor_roles_code (role_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS monitor_permissions (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  permission_code VARCHAR(100) NOT NULL,
  permission_name VARCHAR(150) NOT NULL,
  permission_description TEXT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_monitor_permissions_code (permission_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS monitor_role_permissions (
  role_id BIGINT UNSIGNED NOT NULL,
  permission_id BIGINT UNSIGNED NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (role_id, permission_id),
  KEY idx_mrp_permission (permission_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS monitor_users (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  username VARCHAR(100) NOT NULL,
  display_name VARCHAR(150) NULL,
  password_hash VARCHAR(255) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  require_password_change TINYINT(1) NOT NULL DEFAULT 0,
  last_login_at DATETIME(6) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_monitor_users_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS monitor_user_roles (
  user_id BIGINT UNSIGNED NOT NULL,
  role_id BIGINT UNSIGNED NOT NULL,
  assigned_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (user_id, role_id),
  KEY idx_mur_role (role_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS monitor_config_change_requests (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  request_type VARCHAR(30) NOT NULL,
  target_key VARCHAR(255) NULL,
  payload_json LONGTEXT NOT NULL,
  requested_by VARCHAR(100) NOT NULL,
  reason_text TEXT NULL,
  status ENUM('pending','approved','rejected','applied') NOT NULL DEFAULT 'pending',
  approved_by VARCHAR(100) NULL,
  approved_at DATETIME(6) NULL,
  applied_at DATETIME(6) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_mccr_status_created (status, created_at),
  KEY idx_mccr_requested_by (requested_by, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO monitor_roles (role_code, role_name, role_description, is_system) VALUES
('viewer', 'Viewer', 'ดู dashboard, incidents, trends, jobs, alerts, settings แบบอ่านอย่างเดียว', 1),
('operator', 'Operator', 'จัดการ incident, กดงาน manual, ขอเปลี่ยน config', 1),
('maintainer', 'Maintainer', 'งาน maintenance และ operational actions เพิ่มเติม', 1),
('admin', 'Admin', 'จัดการผู้ใช้ สิทธิ์ อนุมัติ config และจัดการระบบทั้งหมด', 1);

INSERT IGNORE INTO monitor_permissions (permission_code, permission_name, permission_description) VALUES
('dashboard.view', 'View dashboard', 'ดูหน้า dashboard และ mismatches'),
('incidents.view', 'View incidents', 'ดู incident และรายละเอียด'),
('incidents.manage', 'Manage incidents', 'แก้สถานะ incident และเพิ่ม note'),
('trends.view', 'View trends', 'ดู trend และ analytics'),
('jobs.view', 'View jobs', 'ดู queue และประวัติงาน'),
('jobs.manage', 'Manage jobs', 'enqueue/cancel job และสั่ง schema/deep compare'),
('alerts.view', 'View alerts', 'ดู policy และ target ของ alert'),
('alerts.manage', 'Manage alerts', 'เพิ่ม/แก้ policy และ target ของ alert'),
('settings.view', 'View settings', 'ดู settings และ watchlist'),
('settings.manage', 'Manage settings', 'แก้ settings/watchlist โดยตรง'),
('settings.request', 'Request config change', 'ส่งคำขอเปลี่ยน settings/watchlist'),
('settings.approve', 'Approve config change', 'อนุมัติหรือปฏิเสธคำขอเปลี่ยน config'),
('maintenance.run', 'Run maintenance', 'รัน cleanup และ evidence export'),
('access.view', 'View access', 'ดูผู้ใช้ บทบาท และคำขอเปลี่ยน config'),
('access.manage', 'Manage access', 'สร้างผู้ใช้ รีเซ็ตรหัสผ่าน และกำหนดบทบาท');

INSERT IGNORE INTO monitor_role_permissions (role_id, permission_id)
SELECT r.id, p.id
  FROM monitor_roles r
  INNER JOIN monitor_permissions p
    ON (
      (r.role_code = 'viewer' AND p.permission_code IN ('dashboard.view','incidents.view','trends.view','jobs.view','alerts.view','settings.view'))
      OR (r.role_code = 'operator' AND p.permission_code IN ('dashboard.view','incidents.view','incidents.manage','trends.view','jobs.view','jobs.manage','alerts.view','settings.view','settings.request'))
      OR (r.role_code = 'maintainer' AND p.permission_code IN ('dashboard.view','incidents.view','incidents.manage','trends.view','jobs.view','jobs.manage','alerts.view','alerts.manage','settings.view','settings.request','maintenance.run','access.view'))
      OR (r.role_code = 'admin' AND p.permission_code IN ('dashboard.view','incidents.view','incidents.manage','trends.view','jobs.view','jobs.manage','alerts.view','alerts.manage','settings.view','settings.manage','settings.request','settings.approve','maintenance.run','access.view','access.manage'))
    );
