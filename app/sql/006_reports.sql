USE db_monitor;

CREATE TABLE IF NOT EXISTS monitor_report_exports (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  job_run_id BIGINT UNSIGNED NOT NULL,
  report_type ENUM('daily','weekly','monthly') NOT NULL,
  export_format ENUM('csv','pdf') NOT NULL,
  period_days INT NOT NULL DEFAULT 0,
  requested_by VARCHAR(100) NOT NULL,
  status ENUM('queued','running','success','failed','canceled') NOT NULL DEFAULT 'queued',
  file_name VARCHAR(255) NULL,
  file_relpath VARCHAR(500) NULL,
  file_size_bytes BIGINT UNSIGNED NULL,
  sha256_hex CHAR(64) NULL,
  request_payload_json LONGTEXT NULL,
  manifest_json LONGTEXT NULL,
  error_message TEXT NULL,
  generated_at DATETIME(6) NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  UNIQUE KEY uk_monitor_report_exports_job_run (job_run_id),
  KEY idx_monitor_report_exports_status_created (status, created_at),
  KEY idx_monitor_report_exports_type_created (report_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO monitor_permissions (permission_code, permission_name, permission_description) VALUES
('reports.view', 'View reports', 'ดูหน้ารายงานและดาวน์โหลดไฟล์ export'),
('reports.manage', 'Manage reports', 'สั่ง queue รายงานและ export CSV/PDF');

INSERT IGNORE INTO monitor_role_permissions (role_id, permission_id)
SELECT r.id, p.id
  FROM monitor_roles r
  INNER JOIN monitor_permissions p
    ON (
      (r.role_code = 'viewer' AND p.permission_code IN ('reports.view'))
      OR (r.role_code = 'operator' AND p.permission_code IN ('reports.view','reports.manage'))
      OR (r.role_code = 'maintainer' AND p.permission_code IN ('reports.view','reports.manage'))
      OR (r.role_code = 'admin' AND p.permission_code IN ('reports.view','reports.manage'))
    );
