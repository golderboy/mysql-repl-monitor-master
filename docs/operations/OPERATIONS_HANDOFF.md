# Operations Handoff

เอกสารนี้ใช้เป็น operations handoff ฉบับสั้นสำหรับ IT/DBA/Operator หลังปิด Phase 3

## Scope
- baseline นี้คือ Flask + Gunicorn หลัง Apache reverse proxy
- monitor DB คือ `db_monitor` บน MariaDB local
- production access เป็น read-only เท่านั้น
- งานรอบหลักยังคงเป็น `run_cycle` เดิม และงาน summary/worker/report เป็น additive ต่อจาก baseline

## startup order
1. ตรวจ `.env` และสิทธิ์ไฟล์ (`600`) ให้ถูกต้อง
2. ตรวจ MariaDB local พร้อมใช้งาน
3. ตรวจ Apache config และ certificate/key
4. start `mysql-repl-monitor-web.service`
5. enable/start timers: cycle, summary, worker, maintenance, backup
6. รัน `python scripts/verify_patch.py`
7. รัน `python scripts/check_db_connectivity.py --fail-missing-password`
8. รัน `python scripts/final_handoff_check.py`
9. รัน `VERIFY_DB_CONNECTIVITY=1 python scripts/run_manual_smoke.sh`

## Runtime services/timers
- `mysql-repl-monitor-web.service`
- `mysql-repl-monitor-cycle.service` + `.timer`
- `mysql-repl-monitor-summary.service` + `.timer`
- `mysql-repl-monitor-worker.service` + `.timer`
- `mysql-repl-monitor-maintenance.service` + `.timer`
- `mysql-repl-monitor-backup.service` + `.timer`

## recovery step
- ถ้า patch ใหม่ fail ระหว่าง deploy ให้ rollback เฉพาะก้อนที่เพิ่งลง ไม่ทับทั้ง tree
- restore source จาก baseline package/manifest เดิม
- apply systemd/apache files กลับตาม release manifest
- restart เฉพาะ service ที่ก้อนนั้นแตะ
- รัน `scripts/postflight_check.sh` และ `scripts/run_manual_smoke.sh`
- ถ้าเกี่ยวกับฐานข้อมูล ให้หยุดที่ migration boundary ล่าสุดและอ้าง rollback note ของ patch นั้น

## log path
- Apache access/error log: `${APACHE_LOG_DIR}/mysql-repl-monitor-access.log`, `${APACHE_LOG_DIR}/mysql-repl-monitor-error.log`
- app/service log: `journalctl -u mysql-repl-monitor-web.service`
- timer/job log: `journalctl -u mysql-repl-monitor-cycle.service`, `journalctl -u mysql-repl-monitor-summary.service`, `journalctl -u mysql-repl-monitor-worker.service`
- backup artifacts: `$APP_ROOT/backups/`
- export/report artifacts: `$APP_ROOT/exports/`
- config diff artifacts: `$APP_ROOT/run/config_diff/`

## Routine review
- รายวัน: failed jobs, report export status, incidents รุนแรง, backup completion
- รายสัปดาห์: backup status, failed jobs, audit events ที่ผิดปกติ, config diff review
- รายเดือน: retention cleanup, export space usage, role/user review, TLS/certificate อายุคงเหลือ

## TLS renewal
- ตรวจวันหมดอายุของ certificate ก่อนหมดอายุอย่างน้อย 30 วัน
- update path ใน Apache เฉพาะเมื่อมีการเปลี่ยนไฟล์จริง
- `apachectl configtest` ก่อน reload ทุกครั้ง
- reload Apache แล้วรัน browser smoke ที่ `/login` และ `/health`

## Release window และ rollback rule
- release window ใช้ช่วงเวลาที่ DBA/IT/operator อยู่พร้อมกัน
- ห้ามลงหลาย workstream พร้อมกันถ้ายังไม่ผ่าน smoke ของก้อนก่อน
- emergency rollback ใช้ baseline release ล่าสุดที่ verify แล้วเท่านั้น
- ต้องบันทึกผล deploy, smoke, rollback ลง release notes ทุกครั้ง
