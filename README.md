# HOSxP Replication Monitor

README ฉบับนี้ใช้คำศัพท์เดียวกับ **รายงานสรุประบบพัฒนา** และ **คู่มือติดตั้งสำหรับ Admin** เพื่อให้ Dev, IT/DBA และ Operator อ่านตรงกัน
![alt text](https://github.com/golderboy/mysql-repl-monitor-master/blob/main/docs/002.png?raw=true)
![alt text](https://github.com/golderboy/mysql-repl-monitor-master/blob/main/docs/005.png?raw=true)
![alt text](https://github.com/golderboy/mysql-repl-monitor-master/blob/main/docs/009.png?raw=true)
![alt text](https://github.com/golderboy/mysql-repl-monitor-master/blob/main/docs/011.png?raw=true)
![alt text](https://github.com/golderboy/mysql-repl-monitor-master/blob/main/docs/015.png?raw=true)

## 1. วัตถุประสงค์
ระบบนี้ใช้เพื่อตรวจสอบความสอดคล้องของ MySQL Replication สำหรับ HOSxP / MariaDB โดยยึดหลัก read-only ไปยัง production, append-only สำหรับ evidence, และพัฒนาต่อบน baseline จริงเท่านั้น

## 2. Baseline ที่ยึดใช้งาน
- Web app: Flask + Gunicorn (`webapp:app`)
- Reverse proxy: Apache HTTPS -> `127.0.0.1:18080`
- Monitor DB: `db_monitor` local MariaDB
- Cycle worker: `run_cycle.py`
- Summary/Worker: `run_summary.py`, `run_worker.py`
- Access control: DB-backed users/roles + login + audit
- Backup: runtime-safe backup ด้วย `mon_backup`

## 3. ข้อกำกับหลัก
- ห้าม monitor เขียนกลับ production
- ห้ามใช้ root ใน production runtime
- ห้ามเดา user/host/GRANT
- migration ต้อง additive ก่อนเสมอ
- ห้าม rewrite route เดิม, `app/config.py`, `app/db.py`, หรือ service entrypoint โดยไม่มี rollback step

## 4. โครงสร้างหลัก
```text
/opt/mysql-repl-monitor/
├─ app/
├─ deploy/
├─ scripts/
├─ docs/
├─ venv/
└─ .env
```

## 5. เอกสารในชุดเดียวกัน
- `รายงานสรุประบบพัฒนา` : อธิบายภาพรวมระบบและสถานะปัจจุบัน
- `คู่มือติดตั้งสำหรับ Admin` : ใช้ติดตั้งและส่งมอบระบบบนเครื่องจริง
- `คู่มือการใช้งาน` : ใช้สำหรับ Operator และผู้ใช้งานหน้าเว็บ
- `Phase3_Development_Handoff_HOSxP_Replication_Monitor.docx` : baseline, workstream, test strategy, definition of done

## 6. การติดตั้งแบบย่อ
1. เตรียม Ubuntu VM และ package พื้นฐาน
2. แตก baseline source ไปที่ `/opt/mysql-repl-monitor`
3. สร้าง `venv` และติดตั้ง dependency ตาม baseline
4. คัดลอก `deploy/.env.example` เป็น `.env` แล้วแก้ค่าจริง
5. รัน SQL additive migration บน `db_monitor`
6. ติดตั้ง Apache config และ systemd service/timer
7. รัน smoke test ก่อนส่งต่อ

ดูคำสั่งเต็มใน **คู่มือติดตั้งสำหรับ Admin**

## 7. ชุดคำสั่งตรวจสอบเร็ว
```bash
python3 scripts/verify_patch.py
python3 -m unittest discover -s tests -p "test_*.py"
python3 scripts/check_db_connectivity.py
bash scripts/preflight_check.sh
bash scripts/postflight_check.sh
```

## 8. Service / Timer ที่ต้องมี
```text
mysql-repl-monitor-web.service
mysql-repl-monitor-cycle.service
mysql-repl-monitor-cycle.timer
mysql-repl-monitor-summary.service
mysql-repl-monitor-summary.timer
mysql-repl-monitor-worker.service
mysql-repl-monitor-worker.timer
mysql-repl-monitor-maintenance.service
mysql-repl-monitor-maintenance.timer
mysql-repl-monitor-backup.service
mysql-repl-monitor-backup.timer
```

## 9. คำศัพท์มาตรฐาน
- **Baseline**: ชุดโค้ดและบริการที่ใช้งานจริงบนเซิร์ฟเวอร์
- **Release manifest**: รายการไฟล์ที่ patch แตะจริงพร้อมคำสั่งติดตั้ง/rollback
- **Baseline inventory**: รายการไฟล์ เส้นทาง และบริการที่ใช้จริง
- **Smoke test**: ชุดทดสอบขั้นต่ำหลัง deploy หรือ rollback
- **Monitor DB**: `db_monitor`
- **Executive outputs**: รายงานสำหรับผู้บริหารและ IT ตาม WS4

## 10. หมายเหตุสำคัญ
ระบบนี้ไม่ควรถูกติดตั้งหรือแก้ไขโดยการเดา config จากเครื่องจริง แต่ต้องยึด baseline source, release manifest และเอกสาร handoff ทุกครั้ง
