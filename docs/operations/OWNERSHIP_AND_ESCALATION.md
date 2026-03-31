# Ownership And Escalation

## Required owners
- dev owner: ______________________________
- system/DBA owner: _______________________
- operator owner: _________________________

## Contact matrix
| Incident type | Primary owner | Backup owner | ช่องทางติดต่อ | SLA ตอบรับ |
|---|---|---|---|---|
| web/app error | dev owner | system/DBA owner | __________________ | ______ |
| monitor DB / backup error | system/DBA owner | dev owner | __________________ | ______ |
| production replication alert | operator owner | system/DBA owner | __________________ | ______ |
| access/audit issue | dev owner | operator owner | __________________ | ______ |

## Escalation rules
- incident ที่กระทบ production monitoring ให้ operator owner แจ้ง system/DBA owner ทันที
- ถ้าเกี่ยวกับ patch/release regression ให้ dev owner เป็นคนตัดสิน rollback
- ถ้าเกี่ยวกับ DB restore/import ให้ทำโดย system/DBA owner เท่านั้น
- operator ไม่มีสิทธิ์ bypass approval flow หรือแก้ค่าที่ต้อง approval เอง

## Review cadence
- รายสัปดาห์ review backup status, failed jobs, audit events ที่ผิดปกติ
- รายเดือน review users/roles, config drift, retention, export space

## Release governance
- release window: __________________________________________
- emergency rollback rule: ใช้ baseline package ล่าสุดที่ verify แล้วเท่านั้น
- baseline package ต้องเก็บทั้งบน server และเครื่อง dev ที่ใช้เป็น source จริง
