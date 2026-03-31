# Phase 3 Closeout Checklist

## Baseline freeze
- [ ] baseline freeze เสร็จและเก็บ package ไว้ทั้ง dev/server
- [ ] release manifest ล่าสุดตรงกับไฟล์จริง
- [ ] release notes ของ WS1-WS6 ครบ

## Code / patch
- [ ] patch ทุกก้อน deploy แยกและ rollback แยกได้
- [ ] verify patch ผ่าน
- [ ] test suite ผ่าน
- [ ] final_handoff_check ผ่าน

## Database / backup
- [ ] migration additive ทั้งหมด apply แล้ว
- [ ] backup/restore evidence ครบ
- [ ] DBA restore drill record ล่าสุดแนบไว้
- [ ] ไม่มี production runtime script ใช้ root

## Operations handoff
- [ ] operations handoff ส่งให้ IT/DBA/operator แล้ว
- [ ] startup order, recovery step, log path, timer list, TLS renewal ระบุครบ
- [ ] owner/contact matrix ระบุชื่อจริงแล้ว
- [ ] release window และ rollback rule ระบุเป็นลายลักษณ์อักษรแล้ว

## Sign-off
- [ ] dev sign-off
- [ ] system/DBA sign-off
- [ ] operator sign-off
- [ ] Phase 3 final sign-off
