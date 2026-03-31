USE db_monitor;

INSERT INTO monitor_table_watchlist
(db_name, table_name, enabled, priority, compare_strategy, pk_column, date_column, note)
VALUES
('hosxp', 'ovst', 1, 10, 'business_counter', 'vn', 'vstdate', 'ตารางหลัก OPD'),
('hosxp', 'vn_stat', 1, 20, 'business_counter', 'vn', 'vstdate', 'summary visit'),
('hosxp', 'an_stat', 1, 30, 'business_counter', 'an', 'regdate', 'summary admit'),
('hosxp', 'patient', 1, 40, 'business_counter', 'hn', NULL, 'master data patient'),
('hosxp', 'person', 1, 50, 'business_counter', 'person_id', NULL, 'master data person'),
('hosxp', 'opitemrece', 1, 60, 'business_counter', 'vn', 'vstdate', 'ค่าใช้จ่าย/ยา/หัตถการ'),
('hosxp', 'lab_head', 1, 70, 'business_counter', 'lab_order_number', 'order_date', 'หัวใบสั่งแลบ'),
('hosxp', 'lab_order', 1, 80, 'business_counter', 'lab_order_number', NULL, 'ผลแลบ'),
('hosxp', 'ipt', 1, 90, 'business_counter', 'an', 'regdate', 'งาน IPD'),
('hosxp', 'ovstdiag', 1, 100, 'pk_tail', 'ovst_diag_id', 'vstdate', 'งานวินิจฉัย');
