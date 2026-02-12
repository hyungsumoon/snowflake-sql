-- 리포트 샘플: 최근 7일간 테이블 데이터 수
SELECT table_name, row_count FROM information_schema.tables
WHERE table_schema = 'PUBLIC' AND table_catalog = current_database();
