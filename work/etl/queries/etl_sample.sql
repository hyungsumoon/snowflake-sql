-- ETL 샘플: 임시 테이블을 만들어 데이터 적재 시뮬레이션
CREATE OR REPLACE TABLE etl_test AS
SELECT seq4() AS id, current_timestamp() AS ts;

SELECT COUNT(*) FROM etl_test;
