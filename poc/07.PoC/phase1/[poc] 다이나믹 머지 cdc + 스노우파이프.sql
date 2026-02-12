USE DATABASE poc;
USE SCHEMA poc;
USE WAREHOUSE compute_wh;


SELECT * FROM STAGING_TABLE LIMIT 10;

--다이나믹 머지 프로시저
CREATE OR REPLACE PROCEDURE poc.poc.DYNAMIC_MERGE_CDC_FOR_HANATOUR(
    STREAM_NAME STRING,    -- 소스 스테이지 테이블
    TARGET_TABLE_NAME STRING,     -- 타겟 테이블 이름
    PRIMARY_KEY_COLUMN STRING,    -- Primary Key 컬럼명
    CHANGE_SEQ_COL STRING,        -- CDC 순서 컬럼 (예: hd_change_seq)
    CHANGE_OPER_COL STRING,       -- CDC 타입 컬럼 (예: hd_change_oper)
    LOG_TABLE_NAME STRING         -- 로그 테이블 이름
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'merge_cdc_handler'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, row_number, lit, when_matched, when_not_matched
from snowflake.snowpark.window import Window

def merge_cdc_handler(
    session: snowpark.Session,
    stream_name: str,
    target_table_name: str,
    primary_key_column: str,
    change_seq_col: str,  
    change_oper_col: str,
    log_table_name: str
):
    pk_safe = primary_key_column if primary_key_column.startswith('"') else f'"{primary_key_column}"'
    seq_safe = change_seq_col if change_seq_col.startswith('"') else f'"{change_seq_col}"'
    oper_safe = change_oper_col if change_oper_col.startswith('"') else f'"{change_oper_col}"'

    parts = target_table_name.upper().split('.')
    if len(parts) == 3:
        t_db, t_schema, t_table = [p.upper() for p in parts] # UPPER() 적용
    elif len(parts) == 2:
        t_db = session.get_current_database().upper()
        t_schema, t_table = [p.upper() for p in parts] # UPPER() 적용
    else:
        t_db = session.get_current_database().upper()
        t_schema = session.get_current_schema().upper()
        t_table = parts[0].upper() # UPPER() 적용

    query = f"""
        SELECT COLUMN_NAME
        FROM {t_db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{t_db}'
          AND TABLE_SCHEMA = '{t_schema}'
          AND TABLE_NAME = '{t_table}'
        ORDER BY ORDINAL_POSITION
    """
    df_columns = session.sql(query).collect()
   
    if not df_columns:
         return f"Error: No columns found for {target_table_name}"

    all_target_columns = [f'"{row["COLUMN_NAME"]}"' for row in df_columns]

    target_df = session.table(target_table_name)
    source_df = session.table(stream_name)

    # PROCESSED_AT이 NULL인 레코드만 처리 (간단!)
    incremental_source_df = source_df.filter(col("PROCESSED_AT").isNull())
    stream_count = incremental_source_df.count()
    if stream_count==0:
        return f"Success: 새로운 row가 {stream_name}에 없습니다."

    window_spec = Window.partition_by(col(pk_safe)).order_by(col(seq_safe).desc())
   
    deduped_source_df = incremental_source_df.select(
        "*",
        row_number().over(window_spec).alias("RNUM")
    ).filter(col("RNUM") == 1)

    full_assignments = {c: deduped_source_df[c] for c in all_target_columns}

    merge_result = target_df.merge(
        source=deduped_source_df,
        join_expr=(target_df[pk_safe] == deduped_source_df[pk_safe]),
        clauses=[
            when_matched(
                condition=(
                    (deduped_source_df[oper_safe] == lit('U')) |
                    (deduped_source_df[oper_safe] == lit('I'))
                )
            ).update(full_assignments),
           
            when_matched(
                condition=(deduped_source_df[oper_safe] == lit('D'))
            ).delete(),
           
            when_not_matched(
                condition=(
                    (deduped_source_df[oper_safe] == lit('I')) |
                    (deduped_source_df[oper_safe] == lit('U'))
                )
            ).insert(full_assignments)
        ]
    )

    inserted_cnt = merge_result.rows_inserted
    updated_cnt = merge_result.rows_updated
    deleted_cnt = merge_result.rows_deleted

    # 처리 완료 시 현재 시간 기록 (NULL → TIMESTAMP)
    session.sql(f"""
        UPDATE {stream_name}
        SET PROCESSED_AT = CURRENT_TIMESTAMP()
        WHERE PROCESSED_AT IS NULL
    """).collect()
   
    log_sql = f"""
        INSERT INTO {log_table_name} (
            EXEC_DT, OP_TYPE, STREAM_TABLE, TARGET_TABLE,
            STREAM_COUNT, INSERT_CNT, UPDATE_CNT, DELETE_CNT
        )
        VALUES (
            CURRENT_TIMESTAMP(), 'MERGE', '{stream_name}', '{target_table_name}',
            {stream_count}, {inserted_cnt}, {updated_cnt}, {deleted_cnt}
        )
    """
    session.sql(log_sql).collect()

    return f"Success: {stream_count} rows processed (I:{inserted_cnt}, U:{updated_cnt}, D:{deleted_cnt})"
$$;

--=========================================
-- 0. CDC log table 생성
-- seq, 실행시간, 작업 유형, 건수 등 데이터 적재
--==========================================
CREATE TABLE poc.poc.CDC_MERGE_LOG (
    LOG_SEQ         NUMBER IDENTITY(1,1), -- 자동 증가 시퀀스
    EXEC_DT         TIMESTAMP_LTZ,        -- 실행 시간
    OP_TYPE         VARCHAR(20),          -- 작업 유형 (MERGE)
    STREAM_TABLE    VARCHAR(200),         -- 소스 테이블(스트림) 명
    TARGET_TABLE    VARCHAR(200),         -- 타겟 테이블 명
    STREAM_COUNT    NUMBER,               -- 스트림 유입 건수
    INSERT_CNT      NUMBER,               -- MERGE Insert 건수
    UPDATE_CNT      NUMBER,               -- MERGE Update 건수
    DELETE_CNT      NUMBER                -- MERGE Delete 건수
);

-- ========================================
-- 1. Staging Table 생성 (간소화된 버전)
-- ========================================

CREATE OR REPLACE TABLE poc.poc.staging_table
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/20251118-060841007.snappy.parquet',
      FILE_FORMAT=>'STG_EXTERNAL.CDC.PARQUET'
    )
  )
);

-- TIMESTAMP 플래그 (NULL = 미처리, 값있음 = 처리됨)
ALTER TABLE poc.poc.staging_table ADD COLUMN PROCESSED_AT TIMESTAMP_LTZ DEFAULT NULL;


select * from staging_table limit 10;

select * from STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_ct_TEST2;
-- ========================================
-- 2. Target Table 생성 (테스트용)
-- ========================================
CREATE OR REPLACE TABLE target_table
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M/LOAD00000001.snappy.parquet',
      FILE_FORMAT=>'STG_EXTERNAL.CDC.PARQUET'
    )
  )
);

-- ========================================
-- 3. 초기 적재 (원본 테이블 → Target Table) - 테스트를 위해 임시 생성
-- ========================================
-- copy into로 초기 적재
COPY INTO poc.poc.target_table
FROM '@"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M/'
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
FILE_FORMAT = (FORMAT_NAME = 'STG_EXTERNAL.CDC.PARQUET')
ON_ERROR='CONTINUE'
PATTERN = '.*\\.parquet$'
FORCE=TRUE;


-- 초기 적재 확인
SELECT COUNT(*) FROM target_table;
SELECT * FROM STAGING_TABLE;

-- ========================================
-- 4. CDC Warehouse 생성
-- ========================================
CREATE OR REPLACE WAREHOUSE cdc_wh
    WAREHOUSE_SIZE = 'XSmall'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;


-- ========================================
-- 5. Snowpipe 생성 (간소화된 버전)
-- ========================================
-- 방법 1: MATCH_BY_COLUMN_NAME 사용 (간단, DEFAULT FALSE 의존)
CREATE OR REPLACE PIPE voc_cdc_auto_pipe
AUTO_INGEST = TRUE
AS
COPY INTO staging_table
FROM @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = parquet_format
PATTERN = '.*\.parquet'
ON_ERROR = 'SKIP_FILE';

--pipe paused 되어있는상태
alter pipe VOC_CDC_AUTO_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
alter pipe VOC_CDC_AUTO_PIPE SET PIPE_EXECUTION_PAUSED = false;


-- ========================================
-- 6. 초기 CDC 데이터 적재 (Stage → Staging table)
-- ========================================
-- Stage에 있는 기존 CDC 파일을 Staging 테이블로 로드
-- MATCH_BY_COLUMN_NAME 사용 시 DEFAULT FALSE가 자동 적용됨
COPY INTO staging_table
FROM @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = parquet_format
PATTERN = '.*\.parquet';


--staging table 로우 개수
--6656 row
select count(*) from staging_table;


--target table 로우 개수
--872128
select count(*) from target_table;

-- ========================================
-- 7. CDC 처리 Task
-- ========================================

use database poc;
use schema poc;

CREATE OR REPLACE TASK process_cdc
    WAREHOUSE = cdc_wh
    SCHEDULE = '60 MINUTE'
AS
    CALL DYNAMIC_MERGE_CDC_FOR_HANATOUR(
        'POC.POC.STAGING_TABLE',  -- staging_table
        'POC.POC.TARGET_TABLE',     -- TARGET_TABLE_NAME
        'RCPN_NUM',                                  -- PRIMARY_KEY_COLUMN (따옴표 없어도 됨)
        'hd_change_seq',                             -- CHANGE_SEQ_COL
        'hd_change_oper',                            -- CHANGE_OPER_COL
        'POC.POC.CDC_MERGE_LOG'             -- LOG_TABLE_NAME
    );

--일단 서스펜드
alter task process_cdc suspend;


CALL DYNAMIC_MERGE_CDC_FOR_HANATOUR(
        'POC.POC.STAGING_TABLE',  -- staging_table
        'POC.POC.TARGET_TABLE',     -- TARGET_TABLE_NAME
        'RCPN_NUM',                                  -- PRIMARY_KEY_COLUMN (따옴표 없어도 됨)
        'hd_change_seq',                             -- CHANGE_SEQ_COL
        'hd_change_oper',                            -- CHANGE_OPER_COL
        'POC.POC.CDC_MERGE_LOG'             -- LOG_TABLE_NAME
    );

select * from staging_table limit 10;


select * from staging_table where processed_at is null limit 10;

select count(*) from staging_table;

-- 처리 상태 확인
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN PROCESSED_AT IS NULL THEN 1 END) as pending,
    COUNT(CASE WHEN PROCESSED_AT IS NOT NULL THEN 1 END) as processed
FROM staging_table;

-- 미처리 데이터만 조회
SELECT * FROM staging_table WHERE PROCESSED_AT IS NULL LIMIT 10;

-- 처리된 데이터 조회 (언제 처리됐는지 확인 가능)
SELECT * FROM staging_table WHERE PROCESSED_AT IS NOT NULL 
ORDER BY PROCESSED_AT DESC LIMIT 10;

--cdc 후 target table 로우 개수
--873962
select count(*) from target_table;

--1차 cdc processed rows: 6466
--2차 cdc processed rows:


select * from cdc_merge_log;

--==========================================
--8. 로그 데이터 확인
--==========================================
select * from cdc_merge_log;



-- Task가 첫 실행될 때 Stream의 초기 데이터를 모두 처리함!

-- ========================================
-- 9. 모니터링 쿼리
-- ========================================

-- Staging 테이블 확인
SELECT COUNT(*) FROM staging_table;

-- Target 테이블 확인 (Task 첫 실행 후)
SELECT COUNT(*) FROM target_table;

-- Pipe 상태 확인
SHOW PIPES;
SELECT SYSTEM$PIPE_STATUS('VOC_CDC_AUTO_PIPE');

-- Task 이력 확인 (Snowflake 시스템)
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'PROCESS_CDC_STREAM_REALTIME'
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- COPY 이력 확인
SELECT
    FILE_NAME,
    STATUS,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME,
    ROW_COUNT
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STAGING_VOC_DMND_EXNS_M',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- Stage 파일 목록 확인
LIST @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/;

-- CDC Operation별 확인 (Staging)
SELECT
    hd_change_oper,
    COUNT(*) as count
FROM staging_voc_dmnd_exns_m
GROUP BY hd_change_oper;