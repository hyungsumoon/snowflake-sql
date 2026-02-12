-- 하나투어 CDC 처리 (SHOW_INITIAL_ROWS = TRUE 사용)
-- 초기 적재 유지 + 시스템 메타데이터 제거 + Hard Delete + Stream 자동 초기 처리

USE DATABASE poc;
USE SCHEMA poc;
USE WAREHOUSE compute_wh;

-- ========================================
-- 1. Staging Table 생성 (간소화된 버전)
-- ========================================
CREATE OR REPLACE TABLE staging_voc_dmnd_exns_m (
    -- CDC 메타데이터만 유지
    record_content VARIANT,
    hd_change_seq TEXT,
    hd_change_oper TEXT,  -- I, U, D
    hd_timestamp TIMESTAMP_NTZ,
    loaded_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


select * from staging_voc_dmnd_exns_m;

select * from target_voc_dmnd_exns_m;

select count(*) from target_voc_dmnd_exns_m;



-- ========================================
-- 2. Target Table 생성 (시스템 컬럼 제거)
-- ========================================
CREATE OR REPLACE TABLE target_voc_dmnd_exns_m (
    RCPN_NUM TEXT PRIMARY KEY,
    VOC_DMND_TYPE_CD1 TEXT,
    VOC_DMND_TYPE_CD2 TEXT,
    VOC_DMND_TYPE_CD3 TEXT,
    VOC_DMND_TYPE_CD4 TEXT,
    VOC_DMND_TYPE_CD5 TEXT,
    CATG_SSC_TYPE_CD TEXT,
    VOC_DMND_RCPN_REFN_CD_CONT TEXT,
    VOC_DMND_RCPN_REFN_NM TEXT,
    VOC_DMND_RCPN_REFN_CMNT_MEMO TEXT,
    KPI_SCP_CD TEXT,
    VIST_DT TEXT,
    RPPR_DEPT_CD TEXT,
    RPPR_EMPN TEXT,
    RPPR_NM TEXT,
    OPPB_YN TEXT,
    AGE_DV_CD TEXT,
    SMS_BLCKF_STAT_CD TEXT,
    VOC_RCPN_BRWS_CNT NUMBER(22, 0),
    CMPN_YN TEXT,
    DPLT_RCPN_YN TEXT,
    ARR_YN TEXT,
    TP_CNSL_YN TEXT,
    ANSR_RCMS_TYPE_CD TEXT,
    VOC_PROS_STG_INFM_CD TEXT,
    SEND_YN TEXT,
    INP_DTTM TIMESTAMP_NTZ,
    VOC_OCRN_CITY_CD TEXT,
    INPR_ID TEXT,
    INP_PRGM_ID TEXT,
    UPD_DTTM TIMESTAMP_NTZ,
    UPDR_ID TEXT,
    UPD_PRGM_ID TEXT,
    CDC_LD_DTTM TIMESTAMP_NTZ
);

-- ========================================
-- 3. 초기 적재 (원본 테이블 → Target Table) - 테스트를 위해 임시 생성
-- ========================================
-- 원본 DB에서 직접 초기 적재하는 경우 사용
INSERT INTO target_voc_dmnd_exns_m (
    RCPN_NUM, VOC_DMND_TYPE_CD1, VOC_DMND_TYPE_CD2, VOC_DMND_TYPE_CD3, VOC_DMND_TYPE_CD4, 
    VOC_DMND_TYPE_CD5, CATG_SSC_TYPE_CD, VOC_DMND_RCPN_REFN_CD_CONT, VOC_DMND_RCPN_REFN_NM, 
    VOC_DMND_RCPN_REFN_CMNT_MEMO, KPI_SCP_CD, VIST_DT, RPPR_DEPT_CD, RPPR_EMPN, RPPR_NM, 
    OPPB_YN, AGE_DV_CD, SMS_BLCKF_STAT_CD, VOC_RCPN_BRWS_CNT, CMPN_YN, DPLT_RCPN_YN, 
    ARR_YN, TP_CNSL_YN, ANSR_RCMS_TYPE_CD, VOC_PROS_STG_INFM_CD, SEND_YN, INP_DTTM, 
    VOC_OCRN_CITY_CD, INPR_ID, INP_PRGM_ID, UPD_DTTM, UPDR_ID, UPD_PRGM_ID, CDC_LD_DTTM
)
SELECT
    RCPN_NUM, VOC_DMND_TYPE_CD1, VOC_DMND_TYPE_CD2, VOC_DMND_TYPE_CD3, VOC_DMND_TYPE_CD4,
    VOC_DMND_TYPE_CD5, CATG_SSC_TYPE_CD, VOC_DMND_RCPN_REFN_CD_CONT, VOC_DMND_RCPN_REFN_NM,
    VOC_DMND_RCPN_REFN_CMNT_MEMO, KPI_SCP_CD, VIST_DT, RPPR_DEPT_CD, RPPR_EMPN, RPPR_NM,
    OPPB_YN, AGE_DV_CD, SMS_BLCKF_STAT_CD, VOC_RCPN_BRWS_CNT, CMPN_YN, DPLT_RCPN_YN,
    ARR_YN, TP_CNSL_YN, ANSR_RCMS_TYPE_CD, VOC_PROS_STG_INFM_CD, SEND_YN, INP_DTTM,
    VOC_OCRN_CITY_CD, INPR_ID, INP_PRGM_ID, UPD_DTTM, UPDR_ID, UPD_PRGM_ID, CDC_LD_DTTM
FROM cdc.hub01.voc_dmnd_exns_m;

-- 초기 적재 확인
SELECT COUNT(*) FROM target_voc_dmnd_exns_m;

-- ========================================
-- 4. CDC Warehouse 생성
-- ========================================
CREATE OR REPLACE WAREHOUSE cdc_wh
    WAREHOUSE_SIZE = 'XSmall'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE;

-- ========================================
-- 5. Snowpipe 생성 (간소화된 버전)
-- ========================================
CREATE OR REPLACE PIPE voc_cdc_auto_pipe
AUTO_INGEST = TRUE
AS
COPY INTO staging_voc_dmnd_exns_m (record_content, hd_change_seq, hd_change_oper, hd_timestamp)
FROM (
    SELECT 
        $1,                                    -- 전체 record를 VARIANT로 저장
        $1:hd_change_seq::TEXT,
        $1:hd_change_oper::TEXT,
        $1:hd_timestamp::TIMESTAMP_NTZ
    FROM @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/
)
FILE_FORMAT = parquet_format
PATTERN = '.*\.parquet'
ON_ERROR = 'SKIP_FILE';



-- ========================================
-- 6. CDC 로그 테이블 생성 (간단한 버전)
-- ========================================
CREATE OR REPLACE TABLE cdc_processing_log (
    log_id NUMBER AUTOINCREMENT PRIMARY KEY,
    execution_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    table_name TEXT DEFAULT 'target_voc_dmnd_exns_m',
    records_processed NUMBER,
    notes TEXT
);




-- ========================================
-- 7. 초기 CDC 데이터 적재 (Stage → Staging)
-- ========================================
-- Stage에 있는 기존 CDC 파일을 Staging 테이블로 로드
COPY INTO staging_voc_dmnd_exns_m (record_content, hd_change_seq, hd_change_oper, hd_timestamp)
FROM (
    SELECT 
        $1,                                    -- 전체 record를 VARIANT로 저장
        $1:hd_change_seq::TEXT,
        $1:hd_change_oper::TEXT,
        $1:hd_timestamp::TIMESTAMP_NTZ
    FROM @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/
)
FILE_FORMAT = parquet_format
PATTERN = '.*\.parquet';

-- 로드된 데이터 확인
SELECT COUNT(*) FROM staging_voc_dmnd_exns_m;

-- ========================================
-- 8. Stream 생성 (SHOW_INITIAL_ROWS = TRUE)
-- ========================================
-- 이 옵션으로 Staging 테이블의 기존 데이터가 Stream에 포함됨
-- Task가 첫 실행될 때 초기 데이터를 모두 처리함
CREATE OR REPLACE STREAM staging_voc_stream 
ON TABLE staging_voc_dmnd_exns_m
SHOW_INITIAL_ROWS = TRUE;  -- ⭐ 핵심! 기존 데이터도 Stream에 포함


select* from staging_voc_stream;
select count(*) from staging_voc_dmnd_exns_m;

-- Stream 데이터 확인 (초기 데이터가 보여야 함)
SELECT COUNT(*) FROM staging_voc_stream;

-- ========================================
-- 9. CDC 처리 Task (간단하게!)
-- ========================================
CREATE OR REPLACE TASK process_cdc_stream_realtime
  WAREHOUSE = cdc_wh
  SCHEDULE = '10 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('staging_voc_stream')
AS
  MERGE INTO target_voc_dmnd_exns_m AS t
  USING (
    -- PK별 가장 최신 변경 이벤트만 선택
    WITH processed_data AS (
        SELECT 
            record_content:RCPN_NUM::TEXT AS RCPN_NUM,
            record_content:VOC_DMND_TYPE_CD1::TEXT AS VOC_DMND_TYPE_CD1,
            record_content:VOC_DMND_TYPE_CD2::TEXT AS VOC_DMND_TYPE_CD2,
            record_content:VOC_DMND_TYPE_CD3::TEXT AS VOC_DMND_TYPE_CD3,
            record_content:VOC_DMND_TYPE_CD4::TEXT AS VOC_DMND_TYPE_CD4,
            record_content:VOC_DMND_TYPE_CD5::TEXT AS VOC_DMND_TYPE_CD5,
            record_content:CATG_SSC_TYPE_CD::TEXT AS CATG_SSC_TYPE_CD,
            record_content:VOC_DMND_RCPN_REFN_CD_CONT::TEXT AS VOC_DMND_RCPN_REFN_CD_CONT,
            record_content:VOC_DMND_RCPN_REFN_NM::TEXT AS VOC_DMND_RCPN_REFN_NM,
            record_content:VOC_DMND_RCPN_REFN_CMNT_MEMO::TEXT AS VOC_DMND_RCPN_REFN_CMNT_MEMO,
            record_content:KPI_SCP_CD::TEXT AS KPI_SCP_CD,
            record_content:VIST_DT::TEXT AS VIST_DT,
            record_content:RPPR_DEPT_CD::TEXT AS RPPR_DEPT_CD,
            record_content:RPPR_EMPN::TEXT AS RPPR_EMPN,
            record_content:RPPR_NM::TEXT AS RPPR_NM,
            record_content:OPPB_YN::TEXT AS OPPB_YN,
            record_content:AGE_DV_CD::TEXT AS AGE_DV_CD,
            record_content:SMS_BLCKF_STAT_CD::TEXT AS SMS_BLCKF_STAT_CD,
            record_content:VOC_RCPN_BRWS_CNT::NUMBER(22, 0) AS VOC_RCPN_BRWS_CNT,
            record_content:CMPN_YN::TEXT AS CMPN_YN,
            record_content:DPLT_RCPN_YN::TEXT AS DPLT_RCPN_YN,
            record_content:ARR_YN::TEXT AS ARR_YN,
            record_content:TP_CNSL_YN::TEXT AS TP_CNSL_YN,
            record_content:ANSR_RCMS_TYPE_CD::TEXT AS ANSR_RCMS_TYPE_CD,
            record_content:VOC_PROS_STG_INFM_CD::TEXT AS VOC_PROS_STG_INFM_CD,
            record_content:SEND_YN::TEXT AS SEND_YN,
            record_content:INP_DTTM::TIMESTAMP_NTZ AS INP_DTTM,
            record_content:VOC_OCRN_CITY_CD::TEXT AS VOC_OCRN_CITY_CD,
            record_content:INPR_ID::TEXT AS INPR_ID,
            record_content:INP_PRGM_ID::TEXT AS INP_PRGM_ID,
            record_content:UPD_DTTM::TIMESTAMP_NTZ AS UPD_DTTM,
            record_content:UPDR_ID::TEXT AS UPDR_ID,
            record_content:UPD_PRGM_ID::TEXT AS UPD_PRGM_ID,
            record_content:CDC_LD_DTTM::TIMESTAMP_NTZ AS CDC_LD_DTTM,
            hd_change_seq,
            hd_change_oper,
            hd_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY record_content:RCPN_NUM::TEXT
                ORDER BY hd_change_seq DESC, hd_timestamp DESC
            ) AS rn
        FROM staging_voc_stream
        WHERE record_content:RCPN_NUM IS NOT NULL
    )
    SELECT *
    FROM processed_data
    WHERE rn = 1
  ) AS s ON t.RCPN_NUM = s.RCPN_NUM
  
  -- DELETE 처리
  WHEN MATCHED AND s.hd_change_oper = 'D' THEN
    DELETE
    
  -- UPDATE 처리  
  WHEN MATCHED AND s.hd_change_oper IN ('U', 'I') THEN
    UPDATE SET
      VOC_DMND_TYPE_CD1 = s.VOC_DMND_TYPE_CD1,
      VOC_DMND_TYPE_CD2 = s.VOC_DMND_TYPE_CD2,
      VOC_DMND_TYPE_CD3 = s.VOC_DMND_TYPE_CD3,
      VOC_DMND_TYPE_CD4 = s.VOC_DMND_TYPE_CD4,
      VOC_DMND_TYPE_CD5 = s.VOC_DMND_TYPE_CD5,
      CATG_SSC_TYPE_CD = s.CATG_SSC_TYPE_CD,
      VOC_DMND_RCPN_REFN_CD_CONT = s.VOC_DMND_RCPN_REFN_CD_CONT,
      VOC_DMND_RCPN_REFN_NM = s.VOC_DMND_RCPN_REFN_NM,
      VOC_DMND_RCPN_REFN_CMNT_MEMO = s.VOC_DMND_RCPN_REFN_CMNT_MEMO,
      KPI_SCP_CD = s.KPI_SCP_CD,
      VIST_DT = s.VIST_DT,
      RPPR_DEPT_CD = s.RPPR_DEPT_CD,
      RPPR_EMPN = s.RPPR_EMPN,
      RPPR_NM = s.RPPR_NM,
      OPPB_YN = s.OPPB_YN,
      AGE_DV_CD = s.AGE_DV_CD,
      SMS_BLCKF_STAT_CD = s.SMS_BLCKF_STAT_CD,
      VOC_RCPN_BRWS_CNT = s.VOC_RCPN_BRWS_CNT,
      CMPN_YN = s.CMPN_YN,
      DPLT_RCPN_YN = s.DPLT_RCPN_YN,
      ARR_YN = s.ARR_YN,
      TP_CNSL_YN = s.TP_CNSL_YN,
      ANSR_RCMS_TYPE_CD = s.ANSR_RCMS_TYPE_CD,
      VOC_PROS_STG_INFM_CD = s.VOC_PROS_STG_INFM_CD,
      SEND_YN = s.SEND_YN,
      INP_DTTM = s.INP_DTTM,
      VOC_OCRN_CITY_CD = s.VOC_OCRN_CITY_CD,
      INPR_ID = s.INPR_ID,
      INP_PRGM_ID = s.INP_PRGM_ID,
      UPD_DTTM = s.UPD_DTTM,
      UPDR_ID = s.UPDR_ID,
      UPD_PRGM_ID = s.UPD_PRGM_ID,
      CDC_LD_DTTM = s.CDC_LD_DTTM
      
  -- INSERT 처리
  WHEN NOT MATCHED AND s.hd_change_oper IN ('I', 'U') THEN
    INSERT (
      RCPN_NUM, VOC_DMND_TYPE_CD1, VOC_DMND_TYPE_CD2, VOC_DMND_TYPE_CD3,
      VOC_DMND_TYPE_CD4, VOC_DMND_TYPE_CD5, CATG_SSC_TYPE_CD,
      VOC_DMND_RCPN_REFN_CD_CONT, VOC_DMND_RCPN_REFN_NM, VOC_DMND_RCPN_REFN_CMNT_MEMO,
      KPI_SCP_CD, VIST_DT, RPPR_DEPT_CD, RPPR_EMPN, RPPR_NM, OPPB_YN,
      AGE_DV_CD, SMS_BLCKF_STAT_CD, VOC_RCPN_BRWS_CNT, CMPN_YN, DPLT_RCPN_YN,
      ARR_YN, TP_CNSL_YN, ANSR_RCMS_TYPE_CD, VOC_PROS_STG_INFM_CD, SEND_YN,
      INP_DTTM, VOC_OCRN_CITY_CD, INPR_ID, INP_PRGM_ID, UPD_DTTM,
      UPDR_ID, UPD_PRGM_ID, CDC_LD_DTTM
    )
    VALUES (
      s.RCPN_NUM, s.VOC_DMND_TYPE_CD1, s.VOC_DMND_TYPE_CD2, s.VOC_DMND_TYPE_CD3,
      s.VOC_DMND_TYPE_CD4, s.VOC_DMND_TYPE_CD5, s.CATG_SSC_TYPE_CD,
      s.VOC_DMND_RCPN_REFN_CD_CONT, s.VOC_DMND_RCPN_REFN_NM, s.VOC_DMND_RCPN_REFN_CMNT_MEMO,
      s.KPI_SCP_CD, s.VIST_DT, s.RPPR_DEPT_CD, s.RPPR_EMPN, s.RPPR_NM, s.OPPB_YN,
      s.AGE_DV_CD, s.SMS_BLCKF_STAT_CD, s.VOC_RCPN_BRWS_CNT, s.CMPN_YN, s.DPLT_RCPN_YN,
      s.ARR_YN, s.TP_CNSL_YN, s.ANSR_RCMS_TYPE_CD, s.VOC_PROS_STG_INFM_CD, s.SEND_YN,
      s.INP_DTTM, s.VOC_OCRN_CITY_CD, s.INPR_ID, s.INP_PRGM_ID, s.UPD_DTTM,
      s.UPDR_ID, s.UPD_PRGM_ID, s.CDC_LD_DTTM
    );

-- ========================================
-- 10. Task 활성화
-- ========================================
ALTER TASK process_cdc_stream_realtime RESUME;


--현재는 서스펜드 되어있음
ALTER TASK process_cdc_stream_realtime SUSPEND;


-- Task가 첫 실행될 때 Stream의 초기 데이터를 모두 처리함!

-- ========================================
-- 11. 모니터링 쿼리
-- ========================================

-- Staging 테이블 확인
SELECT COUNT(*) FROM staging_voc_dmnd_exns_m;

-- Target 테이블 확인 (Task 첫 실행 후)
SELECT COUNT(*) FROM target_voc_dmnd_exns_m;

-- Stream 확인 (Task 실행 전에는 데이터 있음, 실행 후에는 비워짐)
SELECT COUNT(*) FROM staging_voc_stream;
SELECT * FROM staging_voc_stream LIMIT 10;

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

-- ========================================
-- 12. 간단한 로그 조회 (선택사항)
-- ========================================

-- 로그 테이블 조회 (수동으로 기록하는 경우)
SELECT * FROM cdc_processing_log ORDER BY execution_time DESC LIMIT 10;

 