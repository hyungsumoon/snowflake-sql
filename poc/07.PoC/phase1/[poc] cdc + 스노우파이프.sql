use database poc;
use schema poc;

use warehouse compute_wh;

-- 2. Staging Table 생성 (파일 도착 시간 추가)
CREATE OR REPLACE TABLE staging_voc_dmnd_exns_m (
    -- CDC 메타데이터
    hd_change_seq TEXT NOT NULL,
    hd_change_oper TEXT NOT NULL,  -- I, U, D
    hd_timestamp TIMESTAMP_NTZ NOT NULL,
    
    -- 비즈니스 컬럼들
    RCPN_NUM TEXT,
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
    CDC_LD_DTTM TIMESTAMP_NTZ,
    
    -- 시스템 메타데이터 (요청사항)
    _file_name TEXT,
    _file_arrived_time TIMESTAMP_NTZ,  -- 파일이 스테이지에 도착한 시간
    _loaded_to_staging_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- 스테이징 테이블 로드 시간
);

select count(*) from staging_voc_dmnd_exns_m;
/*
-- 3. Target Table 생성
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
    CDC_LD_DTTM TIMESTAMP_NTZ,
    
    -- 시스템 컬럼들
    _cdc_sequence TEXT,
    _last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _is_deleted BOOLEAN DEFAULT FALSE
);*/

-- Target Table 생성 (Primary Key와 시스템 컬럼 포함)
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
    CDC_LD_DTTM TIMESTAMP_NTZ,

    -- 시스템 컬럼들
    _cdc_sequence TEXT,
    _last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _is_deleted BOOLEAN DEFAULT FALSE
);


-- Target Table에 원본 데이터 초기 적재 (CDC 메타데이터 초기화)
INSERT INTO target_voc_dmnd_exns_m (
    RCPN_NUM, VOC_DMND_TYPE_CD1, VOC_DMND_TYPE_CD2, VOC_DMND_TYPE_CD3, VOC_DMND_TYPE_CD4, 
    VOC_DMND_TYPE_CD5, CATG_SSC_TYPE_CD, VOC_DMND_RCPN_REFN_CD_CONT, VOC_DMND_RCPN_REFN_NM, 
    VOC_DMND_RCPN_REFN_CMNT_MEMO, KPI_SCP_CD, VIST_DT, RPPR_DEPT_CD, RPPR_EMPN, RPPR_NM, 
    OPPB_YN, AGE_DV_CD, SMS_BLCKF_STAT_CD, VOC_RCPN_BRWS_CNT, CMPN_YN, DPLT_RCPN_YN, 
    ARR_YN, TP_CNSL_YN, ANSR_RCMS_TYPE_CD, VOC_PROS_STG_INFM_CD, SEND_YN, INP_DTTM, 
    VOC_OCRN_CITY_CD, INPR_ID, INP_PRGM_ID, UPD_DTTM, UPDR_ID, UPD_PRGM_ID, CDC_LD_DTTM,
    
    -- 시스템 컬럼 초기값 설정: 초기 로드는 CDC SEQ = 0, UPDATE 시간 = INP_DTTM
    _cdc_sequence, _last_updated, _is_deleted
)
SELECT
    RCPN_NUM, VOC_DMND_TYPE_CD1, VOC_DMND_TYPE_CD2, VOC_DMND_TYPE_CD3, VOC_DMND_TYPE_CD4,
    VOC_DMND_TYPE_CD5, CATG_SSC_TYPE_CD, VOC_DMND_RCPN_REFN_CD_CONT, VOC_DMND_RCPN_REFN_NM,
    VOC_DMND_RCPN_REFN_CMNT_MEMO, KPI_SCP_CD, VIST_DT, RPPR_DEPT_CD, RPPR_EMPN, RPPR_NM,
    OPPB_YN, AGE_DV_CD, SMS_BLCKF_STAT_CD, VOC_RCPN_BRWS_CNT, CMPN_YN, DPLT_RCPN_YN,
    ARR_YN, TP_CNSL_YN, ANSR_RCMS_TYPE_CD, VOC_PROS_STG_INFM_CD, SEND_YN, INP_DTTM,
    VOC_OCRN_CITY_CD, INPR_ID, INP_PRGM_ID, UPD_DTTM, UPDR_ID, UPD_PRGM_ID, CDC_LD_DTTM,
    
    -- 시스템 컬럼 초기값
    '0', COALESCE(UPD_DTTM, INP_DTTM, CURRENT_TIMESTAMP()), FALSE
FROM cdc.hub01.voc_dmnd_exns_m;


select count(*) from target_voc_dmnd_exns_m;

create or replace warehouse cdc_wh
    warehouse_size = 'XSmall'
    auto_suspend=600;

-- 4. Snowpipe 생성 (자동 로딩)
CREATE OR REPLACE PIPE voc_cdc_auto_pipe
AUTO_INGEST = TRUE
AS
COPY INTO staging_voc_dmnd_exns_m
FROM (
    SELECT 
        -- CDC 메타데이터
        $1:hd_change_seq::TEXT,
        $1:hd_change_oper::TEXT,
        $1:hd_timestamp::TIMESTAMP_NTZ,
        
        -- 비즈니스 데이터
        $1:RCPN_NUM::TEXT,
        $1:VOC_DMND_TYPE_CD1::TEXT,
        $1:VOC_DMND_TYPE_CD2::TEXT,
        $1:VOC_DMND_TYPE_CD3::TEXT,
        $1:VOC_DMND_TYPE_CD4::TEXT,
        $1:VOC_DMND_TYPE_CD5::TEXT,
        $1:CATG_SSC_TYPE_CD::TEXT,
        $1:VOC_DMND_RCPN_REFN_CD_CONT::TEXT,
        $1:VOC_DMND_RCPN_REFN_NM::TEXT,
        $1:VOC_DMND_RCPN_REFN_CMNT_MEMO::TEXT,
        $1:KPI_SCP_CD::TEXT,
        $1:VIST_DT::TEXT,
        $1:RPPR_DEPT_CD::TEXT,
        $1:RPPR_EMPN::TEXT,
        $1:RPPR_NM::TEXT,
        $1:OPPB_YN::TEXT,
        $1:AGE_DV_CD::TEXT,
        $1:SMS_BLCKF_STAT_CD::TEXT,
        $1:VOC_RCPN_BRWS_CNT::NUMBER(22, 0),
        $1:CMPN_YN::TEXT,
        $1:DPLT_RCPN_YN::TEXT,
        $1:ARR_YN::TEXT,
        $1:TP_CNSL_YN::TEXT,
        $1:ANSR_RCMS_TYPE_CD::TEXT,
        $1:VOC_PROS_STG_INFM_CD::TEXT,
        $1:SEND_YN::TEXT,
        $1:INP_DTTM::TIMESTAMP_NTZ,
        $1:VOC_OCRN_CITY_CD::TEXT,
        $1:INPR_ID::TEXT,
        $1:INP_PRGM_ID::TEXT,
        $1:UPD_DTTM::TIMESTAMP_NTZ,
        $1:UPDR_ID::TEXT,
        $1:UPD_PRGM_ID::TEXT,
        $1:CDC_LD_DTTM::TIMESTAMP_NTZ,
        
        -- 시스템 메타데이터
        METADATA$FILENAME,
        TO_TIMESTAMP(REGEXP_SUBSTR(METADATA$FILENAME, '(\\d{8}-\\d{9})', 1, 1, 'e'), 'YYYYMMDD-HHMISSFF3'),
        CURRENT_TIMESTAMP()
    FROM @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/
)
FILE_FORMAT = parquet_format
PATTERN = '.*\.parquet$'
ON_ERROR = 'SKIP_FILE';

--3129
select count(*) from staging_voc_dmnd_exns_m;

show pipes;
SELECT SYSTEM$PIPE_STATUS('VOC_CDC_AUTO_PIPE');


LIST @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/;

-- 5. Stream 생성
CREATE OR REPLACE STREAM staging_voc_stream 
ON TABLE staging_voc_dmnd_exns_m;

select * from staging_voc_stream;

-- 5-1. 기존 데이터를 스테이징 테이블에 초기 로드 (COPY INTO)
COPY INTO staging_voc_dmnd_exns_m
FROM (
    SELECT
        -- CDC 메타데이터
        $1:hd_change_seq::TEXT AS hd_change_seq,
        $1:hd_change_oper::TEXT AS hd_change_oper,
        $1:hd_timestamp::TIMESTAMP_NTZ AS hd_timestamp,

        -- 비즈니스 데이터
        $1:RCPN_NUM::TEXT AS RCPN_NUM,
        $1:VOC_DMND_TYPE_CD1::TEXT AS VOC_DMND_TYPE_CD1,
        $1:VOC_DMND_TYPE_CD2::TEXT AS VOC_DMND_TYPE_CD2,
        $1:VOC_DMND_TYPE_CD3::TEXT AS VOC_DMND_TYPE_CD3,
        $1:VOC_DMND_TYPE_CD4::TEXT AS VOC_DMND_TYPE_CD4,
        $1:VOC_DMND_TYPE_CD5::TEXT AS VOC_DMND_TYPE_CD5,
        $1:CATG_SSC_TYPE_CD::TEXT AS CATG_SSC_TYPE_CD,
        $1:VOC_DMND_RCPN_REFN_CD_CONT::TEXT AS VOC_DMND_RCPN_REFN_CD_CONT,
        $1:VOC_DMND_RCPN_REFN_NM::TEXT AS VOC_DMND_RCPN_REFN_NM,
        $1:VOC_DMND_RCPN_REFN_CMNT_MEMO::TEXT AS VOC_DMND_RCPN_REFN_CMNT_MEMO,
        $1:KPI_SCP_CD::TEXT AS KPI_SCP_CD,
        $1:VIST_DT::TEXT AS VIST_DT,
        $1:RPPR_DEPT_CD::TEXT AS RPPR_DEPT_CD,
        $1:RPPR_EMPN::TEXT AS RPPR_EMPN,
        $1:RPPR_NM::TEXT AS RPPR_NM,
        $1:OPPB_YN::TEXT AS OPPB_YN,
        $1:AGE_DV_CD::TEXT AS AGE_DV_CD,
        $1:SMS_BLCKF_STAT_CD::TEXT AS SMS_BLCKF_STAT_CD,
        $1:VOC_RCPN_BRWS_CNT::NUMBER(22, 0) AS VOC_RCPN_BRWS_CNT,
        $1:CMPN_YN::TEXT AS CMPN_YN,
        $1:DPLT_RCPN_YN::TEXT AS DPLT_RCPN_YN,
        $1:ARR_YN::TEXT AS ARR_YN,
        $1:TP_CNSL_YN::TEXT AS TP_CNSL_YN,
        $1:ANSR_RCMS_TYPE_CD::TEXT AS ANSR_RCMS_TYPE_CD,
        $1:VOC_PROS_STG_INFM_CD::TEXT AS VOC_PROS_STG_INFM_CD,
        $1:SEND_YN::TEXT AS SEND_YN,
        $1:INP_DTTM::TIMESTAMP_NTZ AS INP_DTTM,
        $1:VOC_OCRN_CITY_CD::TEXT AS VOC_OCRN_CITY_CD,
        $1:INPR_ID::TEXT AS INPR_ID,
        $1:INP_PRGM_ID::TEXT AS INP_PRGM_ID,
        $1:UPD_DTTM::TIMESTAMP_NTZ AS UPD_DTTM,
        $1:UPDR_ID::TEXT AS UPDR_ID,
        $1:UPD_PRGM_ID::TEXT AS UPD_PRGM_ID,
        $1:CDC_LD_DTTM::TIMESTAMP_NTZ AS CDC_LD_DTTM,

        -- 시스템 메타데이터 (파일 도착 시간 추출 포함)
        METADATA$FILENAME AS SOURCE_FILENAME,
        TO_TIMESTAMP(REGEXP_SUBSTR(METADATA$FILENAME, '(\\d{8}-\\d{9})', 1, 1, 'e'), 'YYYYMMDD-HHMISSFF3') AS FILE_LOAD_TS,
        CURRENT_TIMESTAMP() AS ETL_INSERT_TS
    FROM @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/
)
FILE_FORMAT = parquet_format;
--PATTERN = '.*HUB01.VOC_DMND_EXNS_M_ct.*';


select count(*) from staging_voc_dmnd_exns_m;


-- 5-2. 초기 데이터를 타겟 테이블에 CDC 적용 (MERGE INTO)
MERGE INTO target_voc_dmnd_exns_m t
USING (
    -- PK별 가장 최신 변경 이벤트만 선택 (데이터 중복 방지)
    WITH processed_data AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY RCPN_NUM
                ORDER BY hd_change_seq DESC, hd_timestamp DESC
            ) AS rn
        FROM staging_voc_dmnd_exns_m
        WHERE RCPN_NUM IS NOT NULL
    )
    SELECT *
    FROM processed_data
    WHERE rn = 1
) s ON t.RCPN_NUM = s.RCPN_NUM

-- 1. DELETE 처리 (hd_change_oper = 'D')
WHEN MATCHED AND s.hd_change_oper = 'D' THEN
    UPDATE SET
        _is_deleted = TRUE,
        _last_updated = s.hd_timestamp,
        _cdc_sequence = s.hd_change_seq

-- 2. UPDATE 처리 (hd_change_oper = 'U' 또는 'I'인데 기존 데이터가 있는 경우)
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
        CDC_LD_DTTM = s.CDC_LD_DTTM,
        
        -- 메타데이터 업데이트
        _cdc_sequence = s.hd_change_seq,
        _last_updated = s.hd_timestamp,
        _is_deleted = FALSE

-- 3. INSERT 처리 (hd_change_oper = 'I' 또는 'U'인데 새로운 데이터인 경우)
WHEN NOT MATCHED AND s.hd_change_oper IN ('I', 'U') THEN
    INSERT (
        RCPN_NUM, VOC_DMND_TYPE_CD1, VOC_DMND_TYPE_CD2, VOC_DMND_TYPE_CD3,
        VOC_DMND_TYPE_CD4, VOC_DMND_TYPE_CD5, CATG_SSC_TYPE_CD,
        VOC_DMND_RCPN_REFN_CD_CONT, VOC_DMND_RCPN_REFN_NM, VOC_DMND_RCPN_REFN_CMNT_MEMO,
        KPI_SCP_CD, VIST_DT, RPPR_DEPT_CD, RPPR_EMPN, RPPR_NM, OPPB_YN,
        AGE_DV_CD, SMS_BLCKF_STAT_CD, VOC_RCPN_BRWS_CNT, CMPN_YN, DPLT_RCPN_YN,
        ARR_YN, TP_CNSL_YN, ANSR_RCMS_TYPE_CD, VOC_PROS_STG_INFM_CD, SEND_YN,
        INP_DTTM, VOC_OCRN_CITY_CD, INPR_ID, INP_PRGM_ID, UPD_DTTM,
        UPDR_ID, UPD_PRGM_ID, CDC_LD_DTTM, _cdc_sequence, _last_updated
    )
    VALUES (
        s.RCPN_NUM, s.VOC_DMND_TYPE_CD1, s.VOC_DMND_TYPE_CD2, s.VOC_DMND_TYPE_CD3,
        s.VOC_DMND_TYPE_CD4, s.VOC_DMND_TYPE_CD5, s.CATG_SSC_TYPE_CD,
        s.VOC_DMND_RCPN_REFN_CD_CONT, s.VOC_DMND_RCPN_REFN_NM, s.VOC_DMND_RCPN_REFN_CMNT_MEMO,
        s.KPI_SCP_CD, s.VIST_DT, s.RPPR_DEPT_CD, s.RPPR_EMPN, s.RPPR_NM, s.OPPB_YN,
        s.AGE_DV_CD, s.SMS_BLCKF_STAT_CD, s.VOC_RCPN_BRWS_CNT, s.CMPN_YN, s.DPLT_RCPN_YN,
        s.ARR_YN, s.TP_CNSL_YN, s.ANSR_RCMS_TYPE_CD, s.VOC_PROS_STG_INFM_CD, s.SEND_YN,
        s.INP_DTTM, s.VOC_OCRN_CITY_CD, s.INPR_ID, s.INP_PRGM_ID, s.UPD_DTTM,
        s.UPDR_ID, s.UPD_PRGM_ID, s.CDC_LD_DTTM, s.hd_change_seq, s.hd_timestamp
    );


--target table: 873039
select count(*) from target_voc_dmnd_exns_m;

-- 6. CDC 처리 Task (더 빈번한 실행)
CREATE OR REPLACE TASK process_cdc_stream_realtime
--SCHEDULE = 'USING CRON 0 * * * * UTC'  -- 매시간 정각 실행
--WAREHOUSE = 'CDC_WH'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '60 MINUTE'
AS
-- MERGE 로직은 동일
    MERGE INTO target_voc_dmnd_exns_m t
    USING (
        -- PK별 가장 최신 변경 이벤트만 선택 (데이터 중복 방지)
        WITH processed_data AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY RCPN_NUM
                    ORDER BY hd_change_seq DESC, hd_timestamp DESC
                ) AS rn
            FROM staging_voc_stream
            WHERE RCPN_NUM IS NOT NULL
        )
        SELECT *
        FROM processed_data
        WHERE rn = 1
    ) s ON t.RCPN_NUM = s.RCPN_NUM
    
    -- 1. DELETE 처리 (hd_change_oper = 'D')
    WHEN MATCHED AND s.hd_change_oper = 'D' THEN
        UPDATE SET
            _is_deleted = TRUE,
            _last_updated = s.hd_timestamp,
            _cdc_sequence = s.hd_change_seq
    
    -- 2. UPDATE 처리 (hd_change_oper = 'U' 또는 'I'인데 기존 데이터가 있는 경우)
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
            CDC_LD_DTTM = s.CDC_LD_DTTM,
            
            -- 메타데이터 업데이트
            _cdc_sequence = s.hd_change_seq,
            _last_updated = s.hd_timestamp,
            _is_deleted = FALSE
    
    -- 3. INSERT 처리 (hd_change_oper = 'I' 또는 'U'인데 새로운 데이터인 경우)
    WHEN NOT MATCHED AND s.hd_change_oper IN ('I', 'U') THEN
        INSERT (
            RCPN_NUM, VOC_DMND_TYPE_CD1, VOC_DMND_TYPE_CD2, VOC_DMND_TYPE_CD3,
            VOC_DMND_TYPE_CD4, VOC_DMND_TYPE_CD5, CATG_SSC_TYPE_CD,
            VOC_DMND_RCPN_REFN_CD_CONT, VOC_DMND_RCPN_REFN_NM, VOC_DMND_RCPN_REFN_CMNT_MEMO,
            KPI_SCP_CD, VIST_DT, RPPR_DEPT_CD, RPPR_EMPN, RPPR_NM, OPPB_YN,
            AGE_DV_CD, SMS_BLCKF_STAT_CD, VOC_RCPN_BRWS_CNT, CMPN_YN, DPLT_RCPN_YN,
            ARR_YN, TP_CNSL_YN, ANSR_RCMS_TYPE_CD, VOC_PROS_STG_INFM_CD, SEND_YN,
            INP_DTTM, VOC_OCRN_CITY_CD, INPR_ID, INP_PRGM_ID, UPD_DTTM,
            UPDR_ID, UPD_PRGM_ID, CDC_LD_DTTM, _cdc_sequence, _last_updated
        )
        VALUES (
            s.RCPN_NUM, s.VOC_DMND_TYPE_CD1, s.VOC_DMND_TYPE_CD2, s.VOC_DMND_TYPE_CD3,
            s.VOC_DMND_TYPE_CD4, s.VOC_DMND_TYPE_CD5, s.CATG_SSC_TYPE_CD,
            s.VOC_DMND_RCPN_REFN_CD_CONT, s.VOC_DMND_RCPN_REFN_NM, s.VOC_DMND_RCPN_REFN_CMNT_MEMO,
            s.KPI_SCP_CD, s.VIST_DT, s.RPPR_DEPT_CD, s.RPPR_EMPN, s.RPPR_NM, s.OPPB_YN,
            s.AGE_DV_CD, s.SMS_BLCKF_STAT_CD, s.VOC_RCPN_BRWS_CNT, s.CMPN_YN, s.DPLT_RCPN_YN,
            s.ARR_YN, s.TP_CNSL_YN, s.ANSR_RCMS_TYPE_CD, s.VOC_PROS_STG_INFM_CD, s.SEND_YN,
            s.INP_DTTM, s.VOC_OCRN_CITY_CD, s.INPR_ID, s.INP_PRGM_ID, s.UPD_DTTM,
            s.UPDR_ID, s.UPD_PRGM_ID, s.CDC_LD_DTTM, s.hd_change_seq, s.hd_timestamp
        );
-- 7. Task 시작
ALTER TASK process_cdc_stream_realtime RESUME;

-- 8. Snowpipe SQS 큐 확인
SHOW PIPES;
-- notification_channel에서 SQS ARN을 복사하여 S3 이벤트 알림 설정

select * from staging_voc_stream;


ALTER TASK process_cdc_stream_realtime SUSPEND;

show pipes;

--타겟 테이블 기존: 872128

--target table: 873039
select count(*) from target_voc_dmnd_exns_m;


select count(*) from target_voc_dmnd_exns_m;

SELECT SYSTEM$PIPE_STATUS('voc_cdc_auto_pipe');

SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
     TASK_NAME => 'process_cdc_stream_realtime'
     )) ORDER BY SCHEDULED_TIME DESC LIMIT 10;

SELECT
    FILE_NAME,
    STATUS, -- LOADED, SKIPPED, PARTIALLY_LOADED 등
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM
    TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'staging_voc_dmnd_exns_m',
        START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP()) -- 최근 1시간 동안의 기록 조회
    ))
ORDER BY LAST_LOAD_TIME DESC;

SHOW PIPES;

DESCRIBE PIPE voc_cdc_auto_pipe;



SELECT * 
FROM staging_voc_dmnd_exns_m;


SELECT * 
FROM STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_CT_TEST;


SELECT COUNT(*) FROM STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_CT_TEST;
SELECT COUNT(*) FROM STAGING_VOC_DMND_EXNS_M;

