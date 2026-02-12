use database poc;
use schema poc;

use warehouse cdc_wh;

-- =====================================================
-- Snowflake CDC 파이프라인 완전 구현
-- =====================================================

-- 1. External Table 생성 (ext_voc_dmnd_exns_m)
CREATE OR REPLACE EXTERNAL TABLE ext_voc_dmnd_exns_m (
    -- CDC 메타데이터 (소문자)
    hd_change_seq VARCHAR AS (value:hd_change_seq::VARCHAR),
    hd_change_oper VARCHAR AS (value:hd_change_oper::VARCHAR),
    hd_timestamp TIMESTAMP AS (value:hd_timestamp::TIMESTAMP),
    
    -- 비즈니스 컬럼들 (소문자)
    rcpn_num VARCHAR(10) AS (value:RCPN_NUM::VARCHAR),
    voc_dmnd_type_cd1 VARCHAR(30) AS (value:VOC_DMND_TYPE_CD1::VARCHAR),
    voc_dmnd_type_cd2 VARCHAR(30) AS (value:VOC_DMND_TYPE_CD2::VARCHAR),
    voc_dmnd_type_cd3 VARCHAR(30) AS (value:VOC_DMND_TYPE_CD3::VARCHAR),
    voc_dmnd_type_cd4 VARCHAR(30) AS (value:VOC_DMND_TYPE_CD4::VARCHAR),
    voc_dmnd_type_cd5 VARCHAR(30) AS (value:VOC_DMND_TYPE_CD5::VARCHAR),
    catg_ssc_type_cd VARCHAR(10) AS (value:CATG_SSC_TYPE_CD::VARCHAR),
    voc_dmnd_rcpn_refn_cd_cont VARCHAR(100) AS (value:VOC_DMND_RCPN_REFN_CD_CONT::VARCHAR),
    voc_dmnd_rcpn_refn_nm VARCHAR(500) AS (value:VOC_DMND_RCPN_REFN_NM::VARCHAR),
    voc_dmnd_rcpn_refn_cmnt_memo VARCHAR(500) AS (value:VOC_DMND_RCPN_REFN_CMNT_MEMO::VARCHAR),
    kpi_scp_cd VARCHAR(10) AS (value:KPI_SCP_CD::VARCHAR),
    vist_dt VARCHAR(8) AS (value:VIST_DT::VARCHAR),
    rppr_dept_cd VARCHAR(10) AS (value:RPPR_DEPT_CD::VARCHAR),
    rppr_empn VARCHAR(30) AS (value:RPPR_EMPN::VARCHAR),
    rppr_nm VARCHAR(150) AS (value:RPPR_NM::VARCHAR),
    oppb_yn VARCHAR(1) AS (value:OPPB_YN::VARCHAR),
    age_dv_cd VARCHAR(1) AS (value:AGE_DV_CD::VARCHAR),
    sms_blckf_stat_cd VARCHAR(1) AS (value:SMS_BLCKF_STAT_CD::VARCHAR),
    voc_rcpn_brws_cnt NUMBER(22) AS (value:VOC_RCPN_BRWS_CNT::NUMBER),
    cmpn_yn VARCHAR(1) AS (value:CMPN_YN::VARCHAR),
    dplt_rcpn_yn VARCHAR(1) AS (value:DPLT_RCPN_YN::VARCHAR),
    arr_yn VARCHAR(1) AS (value:ARR_YN::VARCHAR),
    tp_cnsl_yn VARCHAR(1) AS (value:TP_CNSL_YN::VARCHAR),
    ansr_rcms_type_cd VARCHAR(1) AS (value:ANSR_RCMS_TYPE_CD::VARCHAR),
    voc_pros_stg_infm_cd VARCHAR(1) AS (value:VOC_PROS_STG_INFM_CD::VARCHAR),
    send_yn VARCHAR(1) AS (value:SEND_YN::VARCHAR),
    inp_dttm TIMESTAMP AS (value:INP_DTTM::TIMESTAMP),
    voc_ocrn_city_cd VARCHAR(3) AS (value:VOC_OCRN_CITY_CD::VARCHAR),
    inpr_id VARCHAR(30) AS (value:INPR_ID::VARCHAR),
    inp_prgm_id VARCHAR(30) AS (value:INP_PRGM_ID::VARCHAR),
    upd_dttm TIMESTAMP AS (value:UPD_DTTM::TIMESTAMP),
    updr_id VARCHAR(30) AS (value:UPDR_ID::VARCHAR),
    upd_prgm_id VARCHAR(30) AS (value:UPD_PRGM_ID::VARCHAR),
    cdc_ld_dttm TIMESTAMP AS (value:CDC_LD_DTTM::TIMESTAMP),

    partition_date VARCHAR AS split_part(split_part(METADATA$FILENAME,'/', 4),'-',0)
)
PARTITION BY (partition_date)
WITH LOCATION = @"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M_ct/
AUTO_REFRESH = TRUE
FILE_FORMAT = parquet_format;

-- 2. Stream 생성 (stream_voc_dmnd_exns_m)
CREATE OR REPLACE STREAM stream_voc_dmnd_exns_m 
ON EXTERNAL TABLE ext_voc_dmnd_exns_m
INSERT_ONLY = TRUE;

-- 3. 기본 스테이징 테이블 생성 (stg_voc_dmnd_exns_m) - partition_date 제외
CREATE OR REPLACE TABLE stg_voc_dmnd_exns_m (
    -- CDC 메타데이터
    hd_change_seq VARCHAR,
    hd_change_oper VARCHAR,
    hd_timestamp TIMESTAMP,
    
    -- 비즈니스 컬럼들
    rcpn_num VARCHAR(10),
    voc_dmnd_type_cd1 VARCHAR(30),
    voc_dmnd_type_cd2 VARCHAR(30),
    voc_dmnd_type_cd3 VARCHAR(30),
    voc_dmnd_type_cd4 VARCHAR(30),
    voc_dmnd_type_cd5 VARCHAR(30),
    catg_ssc_type_cd VARCHAR(10),
    voc_dmnd_rcpn_refn_cd_cont VARCHAR(100),
    voc_dmnd_rcpn_refn_nm VARCHAR(500),
    voc_dmnd_rcpn_refn_cmnt_memo VARCHAR(500),
    kpi_scp_cd VARCHAR(10),
    vist_dt VARCHAR(8),
    rppr_dept_cd VARCHAR(10),
    rppr_empn VARCHAR(30),
    rppr_nm VARCHAR(150),
    oppb_yn VARCHAR(1),
    age_dv_cd VARCHAR(1),
    sms_blckf_stat_cd VARCHAR(1),
    voc_rcpn_brws_cnt NUMBER(22),
    cmpn_yn VARCHAR(1),
    dplt_rcpn_yn VARCHAR(1),
    arr_yn VARCHAR(1),
    tp_cnsl_yn VARCHAR(1),
    ansr_rcms_type_cd VARCHAR(1),
    voc_pros_stg_infm_cd VARCHAR(1),
    send_yn VARCHAR(1),
    inp_dttm TIMESTAMP,
    voc_ocrn_city_cd VARCHAR(3),
    inpr_id VARCHAR(30),
    inp_prgm_id VARCHAR(30),
    upd_dttm TIMESTAMP,
    updr_id VARCHAR(30),
    upd_prgm_id VARCHAR(30),
    cdc_ld_dttm TIMESTAMP,
);

-- 4. View 생성 (voc_dmnd_exns_m) - Stream과 기본 테이블 Union
CREATE OR REPLACE VIEW view_voc_dmnd_exns_m AS
WITH latest_data AS (
    -- Stream 데이터 (Metadata 제거)
    SELECT 
        hd_change_seq, hd_change_oper, hd_timestamp, rcpn_num,
        voc_dmnd_type_cd1, voc_dmnd_type_cd2, voc_dmnd_type_cd3, voc_dmnd_type_cd4, voc_dmnd_type_cd5,
        catg_ssc_type_cd, voc_dmnd_rcpn_refn_cd_cont, voc_dmnd_rcpn_refn_nm, voc_dmnd_rcpn_refn_cmnt_memo,
        kpi_scp_cd, vist_dt, rppr_dept_cd, rppr_empn, rppr_nm, oppb_yn, age_dv_cd, sms_blckf_stat_cd,
        voc_rcpn_brws_cnt, cmpn_yn, dplt_rcpn_yn, arr_yn, tp_cnsl_yn, ansr_rcms_type_cd,
        voc_pros_stg_infm_cd, send_yn, inp_dttm, voc_ocrn_city_cd, inpr_id, inp_prgm_id,
        upd_dttm, updr_id, upd_prgm_id, cdc_ld_dttm, cdc_timestamp, op
    FROM stream_voc_dmnd_exns_m
    
    UNION ALL
    
    -- 기본 테이블 데이터
    SELECT 
        hd_change_seq, hd_change_oper, hd_timestamp, rcpn_num,
        voc_dmnd_type_cd1, voc_dmnd_type_cd2, voc_dmnd_type_cd3, voc_dmnd_type_cd4, voc_dmnd_type_cd5,
        catg_ssc_type_cd, voc_dmnd_rcpn_refn_cd_cont, voc_dmnd_rcpn_refn_nm, voc_dmnd_rcpn_refn_cmnt_memo,
        kpi_scp_cd, vist_dt, rppr_dept_cd, rppr_empn, rppr_nm, oppb_yn, age_dv_cd, sms_blckf_stat_cd,
        voc_rcpn_brws_cnt, cmpn_yn, dplt_rcpn_yn, arr_yn, tp_cnsl_yn, ansr_rcms_type_cd,
        voc_pros_stg_infm_cd, send_yn, inp_dttm, voc_ocrn_city_cd, inpr_id, inp_prgm_id,
        upd_dttm, updr_id, upd_prgm_id, cdc_ld_dttm, cdc_timestamp, op
    FROM stg_voc_dmnd_exns_m
),
ranked_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY rcpn_num 
            ORDER BY 
                COALESCE(cdc_timestamp, hd_timestamp) DESC,
                COALESCE(hd_change_seq, '0') DESC
        ) as rn
    FROM latest_data
)
SELECT 
    hd_change_seq, hd_change_oper, hd_timestamp, rcpn_num,
    voc_dmnd_type_cd1, voc_dmnd_type_cd2, voc_dmnd_type_cd3, voc_dmnd_type_cd4, voc_dmnd_type_cd5,
    catg_ssc_type_cd, voc_dmnd_rcpn_refn_cd_cont, voc_dmnd_rcpn_refn_nm, voc_dmnd_rcpn_refn_cmnt_memo,
    kpi_scp_cd, vist_dt, rppr_dept_cd, rppr_empn, rppr_nm, oppb_yn, age_dv_cd, sms_blckf_stat_cd,
    voc_rcpn_brws_cnt, cmpn_yn, dplt_rcpn_yn, arr_yn, tp_cnsl_yn, ansr_rcms_type_cd,
    voc_pros_stg_infm_cd, send_yn, inp_dttm, voc_ocrn_city_cd, inpr_id, inp_prgm_id,
    upd_dttm, updr_id, upd_prgm_id, cdc_ld_dttm, cdc_timestamp, op
FROM ranked_data
WHERE rn = 1  -- PK별 가장 최근 데이터만
  AND NOT (op IN ('I', 'U') OR op IS NULL);  -- op가 'I', 'U' 또는 null인 경우 제외

-- 5. Task 생성 (task_voc_dmnd_exns_m) - poc.poc.target_table 업데이트 포함
CREATE OR REPLACE TASK task_voc_dmnd_exns_m
WAREHOUSE = 'cdc_wh'
SCHEDULE = 'USING CRON 0 * * * * UTC'  -- 매시간 정각 실행
WHEN SYSTEM$STREAM_HAS_DATA('stream_voc_dmnd_exns_m')
AS
BEGIN
    -- 1단계: Stream에서 Staging Table로 데이터 이동
    MERGE INTO stg_voc_dmnd_exns_m AS staging_target
    USING (
        WITH latest_stream_data AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY rcpn_num 
                    ORDER BY 
                        COALESCE(cdc_timestamp, hd_timestamp) DESC,
                        COALESCE(hd_change_seq, '0') DESC
                ) as rn
            FROM stream_voc_dmnd_exns_m
        )
        SELECT *
        FROM latest_stream_data
        WHERE rn = 1  -- PK별 가장 최근 데이터만
    ) AS staging_source ON staging_target.rcpn_num = staging_source.rcpn_num

    -- Staging Table 업데이트 로직
    WHEN MATCHED AND staging_source.op = 'D' THEN DELETE
    WHEN MATCHED AND staging_source.op = 'U' THEN UPDATE SET
        hd_change_seq = staging_source.hd_change_seq,
        hd_change_oper = staging_source.hd_change_oper,
        hd_timestamp = staging_source.hd_timestamp,
        voc_dmnd_type_cd1 = staging_source.voc_dmnd_type_cd1,
        voc_dmnd_type_cd2 = staging_source.voc_dmnd_type_cd2,
        voc_dmnd_type_cd3 = staging_source.voc_dmnd_type_cd3,
        voc_dmnd_type_cd4 = staging_source.voc_dmnd_type_cd4,
        voc_dmnd_type_cd5 = staging_source.voc_dmnd_type_cd5,
        catg_ssc_type_cd = staging_source.catg_ssc_type_cd,
        voc_dmnd_rcpn_refn_cd_cont = staging_source.voc_dmnd_rcpn_refn_cd_cont,
        voc_dmnd_rcpn_refn_nm = staging_source.voc_dmnd_rcpn_refn_nm,
        voc_dmnd_rcpn_refn_cmnt_memo = staging_source.voc_dmnd_rcpn_refn_cmnt_memo,
        kpi_scp_cd = staging_source.kpi_scp_cd,
        vist_dt = staging_source.vist_dt,
        rppr_dept_cd = staging_source.rppr_dept_cd,
        rppr_empn = staging_source.rppr_empn,
        rppr_nm = staging_source.rppr_nm,
        oppb_yn = staging_source.oppb_yn,
        age_dv_cd = staging_source.age_dv_cd,
        sms_blckf_stat_cd = staging_source.sms_blckf_stat_cd,
        voc_rcpn_brws_cnt = staging_source.voc_rcpn_brws_cnt,
        cmpn_yn = staging_source.cmpn_yn,
        dplt_rcpn_yn = staging_source.dplt_rcpn_yn,
        arr_yn = staging_source.arr_yn,
        tp_cnsl_yn = staging_source.tp_cnsl_yn,
        ansr_rcms_type_cd = staging_source.ansr_rcms_type_cd,
        voc_pros_stg_infm_cd = staging_source.voc_pros_stg_infm_cd,
        send_yn = staging_source.send_yn,
        inp_dttm = staging_source.inp_dttm,
        voc_ocrn_city_cd = staging_source.voc_ocrn_city_cd,
        inpr_id = staging_source.inpr_id,
        inp_prgm_id = staging_source.inp_prgm_id,
        upd_dttm = staging_source.upd_dttm,
        updr_id = staging_source.updr_id,
        upd_prgm_id = staging_source.upd_prgm_id,
        cdc_ld_dttm = staging_source.cdc_ld_dttm,
        cdc_timestamp = staging_source.cdc_timestamp,
        op = staging_source.op
    WHEN NOT MATCHED AND (staging_source.op IN ('I', 'U') OR staging_source.op IS NULL) THEN INSERT (
        hd_change_seq, hd_change_oper, hd_timestamp, rcpn_num, voc_dmnd_type_cd1,
        voc_dmnd_type_cd2, voc_dmnd_type_cd3, voc_dmnd_type_cd4, voc_dmnd_type_cd5,
        catg_ssc_type_cd, voc_dmnd_rcpn_refn_cd_cont, voc_dmnd_rcpn_refn_nm,
        voc_dmnd_rcpn_refn_cmnt_memo, kpi_scp_cd, vist_dt, rppr_dept_cd, rppr_empn,
        rppr_nm, oppb_yn, age_dv_cd, sms_blckf_stat_cd, voc_rcpn_brws_cnt, cmpn_yn,
        dplt_rcpn_yn, arr_yn, tp_cnsl_yn, ansr_rcms_type_cd, voc_pros_stg_infm_cd,
        send_yn, inp_dttm, voc_ocrn_city_cd, inpr_id, inp_prgm_id, upd_dttm,
        updr_id, upd_prgm_id, cdc_ld_dttm, cdc_timestamp, op
    ) VALUES (
        staging_source.hd_change_seq, staging_source.hd_change_oper, staging_source.hd_timestamp,
        staging_source.rcpn_num, staging_source.voc_dmnd_type_cd1, staging_source.voc_dmnd_type_cd2,
        staging_source.voc_dmnd_type_cd3, staging_source.voc_dmnd_type_cd4, staging_source.voc_dmnd_type_cd5,
        staging_source.catg_ssc_type_cd, staging_source.voc_dmnd_rcpn_refn_cd_cont,
        staging_source.voc_dmnd_rcpn_refn_nm, staging_source.voc_dmnd_rcpn_refn_cmnt_memo,
        staging_source.kpi_scp_cd, staging_source.vist_dt, staging_source.rppr_dept_cd,
        staging_source.rppr_empn, staging_source.rppr_nm, staging_source.oppb_yn,
        staging_source.age_dv_cd, staging_source.sms_blckf_stat_cd, staging_source.voc_rcpn_brws_cnt,
        staging_source.cmpn_yn, staging_source.dplt_rcpn_yn, staging_source.arr_yn,
        staging_source.tp_cnsl_yn, staging_source.ansr_rcms_type_cd, staging_source.voc_pros_stg_infm_cd,
        staging_source.send_yn, staging_source.inp_dttm, staging_source.voc_ocrn_city_cd,
        staging_source.inpr_id, staging_source.inp_prgm_id, staging_source.upd_dttm,
        staging_source.updr_id, staging_source.upd_prgm_id, staging_source.cdc_ld_dttm,
        staging_source.cdc_timestamp, staging_source.op
    );

    -- 2단계: Staging Table에서 Target Table로 최종 반영
    MERGE INTO poc.poc.target_table AS target
    USING (
        SELECT 
            rcpn_num, voc_dmnd_type_cd1, voc_dmnd_type_cd2, voc_dmnd_type_cd3,
            voc_dmnd_type_cd4, voc_dmnd_type_cd5, catg_ssc_type_cd,
            voc_dmnd_rcpn_refn_cd_cont, voc_dmnd_rcpn_refn_nm, voc_dmnd_rcpn_refn_cmnt_memo,
            kpi_scp_cd, vist_dt, rppr_dept_cd, rppr_empn, rppr_nm, oppb_yn,
            age_dv_cd, sms_blckf_stat_cd, voc_rcpn_brws_cnt, cmpn_yn, dplt_rcpn_yn,
            arr_yn, tp_cnsl_yn, ansr_rcms_type_cd, voc_pros_stg_infm_cd, send_yn,
            inp_dttm, voc_ocrn_city_cd, inpr_id, inp_prgm_id, upd_dttm,
            updr_id, upd_prgm_id, cdc_ld_dttm, op
        FROM stg_voc_dmnd_exns_m
        WHERE op != 'D'  -- 삭제된 레코드는 제외
    ) AS source ON target.rcpn_num = source.rcpn_num

    -- Target Table 업데이트
    WHEN MATCHED THEN UPDATE SET
        voc_dmnd_type_cd1 = source.voc_dmnd_type_cd1,
        voc_dmnd_type_cd2 = source.voc_dmnd_type_cd2,
        voc_dmnd_type_cd3 = source.voc_dmnd_type_cd3,
        voc_dmnd_type_cd4 = source.voc_dmnd_type_cd4,
        voc_dmnd_type_cd5 = source.voc_dmnd_type_cd5,
        catg_ssc_type_cd = source.catg_ssc_type_cd,
        voc_dmnd_rcpn_refn_cd_cont = source.voc_dmnd_rcpn_refn_cd_cont,
        voc_dmnd_rcpn_refn_nm = source.voc_dmnd_rcpn_refn_nm,
        voc_dmnd_rcpn_refn_cmnt_memo = source.voc_dmnd_rcpn_refn_cmnt_memo,
        kpi_scp_cd = source.kpi_scp_cd,
        vist_dt = source.vist_dt,
        rppr_dept_cd = source.rppr_dept_cd,
        rppr_empn = source.rppr_empn,
        rppr_nm = source.rppr_nm,
        oppb_yn = source.oppb_yn,
        age_dv_cd = source.age_dv_cd,
        sms_blckf_stat_cd = source.sms_blckf_stat_cd,
        voc_rcpn_brws_cnt = source.voc_rcpn_brws_cnt,
        cmpn_yn = source.cmpn_yn,
        dplt_rcpn_yn = source.dplt_rcpn_yn,
        arr_yn = source.arr_yn,
        tp_cnsl_yn = source.tp_cnsl_yn,
        ansr_rcms_type_cd = source.ansr_rcms_type_cd,
        voc_pros_stg_infm_cd = source.voc_pros_stg_infm_cd,
        send_yn = source.send_yn,
        inp_dttm = source.inp_dttm,
        voc_ocrn_city_cd = source.voc_ocrn_city_cd,
        inpr_id = source.inpr_id,
        inp_prgm_id = source.inp_prgm_id,
        upd_dttm = source.upd_dttm,
        updr_id = source.updr_id,
        upd_prgm_id = source.upd_prgm_id,
        cdc_ld_dttm = source.cdc_ld_dttm

    -- Target Table에 새 레코드 삽입
    WHEN NOT MATCHED THEN INSERT (
        rcpn_num, voc_dmnd_type_cd1, voc_dmnd_type_cd2, voc_dmnd_type_cd3,
        voc_dmnd_type_cd4, voc_dmnd_type_cd5, catg_ssc_type_cd,
        voc_dmnd_rcpn_refn_cd_cont, voc_dmnd_rcpn_refn_nm, voc_dmnd_rcpn_refn_cmnt_memo,
        kpi_scp_cd, vist_dt, rppr_dept_cd, rppr_empn, rppr_nm, oppb_yn,
        age_dv_cd, sms_blckf_stat_cd, voc_rcpn_brws_cnt, cmpn_yn, dplt_rcpn_yn,
        arr_yn, tp_cnsl_yn, ansr_rcms_type_cd, voc_pros_stg_infm_cd, send_yn,
        inp_dttm, voc_ocrn_city_cd, inpr_id, inp_prgm_id, upd_dttm,
        updr_id, upd_prgm_id, cdc_ld_dttm
    ) VALUES (
        source.rcpn_num, source.voc_dmnd_type_cd1, source.voc_dmnd_type_cd2,
        source.voc_dmnd_type_cd3, source.voc_dmnd_type_cd4, source.voc_dmnd_type_cd5,
        source.catg_ssc_type_cd, source.voc_dmnd_rcpn_refn_cd_cont,
        source.voc_dmnd_rcpn_refn_nm, source.voc_dmnd_rcpn_refn_cmnt_memo,
        source.kpi_scp_cd, source.vist_dt, source.rppr_dept_cd, source.rppr_empn,
        source.rppr_nm, source.oppb_yn, source.age_dv_cd, source.sms_blckf_stat_cd,
        source.voc_rcpn_brws_cnt, source.cmpn_yn, source.dplt_rcpn_yn, source.arr_yn,
        source.tp_cnsl_yn, source.ansr_rcms_type_cd, source.voc_pros_stg_infm_cd,
        source.send_yn, source.inp_dttm, source.voc_ocrn_city_cd, source.inpr_id,
        source.inp_prgm_id, source.upd_dttm, source.updr_id, source.upd_prgm_id,
        source.cdc_ld_dttm
    );

    -- 3단계: 삭제된 레코드 처리 (Target Table에서 물리적 삭제)
    DELETE FROM poc.poc.target_table 
    WHERE rcpn_num IN (
        SELECT rcpn_num 
        FROM stg_voc_dmnd_exns_m 
        WHERE op = 'D'
    );

END;

select count(*) from ext_voc_dmnd_exns_m;

select * from stream_voc_dmnd_exns_m;

select * from latest_stream_data;
-- 6. Task 시작
ALTER TASK task_voc_dmnd_exns_m RESUME;

-- =====================================================
-- 모니터링 쿼리
-- =====================================================

-- External Table 데이터 확인
SELECT COUNT(*) FROM ext_voc_dmnd_exns_m;

-- Stream 데이터 확인  
SELECT COUNT(*) FROM stream_voc_dmnd_exns_m;

-- Staging Table 데이터 확인
SELECT COUNT(*) FROM stg_voc_dmnd_exns_m;

-- Target Table 데이터 확인 (최종 결과)
SELECT COUNT(*) FROM poc.poc.target_table;

-- View를 통한 통합 데이터 확인
SELECT COUNT(*) FROM voc_dmnd_exns_m;

-- Task 실행 이력 확인
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'TASK_VOC_DMND_EXNS_M'
-- )) ORDER BY SCHEDULED_TIME DESC LIMIT 10;