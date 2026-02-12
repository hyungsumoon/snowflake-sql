--- Task 용 테이블 생성
CREATE OR REPLACE TABLE STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_TEST2
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M/LOAD00000001.snappy.parquet',
      FILE_FORMAT=>'STG_EXTERNAL.CDC.PARQUET'
    )
  )
);

--- 데이터 로드
COPY INTO STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_TEST2
FROM '@"STG_EXTERNAL"."CDC"."NEON"/HUB01.VOC_DMND_EXNS_M/'
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
FILE_FORMAT = (FORMAT_NAME = 'STG_EXTERNAL.CDC.PARQUET')
ON_ERROR='CONTINUE'
PATTERN = '.*\\.parquet$';

    

-- CREATE OR REPLACE TASK STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_CT_TASK AS
    WITH CDC_TABLE AS (
        SELECT T2.* 
        FROM ( 
            SELECT T1.*,
                   ROW_NUMBER() OVER(PARTITION BY T1.RCPN_NUM ORDER BY "hd_change_seq" DESC) AS RNUM
            FROM STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_CT_TEST2 T1
        ) T2
        WHERE T2.RNUM = 1
    )
    , DATA_SCHEMA AS (
        SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
        FROM STG_EXTERNAL.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = 'VOC_DMND_EXNS_M_CT_TEST2'
    )
    , LAST_SEQ AS (
        SELECT "hd_change_seq"
        FROM CDC_TABLE
        ORDER BY "hd_change_seq" DESC
        LIMIT 1
    )
    , LOG_TABLE AS (
        SELECT CURRENT_TIMESTAMP(), t1.*, t2.*
        FROM DATA_SCHEMA t1
        CROSS JOIN LAST_SEQ t2
    )

    
    ---- 2 머지하기
    MERGE INTO STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_TEST AS TARGET
    USING(
        SELECT T2.* 
        FROM ( 
            SELECT T1.*,
                   ROW_NUMBER() OVER(PARTITION BY T1.RCPN_NUM ORDER BY "hd_change_seq" DESC) AS RNUM
            FROM STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_CT_TEST2 T1
        ) T2
        WHERE T2.RNUM = 1
    ) AS CDC_SOURCE
    
    ON TARGET.RCPN_NUM = CDC_SOURCE.RCPN_NUM
    
        WHEN MATCHED AND CDC_SOURCE."hd_change_oper" = 'U' THEN
            UPDATE SET 
                TARGET.RCPN_NUM = CDC_SOURCE.RCPN_NUM,
                TARGET.VOC_DMND_TYPE_CD1 = CDC_SOURCE.VOC_DMND_TYPE_CD1,
                TARGET.VOC_DMND_TYPE_CD2 = CDC_SOURCE.VOC_DMND_TYPE_CD2,
                TARGET.VOC_DMND_TYPE_CD3 = CDC_SOURCE.VOC_DMND_TYPE_CD3,
                TARGET.VOC_DMND_TYPE_CD4 = CDC_SOURCE.VOC_DMND_TYPE_CD4,
                TARGET.VOC_DMND_TYPE_CD5 = CDC_SOURCE.VOC_DMND_TYPE_CD5,
                TARGET.CATG_SSC_TYPE_CD = CDC_SOURCE.CATG_SSC_TYPE_CD,
                TARGET.VOC_DMND_RCPN_REFN_CD_CONT = CDC_SOURCE.VOC_DMND_RCPN_REFN_CD_CONT,
                TARGET.VOC_DMND_RCPN_REFN_NM = CDC_SOURCE.VOC_DMND_RCPN_REFN_NM,
                TARGET.VOC_DMND_RCPN_REFN_CMNT_MEMO = CDC_SOURCE.VOC_DMND_RCPN_REFN_CMNT_MEMO,
                TARGET.KPI_SCP_CD = CDC_SOURCE.KPI_SCP_CD,
                TARGET.VIST_DT = CDC_SOURCE.VIST_DT,
                TARGET.RPPR_DEPT_CD = CDC_SOURCE.RPPR_DEPT_CD,
                TARGET.RPPR_EMPN = CDC_SOURCE.RPPR_EMPN,
                TARGET.RPPR_NM = CDC_SOURCE.RPPR_NM,
                TARGET.OPPB_YN = CDC_SOURCE.OPPB_YN,
                TARGET.AGE_DV_CD = CDC_SOURCE.AGE_DV_CD,
                TARGET.SMS_BLCKF_STAT_CD = CDC_SOURCE.SMS_BLCKF_STAT_CD,
                TARGET.VOC_RCPN_BRWS_CNT = CDC_SOURCE.VOC_RCPN_BRWS_CNT,
                TARGET.CMPN_YN = CDC_SOURCE.CMPN_YN,
                TARGET.DPLT_RCPN_YN = CDC_SOURCE.DPLT_RCPN_YN,
                TARGET.ARR_YN = CDC_SOURCE.ARR_YN,
                TARGET.TP_CNSL_YN = CDC_SOURCE.TP_CNSL_YN,
                TARGET.ANSR_RCMS_TYPE_CD = CDC_SOURCE.ANSR_RCMS_TYPE_CD,
                TARGET.VOC_PROS_STG_INFM_CD = CDC_SOURCE.VOC_PROS_STG_INFM_CD,
                TARGET.SEND_YN = CDC_SOURCE.SEND_YN,
                TARGET.INP_DTTM = CDC_SOURCE.INP_DTTM,
                TARGET.VOC_OCRN_CITY_CD = CDC_SOURCE.VOC_OCRN_CITY_CD,
                TARGET.INPR_ID = CDC_SOURCE.INPR_ID,
                TARGET.INP_PRGM_ID = CDC_SOURCE.INP_PRGM_ID,
                TARGET.UPD_DTTM = CDC_SOURCE.UPD_DTTM,
                TARGET.UPDR_ID = CDC_SOURCE.UPDR_ID,
                TARGET.UPD_PRGM_ID = CDC_SOURCE.UPD_PRGM_ID,
                TARGET.CDC_LD_DTTM = CDC_SOURCE.CDC_LD_DTTM
        
        WHEN MATCHED AND CDC_SOURCE."hd_change_oper" = 'D' THEN
            DELETE
        
        WHEN NOT MATCHED AND CDC_SOURCE."hd_change_oper" = 'I' THEN
            INSERT (
                TARGET.RCPN_NUM, 
                TARGET.VOC_DMND_TYPE_CD1, 
                TARGET.VOC_DMND_TYPE_CD2, 
                TARGET.VOC_DMND_TYPE_CD3, 
                TARGET.VOC_DMND_TYPE_CD4, 
                TARGET.VOC_DMND_TYPE_CD5, 
                TARGET.CATG_SSC_TYPE_CD, 
                TARGET.VOC_DMND_RCPN_REFN_CD_CONT, 
                TARGET.VOC_DMND_RCPN_REFN_NM, 
                TARGET.VOC_DMND_RCPN_REFN_CMNT_MEMO, 
                TARGET.KPI_SCP_CD, 
                TARGET.VIST_DT, 
                TARGET.RPPR_DEPT_CD, 
                TARGET.RPPR_EMPN, 
                TARGET.RPPR_NM, 
                TARGET.OPPB_YN, 
                TARGET.AGE_DV_CD, 
                TARGET.SMS_BLCKF_STAT_CD, 
                TARGET.VOC_RCPN_BRWS_CNT, 
                TARGET.CMPN_YN, 
                TARGET.DPLT_RCPN_YN, 
                TARGET.ARR_YN, 
                TARGET.TP_CNSL_YN, 
                TARGET.ANSR_RCMS_TYPE_CD, 
                TARGET.VOC_PROS_STG_INFM_CD, 
                TARGET.SEND_YN, 
                TARGET.INP_DTTM, 
                TARGET.VOC_OCRN_CITY_CD, 
                TARGET.INPR_ID, 
                TARGET.INP_PRGM_ID, 
                TARGET.UPD_DTTM, 
                TARGET.UPDR_ID,
                TARGET.UPD_PRGM_ID, 
                TARGET.CDC_LD_DTTM
            )
            VALUES(
                CDC_SOURCE.RCPN_NUM, 
                CDC_SOURCE.VOC_DMND_TYPE_CD1, 
                CDC_SOURCE.VOC_DMND_TYPE_CD2, 
                CDC_SOURCE.VOC_DMND_TYPE_CD3, 
                CDC_SOURCE.VOC_DMND_TYPE_CD4, 
                CDC_SOURCE.VOC_DMND_TYPE_CD5, 
                CDC_SOURCE.CATG_SSC_TYPE_CD, 
                CDC_SOURCE.VOC_DMND_RCPN_REFN_CD_CONT, 
                CDC_SOURCE.VOC_DMND_RCPN_REFN_NM, 
                CDC_SOURCE.VOC_DMND_RCPN_REFN_CMNT_MEMO, 
                CDC_SOURCE.KPI_SCP_CD, 
                CDC_SOURCE.VIST_DT, 
                CDC_SOURCE.RPPR_DEPT_CD, 
                CDC_SOURCE.RPPR_EMPN, 
                CDC_SOURCE.RPPR_NM, 
                CDC_SOURCE.OPPB_YN, 
                CDC_SOURCE.AGE_DV_CD, 
                CDC_SOURCE.SMS_BLCKF_STAT_CD, 
                CDC_SOURCE.VOC_RCPN_BRWS_CNT, 
                CDC_SOURCE.CMPN_YN, 
                CDC_SOURCE.DPLT_RCPN_YN, 
                CDC_SOURCE.ARR_YN, 
                CDC_SOURCE.TP_CNSL_YN, 
                CDC_SOURCE.ANSR_RCMS_TYPE_CD, 
                CDC_SOURCE.VOC_PROS_STG_INFM_CD, 
                CDC_SOURCE.SEND_YN, 
                CDC_SOURCE.INP_DTTM, 
                CDC_SOURCE.VOC_OCRN_CITY_CD, 
                CDC_SOURCE.INPR_ID, 
                CDC_SOURCE.INP_PRGM_ID, 
                CDC_SOURCE.UPD_DTTM, 
                CDC_SOURCE.UPDR_ID,
                CDC_SOURCE.UPD_PRGM_ID, 
                CDC_SOURCE.CDC_LD_DTTM
            )
    ;

--     WITH merge_result AS (
--     SELECT *
--     FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
--     )
--     SELECT 
--         rows_inserted,
--         rows_updated,
--         rows_deleted
--     FROM merge_result;

    
    



-- SELECT * FROM STG_EXTERNAL.CDC.VOC_DMND_EXNS_M_CT_TEST
-- ORDER BY "hd_change_seq" DESC
-- ;