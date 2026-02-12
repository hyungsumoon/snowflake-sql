use role sysadmin;

-- create database if not exists mhsdb;
use database mhsdb;

-- create schema if not exists mhsschema;
use schema mhsschema;

-- CREATE OR REPLACE TABLE mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M_MHS (
--     CROS_PROD_RCMN_SEQ NUMBER(20) NOT NULL COMMENT '교차상품추천순번',
--     RCMN_SVC_STAT_VAL VARCHAR(1) NOT NULL COMMENT '추천서비스상태값',
--     RCMN_CITY_CD VARCHAR(10) COMMENT '추천도시코드',
--     CUST_NUM VARCHAR(12) COMMENT '고객번호',
--     RCMN_PROD_SEQ NUMBER(5) COMMENT '추천상품순번',
--     RES_ATTR_CD VARCHAR(2) COMMENT '예약속성코드',
--     RCMN_PROD_CD VARCHAR(30) COMMENT '추천상품코드',
--     RCMN_PROD_NM VARCHAR(1000) COMMENT '추천상품명',
--     RCMN_PROD_MINZ_NM VARCHAR(1000) COMMENT '추천상품축약명',
--     PROD_BRND_NM VARCHAR(50) COMMENT '상품브랜드명',
--     LDG_TYPE_NM VARCHAR(50) COMMENT '숙박유형명',
--     AIRL_CD VARCHAR(2) COMMENT '항공사코드',
--     AIRL_NM VARCHAR(100) COMMENT '항공사명',
--     DEP_APT_CD VARCHAR(3) COMMENT '출발공항코드',
--     DEP_APT_NM VARCHAR(150) COMMENT '출발공항명',
--     ARR_APT_CD VARCHAR(3) COMMENT '도착공항코드',
--     ARR_APT_NM VARCHAR(150) COMMENT '도착공항명',
--     AIR_SEAT_GRAD_CD VARCHAR(2) COMMENT '항공좌석등급코드',
--     AIR_SEAT_GRAD_NM VARCHAR(50) COMMENT '항공좌석등급명',
--     DOME_OVRS_DV_CD VARCHAR(1) COMMENT '국내해외구분코드',
--     PROD_CNTNT_URL_ADRS VARCHAR(300) COMMENT '상품컨텐츠URL주소',
--     RCMN_PROD_PRC NUMBER(18, 4) COMMENT '추천상품가격',
--     RCMN_SCR NUMBER(19, 3) COMMENT '추천점수',
--     CNTRY_CD VARCHAR(3) COMMENT '국가코드',
--     DEP_DT VARCHAR(8) COMMENT '출발일자',
--     DEP_TM VARCHAR(4) COMMENT '출발시간',
--     TRVL_DAY_CNT NUMBER(9) COMMENT '여행일수',
--     PROD_DTL_ATTR_CD VARCHAR(2) COMMENT '상품상세속성코드',
--     HTL_GRAD_CD VARCHAR(8) COMMENT '호텔등급코드',
--     LA NUMBER(17, 14) COMMENT '위도',
--     LO NUMBER(17, 14) COMMENT '경도',
--     PROD_DV_CD VARCHAR(3) COMMENT '상품구분코드',
--     PROD_MSTR_CD VARCHAR(6) COMMENT '상품마스터코드',
--     HTL_CD VARCHAR(10) COMMENT '호텔코드',
--     HTL_KO_NM VARCHAR(500) COMMENT '호텔한글명',
--     INP_DTTM TIMESTAMP COMMENT '입력일시',
--     INPR_ID VARCHAR(30) COMMENT '입력자ID',
--     INP_PRGM_ID VARCHAR(30) COMMENT '입력프로그램ID',
--     UPD_DTTM TIMESTAMP COMMENT '변경일시',
--     UPDR_ID VARCHAR(30) COMMENT '변경자ID',
--     UPD_PRGM_ID VARCHAR(30) COMMENT '변경프로그램ID',
--     PRIMARY KEY (CROS_PROD_RCMN_SEQ, RCMN_SVC_STAT_VAL)
-- ) COMMENT = 'IFPD_교차상품추천기본';


--------------------------------------------------------------------------
-- 원본
-- CREATE OR REPLACE PROCEDURE mhsdb.mhsschema.ETL_CRM_TRUNCATE_AND_COPY()
-- RETURNS STRING
-- LANGUAGE SQL
-- EXECUTE AS OWNER 
-- AS
-- BEGIN  
--   -- -- Step 1: Truncate all tables
--   ASYNC (TRUNCATE TABLE mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M);
--   AWAIT ALL;

--   -- -- Step 2: Copy data to all tables
--   ASYNC (COPY INTO mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M FROM $$@STG_EXTERNAL.CDC.COBALT/CCS01.IFPD_CROSS_PROD_RCMN_M/$$ MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FILE_FORMAT = ( TYPE = PARQUET, REPLACE_INVALID_CHARACTERS = TRUE ) PATTERN = '.*\.parquet$' FORCE = TRUE);
--   AWAIT ALL;
--   RETURN 'mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M operations completed.';
-- END;

--------------------------------------------------------------------------
use role rl_dbt_user;
select * from mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M_MHS;
-- TRUNCATE TABLE IF EXISTS mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M_MHS;
insert into mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M_MHS (CROS_PROD_RCMN_SEQ, RCMN_SVC_STAT_VAL) values(999,'Z');
insert into mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M_MHS (CROS_PROD_RCMN_SEQ, RCMN_SVC_STAT_VAL) values(998,'X');
select * from mhsdb.mhsschema.IFPD_CROSS_PROD_RCMN_M_MHS;
-- commit;