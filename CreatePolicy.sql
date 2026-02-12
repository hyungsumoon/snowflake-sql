use securityadmin;

-- use database sys_security;
-- use schema hnt_policy;
USE SCHEMA sys_security.hnt_policy;
----------------------------------------------------------------------------------
select current_role();
-- show roles;
-- show grants to user current_user();
show password policies; -- 해당 스키마에서 조회
-- show parameters like 'PASSWORD_POLICY' in account;

-- drop PASSWORD POLICY sys_security.hnt_policy.policy_user_password;
CREATE OR REPLACE PASSWORD POLICY sys_security.hnt_policy.policy_user_password
    PASSWORD_MIN_LENGTH = 10
    PASSWORD_MAX_LENGTH = 64
    PASSWORD_MIN_UPPER_CASE_CHARS = 1
    PASSWORD_MIN_LOWER_CASE_CHARS = 1
    PASSWORD_MIN_NUMERIC_CHARS = 1
    PASSWORD_MIN_SPECIAL_CHARS = 1
    PASSWORD_MAX_AGE_DAYS = 90
    PASSWORD_MAX_RETRIES = 5
    PASSWORD_LOCKOUT_TIME_MINS = 30
    PASSWORD_HISTORY = 3
    COMMENT = '사용자 패스워드 보안 정책';

-- ALTER USER H2640 SET PASSWORD_POLICY = sys_security.hnt_policy.policy_user_password;

----------------------------------------------------------------------------------
SHOW SESSION POLICIES;
-- SHOW PARAMETERS LIKE 'SESSION%' IN USER userA;

DROP SESSION POLICY sys_security.hnt_policy.policy_user_timeout_30min;
CREATE OR REPLACE SESSION POLICY sys_security.hnt_policy.policy_user_timeout_30mins
    SESSION_IDLE_TIMEOUT_MINS = 30
    SESSION_UI_IDLE_TIMEOUT_MINS = 15
    COMMENT = '사용자 세션 타임아웃 정책';

CREATE OR REPLACE SESSION POLICY sys_security.hnt_policy.policy_user_timeout_12hours
    SESSION_IDLE_TIMEOUT_MINS = 720
    SESSION_UI_IDLE_TIMEOUT_MINS = 720
    COMMENT = '사용자 세션 타임아웃 정책';
    
-- ALTER USER userA SET SESSION POLICY = sp_user_timeout;
-- ALTER USER userA UNSET SESSION POLICY;

-- 보안정책을 role에 부여하는 것은 부적절하다.
-- 유저는 여러 role을 부여받으므로 role에 대해 보안정책을 적용하면 혼선이 발생한다.
-- ALTER ROLE rl_readonly SET SESSION POLICY = sp_user_timeout;
-- ALTER ROLE rl_readonly UNSET SESSION POLICY;
----------------------------------------------------------------------------------
SHOW MASKING POLICIES;

CREATE OR REPLACE MASKING POLICY sys_security.hnt_policy.policy_masking;
AS (val STRING, col_tag VARCHAR)
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN','SECURITYADMIN',col_tag) THEN val
    WHEN val IS NULL THEN NULL
    ELSE '******'
  END;

-- ALTER TABLE CDC.CST01.CST_MEM_M 
--   MODIFY COLUMN KO_NM SET MASKING POLICY SECURITY.mhs_policy.mhs_mask1;
  
-- ALTER TABLE CDC.CST01.CST_MEM_M 
--   MODIFY COLUMN KO_NM UNSET MASKING POLICY;



-- 일반 유저로 데이터확인
use role useradmin; --masking o
select KO_NM, a.* from CDC.CST01.CST_MEM_M a where KO_NM is not null limit 10;

------------------------------------------------------------------
-- Monitoring Policy
use role accountadmin;
use database SECURITY;
use schema mhs_policy;
SHOW MASKING POLICIES;
-- SHOW MASKING POLICIES IN DATABASE <DB_NAME>;
-- SHOW MASKING POLICIES IN SCHEMA <DB_NAME>.<SCHEMA_NAME>;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES
ORDER BY POLICY_NAME;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
ORDER BY POLICY_NAME, REF_COLUMN_NAME;

SELECT CURRENT_ROLE(), CURRENT_USER(), current_database(), current_schema();


USE DATABASE CDC;
USE SCHEMA CST01;
-- SHOW COLUMNS IN TABLE CST_MEM_M; --x
DESC TABLE CDC.CST01.CST_MEM_M; --o // policy_name

------------------------------------------------------------------
-- 1. 계정 전체 DB 목록 가져오기
-- SELECT s.catalog_name, s.schema_name
--     FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA s;

-- 2. 각 DB의 SCHEMA 목록 가져오기
with cols as (
    SELECT c.*
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
    JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLES t ON t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema and t.table_name = c.table_name
    JOIN SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA s ON t.table_catalog = s.catalog_name AND t.table_schema = s.schema_name
    JOIN SNOWFLAKE.ACCOUNT_USAGE.DATABASES d ON s.catalog_name = d.database_name
    WHERE s.deleted IS NULL
    and d.DELETED IS NULL
    and t.deleted IS NULL
    and c.deleted IS NULL
    )
select c2.*, c1.*
from cols c1, LATERAL (
    -- SHOW COLUMNS 결과를 RESULT_SCAN으로 읽기
    SELECT *
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
) c2
WHERE 1=1
;

USE DATABASE CDC;
USE SCHEMA CST01;
-- SHOW COLUMNS IN TABLE CST_MEM_M;--x
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE 1=1
and "policy_name" IS NOT NULL
;
DESC TABLE CDC.CST01.CST_MEM_M; --o // policy_name
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE 1=1
and "policy name" IS NOT NULL --x
-- and "policy_name" IS NOT NULL --x
-- and policy_name IS NOT NULL --x
;


SHOW ORGANIZATION ACCOUNTS; --x

SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_VERSION(), EDITION
FROM SNOWFLAKE.ACCOUNT_USAGE.ORGANIZATION_ACCOUNTS
WHERE ACCOUNT_NAME = CURRENT_ACCOUNT(); --x

SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_VERSION(), SYSTEM$GET_ACCOUNT_PARAMETER('edition') AS EDITION;

SELECT SYSTEM$GET_ACCOUNT_PARAMETER('edition') AS EDITION;



------------------------------------------------------------------
-- 태그 기반 자동 적용 // 나중에 해보자
-- CREATE OR REPLACE TAG PII_TAG COMMENT = 'PII Column';

-- ALTER TABLE hr_db.public.employee
-- MODIFY COLUMN email SET TAG PII_TAG = 'EMAIL';

-- CREATE MASKING POLICY security_db.policies.pii_policy
-- AS (val STRING, tag_value STRING) RETURNS STRING ->
-- CASE
--   WHEN CURRENT_ROLE() IN ('VIEW_ORIGINAL_ROLE') THEN val
--   ELSE '****MASKED****'
-- END;

-- ALTER TAG PII_TAG SET MASKING POLICY security_db.policies.pii_policy;



----------------------------------------------------------------------------------
-- -- Dynamic Data Masking
-- CREATE MASKING POLICY employee_ssn_mask AS (val string) RETURNS string ->
--   CASE
--     WHEN CURRENT_ROLE() IN ('PAYROLL') THEN val
--     ELSE '******'
--   END;

-- -- External Tokenization
--   CREATE MASKING POLICY employee_ssn_detokenize AS (val string) RETURNS string ->
--   CASE
--     WHEN CURRENT_ROLE() IN ('PAYROLL') THEN ssn_unprotect(VAL)
--     ELSE val -- sees tokenized data
--   END;
----------------------------------------------------------------------------------