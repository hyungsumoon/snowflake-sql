use role useradmin;
-------------------------------------------------------------------------------------------------------------
-- SHOW GRANTS ON SCHEMA database_a.schema_1;
SHOW GRANTS TO ROLE rl_dbt_func;
-- SHOW GRANTS TO USER user1;
-------------------------------------------------------------------------------------------------------------
CREATE ROLE rl_all_r COMMENT = '전체 조회 권한';

-- 전체 조회 권한도 db별로 조정할 수 있지만 안한다
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_all_r;
GRANT USAGE ON DATABASE dwdm TO ROLE rl_all_r;
GRANT USAGE ON ALL SCHEMAS IN DATABASE dwdm TO ROLE rl_all_r;
GRANT SELECT ON ALL TABLES IN DATABASE dwdm TO ROLE rl_all_r;
-- revoke SELECT ON ALL TABLES IN DATABASE dwdm from ROLE rl_all_r; --ownership 때문에 실행
-------------------------------------------------------------------------------------------------------------
-- Obect Access Role // sandbox.it 스키마 ddl 권한
CREATE ROLE rl_sb_it_ddl COMMENT = 'Sandbox DB > it본부 스키마 ddl 권한';

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_sb_it_ddl;
GRANT USAGE ON DATABASE sandbox TO ROLE rl_sb_it_ddl;
GRANT USAGE ON SCHEMA sandbox.it TO ROLE rl_sb_it_ddl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA sandbox.it TO ROLE rl_sb_it_ddl;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA sandbox.it TO ROLE rl_sb_it_ddl;

-- Functional Role // Organization Role // 조직별(직무별) 권한 // it본부 권한
CREATE ROLE rl_it_div COMMENT = 'IT본부 권한';

GRANT ROLE rl_all_r TO ROLE rl_it_div;
GRANT ROLE rl_sb_it_ddl TO ROLE rl_it_div;

use database sandbox;
GRANT ALL PRIVILEGES ON SCHEMA it TO rl_sb_it_ddl;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA it TO rl_sb_it_ddl;

-------------------------------------------------------------------------------------------------------------
CREATE ROLE rl_sb_customer_ddl COMMENT = 'Sandbox DB > 고객경험본부 스키마 ddl 권한';

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_sb_customer_ddl;
GRANT USAGE ON DATABASE sandbox TO ROLE rl_sb_customer_ddl;
GRANT USAGE ON SCHEMA sandbox.customer TO ROLE rl_sb_customer_ddl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA sandbox.customer TO ROLE rl_sb_customer_ddl;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA sandbox.customer TO ROLE rl_sb_customer_ddl;

-- Functional Role // Organization Role // 조직별(직무별) 권한 // it본부 권한
CREATE ROLE rl_customer_div COMMENT = '고객경험본부 권한';

GRANT ROLE rl_all_r TO ROLE rl_customer_div;
GRANT ROLE rl_sb_customer_ddl TO ROLE rl_customer_div;

-------------------------------------------------------------------------------------------------------
CREATE ROLE rl_dbt_func COMMENT = 'DBT Functional 권한';

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_dbt_func;

GRANT USAGE ON DATABASE HANALOG TO ROLE rl_dbt_func;
GRANT USAGE ON ALL SCHEMAS IN DATABASE HANALOG TO ROLE rl_dbt_func;
-- GRANT SELECT ON ALL SCHEMAS IN DATABASE HANALOG TO ROLE rl_dbt_func; --x
GRANT SELECT ON ALL TABLES IN DATABASE HANALOG TO ROLE rl_dbt_func;
GRANT SELECT ON FUTURE TABLES IN DATABASE HANALOG TO ROLE rl_dbt_func;

GRANT USAGE ON DATABASE TMS TO ROLE rl_dbt_func;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TMS TO ROLE rl_dbt_func;
GRANT SELECT ON ALL TABLES IN DATABASE TMS TO ROLE rl_dbt_func;
GRANT SELECT ON FUTURE TABLES IN DATABASE TMS TO ROLE rl_dbt_func;

GRANT USAGE ON DATABASE CDC TO ROLE rl_dbt_func;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CDC TO ROLE rl_dbt_func;
GRANT SELECT ON ALL TABLES IN DATABASE CDC TO ROLE rl_dbt_func;
GRANT SELECT ON FUTURE TABLES IN DATABASE CDC TO ROLE rl_dbt_func;

GRANT USAGE ON DATABASE DWDM TO ROLE rl_dbt_func;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DWDM TO ROLE rl_dbt_func;
GRANT SELECT ON ALL TABLES IN DATABASE DWDM TO ROLE rl_dbt_func;
GRANT SELECT ON FUTURE TABLES IN DATABASE DWDM TO ROLE rl_dbt_func;

GRANT USAGE ON DATABASE INT_DWDM TO ROLE rl_dbt_func;
GRANT USAGE ON ALL SCHEMAS IN DATABASE INT_DWDM TO ROLE rl_dbt_func;
GRANT SELECT ON ALL TABLES IN DATABASE INT_DWDM TO ROLE rl_dbt_func;
GRANT SELECT ON FUTURE TABLES IN DATABASE INT_DWDM TO ROLE rl_dbt_func;

GRANT USAGE ON DATABASE STG_EXTERNAL TO ROLE rl_dbt_func;
GRANT USAGE ON ALL SCHEMAS IN DATABASE STG_EXTERNAL TO ROLE rl_dbt_func;
GRANT SELECT ON ALL TABLES IN DATABASE STG_EXTERNAL TO ROLE rl_dbt_func;
GRANT SELECT ON FUTURE TABLES IN DATABASE STG_EXTERNAL TO ROLE rl_dbt_func;
-------------------------------------------------------------------------------------------------------
CREATE ROLE rl_dbt_user COMMENT = 'DBT user 권한';
GRANT ROLE rl_dbt_func TO ROLE rl_dbt_user;
GRANT ROLE rl_dbt_user TO USER H2640_IT;
-------------------------------------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE DBT_TEST TO ROLE rl_dbt_func; --x
GRANT ALL PRIVILEGES ON ALL DBT PROJECTS IN DATABASE DBT_TEST TO ROLE rl_dbt_func; --x //Unsupported feature 'GRANT on all objects of type DBT_PROJECT'.
GRANT ALL PRIVILEGES ON ALL DBT PROJECTS IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func; --x //Unsupported feature 'GRANT on all objects of type DBT_PROJECT'.
GRANT ALL PRIVILEGES ON DBT PROJECT IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func; --x //SQL compilation error: syntax error line 68 at position 36 unexpected 'IN'.
GRANT select ON "dbt Projects" IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func; --x //SQL compilation error: syntax error line 69 at position 31 unexpected 'IN'.
GRANT DBT PROJECT ON ALL SCHEMAS IN DATABASE DBT_TEST TO ROLE rl_dbt_func; --x // SQL compilation error: syntax error line 70 at position 6 unexpected 'DBT'. syntax error line 1 at position 72 unexpected ';'.

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DBT_TEST TO ROLE rl_dbt_func; --x
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DBT_TEST.DBT_TEST_SCHEMA_MHS TO ROLE rl_dbt_func;

GRANT SELECT ON ALL DBT PROJECTS DBT_TEST.DBT_TEST_SCHEMA_MHS TO ROLE rl_dbt_func;

-------------------------------------------------------------------------------
GRANT USAGE ON DATABASE DBT_TEST TO ROLE rl_dbt_func;
GRANT USAGE ON SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT CREATE TABLE       ON SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT CREATE VIEW        ON SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT CREATE STAGE       ON SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT CREATE FILE FORMAT ON SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT SELECT ON ALL VIEWS IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT USAGE ON ALL STAGES IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT USAGE ON FUTURE STAGES IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT USAGE ON DBT PROJECT DBT_TEST.DEPLOY.DBT_TEST TO ROLE rl_dbt_func; --o



SHOW GRANTS TO ROLE rl_dbt_func;
SHOW TABLES IN SCHEMA DBT_TEST.DEPLOY;
SHOW VIEWS  IN SCHEMA DBT_TEST.DEPLOY;

-------------------------------------------------------------------------------
-- test

GRANT USAGE ON SCHEMA DBT_TEST.DBT TO ROLE rl_dbt_func;
GRANT USAGE ON SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;

GRANT USAGE ON ALL TABLES IN DATABASE DBT_TEST TO ROLE rl_dbt_func; --x
GRANT select ON ALL TABLES IN DATABASE DBT_TEST TO ROLE rl_dbt_func; --?

GRANT ALL PRIVILEGES ON SCHEMA DBT_TEST.DBT TO ROLE rl_dbt_func;
GRANT ALL PRIVILEGES ON SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE DBT_TEST TO ROLE rl_dbt_func; --?
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DBT_TEST.DEPLOY TO ROLE rl_dbt_func; --?

-- use database DBT_TEST;
-- create schema DBT_TEST_SCHEMA_MHS; 
create table mhs_table1 (col1 varchar(10), col2 number(3));


GRANT USAGE ON WAREHOUSE compute_wh TO USER AFWAP01;
GRANT USAGE ON DATABASE DWDM TO USER AFWAP01;
GRANT USAGE ON SCHEMA DWDM.INF01 TO USER AFWAP01;

-------------------------------------------------------------------------------
-- Functional Role // Organization Role // 조직별(직무별) 권한 // it본부 권한
CREATE ROLE rl_dbt_user COMMENT = 'DBT 사용자 권한';

GRANT ROLE rl_dbt_func TO ROLE rl_dbt_user;
GRANT ROLE rl_dbt_user TO USER AFWAP01;
GRANT ROLE rl_dbt_user TO USER AFWAP02;
GRANT ROLE rl_dbt_user TO USER AFWAP03;

