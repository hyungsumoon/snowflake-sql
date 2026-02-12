-- 테이블 > 스키마 > DB 순서로 진행
-- GRANT OWNERSHIP ON TABLE DWDM.inf01.IBCO_STATE_C TO rl_db_dwdm_rw COPY CURRENT GRANTS; --ok
-- GRANT OWNERSHIP ON SCHEMA DWDM.inf01 TO rl_db_dwdm_rw COPY CURRENT GRANTS; --ok
-- GRANT OWNERSHIP ON DATABASE DWDM TO rl_db_dwdm_rw COPY CURRENT GRANTS; --ok
-- GRANT ROLE rl_db_dwdm_rw TO ROLE rl_dbt_func_rw;
-------------------------------------------------------------------------------
-- 일괄변경조회
-- DB : 작업완료
SELECT  '' as tt
        -- , concat('GRANT ROLE rl_db_',database_name,'_rw TO ROLE rl_dbt_func_rw;') as t1
        -- , concat('GRANT OWNERSHIP ON DATABASE ',database_name,' TO ROLE rl_db_',database_name,'_rw COPY CURRENT GRANTS;') as t2
        , a.*
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES a
where 1=1
and deleted is null
and type = 'STANDARD'
and database_name not in ('DBT_TEST','POC','SANDBOX','SECURITY','SNOWPIPE')
-- and database_owner in ('ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN')
order by database_name
;

-- 일단은 sysadmin으로 해두자
SELECT  '' as tt
        -- , concat('GRANT ROLE rl_db_',database_name,'_rw TO ROLE rl_dbt_func_rw;') as t1
        , concat('GRANT OWNERSHIP ON DATABASE ',database_name,' TO ROLE sysadmin COPY CURRENT GRANTS;') as t2
        , a.*
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES a
where 1=1
and deleted is null
and type = 'STANDARD'
and database_name not in ('DBT_TEST','POC','SANDBOX','SECURITY','SNOWPIPE')
-- and database_owner in ('ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN')
and database_owner <> 'SYSADMIN'
order by database_name
;
-------------------------------------------------------------------------------
-- SCHEMA
SELECT  '' as tt
        , concat('GRANT OWNERSHIP ON SCHEMA ',catalog_name,' TO ROLE rl_db_',catalog_name,'_rw COPY CURRENT GRANTS;') as t2
        , a.*
FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA a
WHERE 1=1
and deleted is null
and schema_type = 'STANDARD'
and catalog_name not in ('DBT_TEST','POC','SANDBOX','SECURITY','SNOWPIPE')
and schema_owner in ('ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN')
ORDER BY schema_name
;

