show users;

use role accountadmin;
use role sysadmin;
use role useradmin;

SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.users a
WHERE 1=1
and deleted_on is null
-- and disabled = FALSE
and name like '%HI036%'
order by name 
;

desc user DBTAP01;


select  
        '' as tt
        -- concat('revoke role ', role, ' from user ', grantee_name, ';') as tt
        , a.*
from snowflake.account_usage.grants_to_users a
where 1=1
and deleted_on is null
-- and grantee_name = ''
and grantee_name like '%HI036%'
-- and grantee_name like '%DBT%'
-- and role like '%DBT%'
;
-------------------------------------------------------------
show roles;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.roles
WHERE 1=1
;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE 1=1
and deleted_on is null
and table_catalog = 'DWDM'
-- and table_catalog not in ('HANALOG','CDC','TMS','INT_DWDM')
and table_schema = 'INF01'
-- and name = 'IBCO_STATE_C'
and name like '%IBCO_HTL_CITY%'
-- and granted_on = 'WAREHOUSE'
-- and granted_on like '%DBT%'
-- and granted_on not in ('TABLE')
-- and grantee_name not in ('RL_ALL_R')
-- and grantee_name = 'RL_DBT_USER'
and grantee_name = 'RL_DBT_FUNC_RW'
-- and grantee_name like 'RL%DBT%USER%' --dbt_func_rw --> dbt_user // true
-- and grantee_name in ( 'DBTAP01','H2122' )
-- and grantee_name not in ( 'RL_DBT_FUNC_RW' )
-- and granted_by = 'RL_DBT_USER' --dbt_user --> hxxxx
-- and granted_to = 'ROLE'
-- and privilege = 'SELECT'
-- and privilege like '%CREATE%'
-- and grant_option = TRUE -- with grant option
;
-- SHOW GRANTS ON TABLE DWDM.INF01.IBCO_ETC_BSC_C;
GRANT OWNERSHIP ON TABLE DWDM.INF01.IBCO_HTL_CITY_LDMK_M_DUMP TO ROLE rl_dbt_func_rw;

SHOW GRANTS ON database SYS_DBT;
SHOW GRANTS ON SCHEMA SYS_DBT.YK;

-- grant create DBT PROJECT ON SCHEMA SYS_DBT.YK TO ROLE rl_dbt_func_rw WITH GRANT OPTION; 
revoke usage ON SCHEMA SYS_DBT.YK from user H2122;  
revoke usage ON database SYS_DBT from user H2122;

-- revoke usage ON DATABASE SYS_DBT from ROLE RL_AFW_FUNC_TEST;
-- revoke usage ON ALL SCHEMAS IN DATABASE SYS_DBT from ROLE RL_AFW_FUNC_TEST;
-- revoke all privileges ON ALL SCHEMAS IN DATABASE SYS_DBT from ROLE RL_AFW_FUNC_TEST;

show listings;
-------------------------------------------------------------
SELECT * FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES
WHERE 1=1
order by 1
;

SELECT  concat('grant ownership on schema ',catalog_name,'.',schema_name,' to role sysadmin revoke current grants;') as tt
        , catalog_name
        , schema_name
        , schema_owner
        , a.*
FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA a
WHERE 1=1
and deleted is null
and NOT REGEXP_LIKE(catalog_name, '.*(snowflake|mzc|poc|user).*', 'i')
-- and schema_name not in ('PUBLIC')
and schema_owner not in ('SYSADMIN')
ORDER BY 1,2;

use database DWDM;
use schema INF01;
SELECT  
        -- '' as tt
        concat('grant ownership on table ',table_catalog,'.',table_schema,'.',table_name,' to role sysadmin revoke current grants;') as tt
        -- , catalog_name
        -- , schema_name
        -- , schema_owner
        , a.*
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES a
WHERE 1=1
and deleted is null
and NOT REGEXP_LIKE(table_catalog, '.*(snowflake|mzc|poc|user).*', 'i')
-- and table_schema not in ('PUBLIC')
and table_owner not in ('SYSADMIN','ACCOUNTADMIN')
-- and table_name = 'IFPD_CROSS_PROD_RCMN_M_MHS'
ORDER BY 1,2;

-- table_owner : table 생성자의 role