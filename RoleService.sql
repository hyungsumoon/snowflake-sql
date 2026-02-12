-- use role useradmin;
-- SHOW GRANTS TO ROLE rl_dbt_func_rw;
-----------------------------------------------------------------
-- service role에 부여하는 권한
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE rl_db_cdc_rw;
-- GRANT USAGE ON WAREHOUSE dbt_execute_wh TO ROLE rl_db_cdc_rw;
-- GRANT ROLE rl_db_cdc_rw TO ROLE rl_service_cdc;
-----------------------------------------------------------------
-- service role은 AP계정과 담당개발자에게 부여한다. 일반유저에게 부여하면 안됨.
-----------------------------------------------------------------
CREATE ROLE rl_service_cdc COMMENT = 'CDC Service 권한';
GRANT EXECUTE TASK ON ACCOUNT TO ROLE rl_service_cdc;
-- GRANT USAGE ON WAREHOUSE cdc_wh TO ROLE rl_service_cdc;
-- revoke USAGE ON WAREHOUSE cdc_wh from ROLE rl_service_cdc;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_service_cdc;
GRANT ROLE rl_db_stg_external_rw TO ROLE rl_service_cdc;
GRANT ROLE rl_db_stg_cdc_rw TO ROLE rl_service_cdc;
GRANT ROLE rl_db_cdc_rw TO ROLE rl_service_cdc;
-----------------------------------------------------------------
CREATE ROLE rl_service_dbt COMMENT = 'DBT Service 권한';
GRANT EXECUTE TASK ON ACCOUNT TO ROLE rl_service_dbt;
GRANT USAGE ON WAREHOUSE dbt_execute_wh TO ROLE rl_service_dbt;
-- revoke USAGE ON WAREHOUSE dbt_execute_wh from ROLE rl_service_dbt;
-- GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_service_dbt;
-- revoke USAGE ON WAREHOUSE compute_wh from ROLE rl_service_dbt;
GRANT ROLE rl_db_sys_dbt_rw TO ROLE rl_service_dbt;
GRANT ROLE rl_db_cdc_r TO ROLE rl_service_dbt;
GRANT ROLE rl_db_hanalog_r TO ROLE rl_service_dbt;
GRANT ROLE rl_db_sap_r TO ROLE rl_service_dbt;
GRANT ROLE rl_db_tms_r TO ROLE rl_service_dbt;
GRANT ROLE rl_db_int_dwdm_rw TO ROLE rl_service_dbt;
GRANT ROLE rl_db_dwdm_rw TO ROLE rl_service_dbt;
-----------------------------------------------------------------
CREATE ROLE rl_service_airflow COMMENT = 'Airflow Service 권한';
GRANT EXECUTE TASK ON ACCOUNT TO ROLE rl_service_airflow;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_service_airflow;
GRANT ROLE rl_db_sys_dbt_rw TO ROLE rl_service_airflow;
GRANT ROLE rl_db_cdc_r TO ROLE rl_service_airflow;
GRANT ROLE rl_db_hanalog_r TO ROLE rl_service_airflow;
GRANT ROLE rl_db_tms_r TO ROLE rl_service_airflow;
GRANT ROLE rl_db_int_dwdm_rw TO ROLE rl_service_airflow;
GRANT ROLE rl_db_dwdm_rw TO ROLE rl_service_airflow;

-- 테스트
-- GRANT ROLE rl_service_dbt TO ROLE rl_service_airflow;
-- revoke ROLE rl_service_dbt from ROLE rl_service_airflow;
