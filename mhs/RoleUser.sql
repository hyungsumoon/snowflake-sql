-- use role useradmin;
-- SHOW GRANTS TO ROLE rl_dbt_func_rw;
-----------------------------------------------------------------
-- user role에 부여하는 권한
-- GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_user_rw;
-- GRANT ROLE rl_db_cdc_rw TO ROLE rl_service_cdc;
-----------------------------------------------------------------
-- user role은 일반유저에게 부여한다.
-- 조회권한
--  dwdm : 1개를 전체 유저에게 주면 된다. // rl_db_dwdm_r
--  sandbox : 조직별 role 필요하다. // 조직을 sandbox db내 schema로 나눌거니까 schema별로 생성한다.
-----------------------------------------------------------------
CREATE ROLE rl_user_dwdm COMMENT = 'DWDM User 권한';
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE rl_user_dwdm;
GRANT ROLE rl_db_dwdm_r TO ROLE rl_user_dwdm;
-----------------------------------------------------------------
-- sandbox : 조직대로 해보자