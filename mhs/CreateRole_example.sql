-- 사용자 관리자(USERADMIN 역할이 있는 사용자) 또는 계정에 대한 CREATE ROLE 권한이 있는 다른 역할로서 이 예에서 액세스 역할과 기능 역할을 만듭니다.

CREATE ROLE db_hr_r;
CREATE ROLE db_fin_r;
CREATE ROLE db_fin_rw;
CREATE ROLE accountant;
CREATE ROLE analyst;

-- 보안 관리자(SECURITYADMIN 역할이 있는 사용자) 또는 계정에 대한 MANAGE GRANTS 권한이 있는 다른 역할로서 각 액세스 역할에 필요한 최소한의 권한을 부여합니다.

-- Grant read-only permissions on database HR to db_hr_r role.
GRANT USAGE ON DATABASE hr TO ROLE db_hr_r;
GRANT USAGE ON ALL SCHEMAS IN DATABASE hr TO ROLE db_hr_r;
GRANT SELECT ON ALL TABLES IN DATABASE hr TO ROLE db_hr_r;

-- Grant read-only permissions on database FIN to db_fin_r role.
GRANT USAGE ON DATABASE fin TO ROLE db_fin_r;
GRANT USAGE ON ALL SCHEMAS IN DATABASE fin TO ROLE db_fin_r;
GRANT SELECT ON ALL TABLES IN DATABASE fin TO ROLE db_fin_r;

-- Grant read-write permissions on database FIN to db_fin_rw role.
GRANT USAGE ON DATABASE fin TO ROLE db_fin_rw;
GRANT USAGE ON ALL SCHEMAS IN DATABASE fin TO ROLE db_fin_rw;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN DATABASE fin TO ROLE db_fin_rw;

-- 보안 관리자(SECURITYADMIN 역할이 있는 사용자) 또는 계정에 대한 MANAGE GRANTS 권한이 있는 다른 역할로서 기능 역할 db_fin_rw에 액세스 역할을 부여하고 accountant, 기능 역할 db_hr_r db_fin_r에 액세스 역할을 부여합니다 analyst .

GRANT ROLE db_fin_rw TO ROLE accountant;
GRANT ROLE db_hr_r TO ROLE analyst;
GRANT ROLE db_fin_r TO ROLE analyst;
-- 보안 관리자(SECURITYADMIN 역할이 있는 사용자) 또는 계정에 대한 MANAGE GRANTS 권한이 있는 다른 역할인 경우 시스템 관리자(SYSADMIN) 역할에 analyst및 역할을 모두 부여합니다.accountant

GRANT ROLE accountant,analyst TO ROLE sysadmin;

-- 보안 관리자(SECURITYADMIN 역할이 있는 사용자) 또는 계정에 대해 MANAGE GRANTS 권한이 있는 다른 역할의 사용자로서, 조직에서 해당 비즈니스 기능을 수행하는 사용자에게 비즈니스 기능 역할을 부여합니다. 이 예에서는 analyst 기능 역할이 user 에게 부여되고 user1, accountant기능 역할이 user 에게 부여됩니다 user2.

GRANT ROLE accountant TO USER user1;
GRANT ROLE analyst TO USER user2;
---------------------------------------------
-- Grant the SELECT privilege on all new (future) tables in a schema to role R1
GRANT SELECT ON FUTURE TABLES IN SCHEMA s1 TO ROLE r1;

-- / Create tables in the schema /

-- Grant the SELECT privilege on all new tables in a schema to role R2
GRANT SELECT ON FUTURE TABLES IN SCHEMA s1 TO ROLE r2;

-- Grant the SELECT privilege on all existing tables in a schema to role R2
GRANT SELECT ON ALL TABLES IN SCHEMA s1 TO ROLE r2;

-- Revoke the SELECT privilege on all new tables in a schema (future grant) from role R1
REVOKE SELECT ON FUTURE TABLES IN SCHEMA s1 FROM ROLE r1;

-- Revoke the SELECT privilege on all existing tables in a schema from role R1
REVOKE SELECT ON ALL TABLES IN SCHEMA s1 FROM ROLE r1;
---------------------------------------------
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES;
  WHERE granted_to = 'USER';

SHOW GRANTS TO USER <user_name>
  ->> SELECT * FROM $1 WHERE "role" IS NULL;

SHOW GRANTS ON SCHEMA database_a.schema_1;
SHOW GRANTS TO ROLE r1;
SHOW GRANTS TO USER user1;
---------------------------------------------
GRANT ROLE ACCOUNTADMIN, SYSADMIN TO USER user2;

ALTER USER user2 SET EMAIL='user2@domain.com', DEFAULT_ROLE=SYSADMIN;
---------------------------------------------
CREATE ROLE r1
   COMMENT = 'This role has all privileges on schema_1';

GRANT USAGE ON WAREHOUSE w1 TO ROLE r1;
GRANT USAGE ON DATABASE d1 TO ROLE r1;
GRANT USAGE ON SCHEMA d1.s1 TO ROLE r1;
GRANT SELECT ON TABLE d1.s1.t1 TO ROLE r1;

GRANT ROLE r1 TO USER smith;
ALTER USER smith SET DEFAULT_ROLE = r1;

GRANT USAGE ON DATABASE d1 TO ROLE read_only;

GRANT USAGE ON SCHEMA d1.s1 TO ROLE read_only;

GRANT SELECT ON ALL TABLES IN SCHEMA d1.s1 TO ROLE read_only;

GRANT USAGE ON WAREHOUSE w1 TO ROLE read_only;
GRANT SELECT ON FUTURE TABLES IN SCHEMA d1.s1 TO ROLE read_only;

GRANT ROLE r1 TO ROLE sysadmin;

GRANT USAGE ON WAREHOUSE w1 TO USER user1;
GRANT USAGE ON DATABASE d1 TO USER user1;
GRANT USAGE ON SCHEMA d1.s1 TO USER user1;
GRANT USAGE ON STREAMLIT `streamlitApp1` TO USER user1;

ALTER ACCOUNT SET DISABLE_USER_PRIVILEGE_GRANTS = TRUE;
GRANT SELECT ON FUTURE TABLES IN DATABASE d1 TO ROLE r1;
GRANT INSERT,DELETE ON FUTURE TABLES IN SCHEMA d1.s1 TO ROLE r2;
---------------------------------------------
GRANT MONITOR USAGE ON ACCOUNT TO ROLE custom;
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE custom;