-- drop user CDCAP01;
CREATE USER CDCAP01
  PASSWORD = 'HanaTour123!@#'
  LOGIN_NAME = 'CDCAP01'
  DISPLAY_NAME = 'CDC AP 계정'
  EMAIL = 'kdy6379@hanatour.com'
  MUST_CHANGE_PASSWORD = FALSE
  TYPE = 'PERSON';

-- alter user CDCAP01 set password = 'HanaTour123!@#';
-- alter user CDCAP01 set disabled = false;
-- alter user CDCAP01 set mins_to_unlock = 0;

alter user CDCAP01 set DEFAULT_ROLE = rl_cdc_user;
ALTER USER CDCAP01 SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0bCvaojj7wBOu3jfJsAN
L+W0PEm1Z3jCv5VgrLqLUYU3HQlXffYGtuP2Xx2HyZTXBBVz44A85CC9A4gJvdRF
H2mH5nKyKXdXyi99p/WfcdWVIBy22rpamzNU+enbrj1XgF2xI/NiCqKaba6EUj24
CegUNOqufb/jNiQIpwINEGdcBnuMim4kFI5ITVnbZMEGQnF0P2pRNvpAkT+Jsuak
gr+Go2ORFK6MzW1xqBRWmXJL3Dr2fTrdETQlX3Tt5+ed/UWKdq4hBupybkWMzM1t
mXyiNL/PX2fbiXU1gRWZ9Y9oe4lWk+eg7/5STFDSi9IQbEseGCfErWQArMIZUBn+
kQIDAQAB';

SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER CDCAP01;
desc user CDCAP01;

GRANT ROLE rl_cdc_user TO USER CDCAP01;
--------------------------------------------
CREATE USER CDCAP02
  PASSWORD = 'HanaTour123!@#'
  LOGIN_NAME = 'CDCAP02'
  DISPLAY_NAME = 'CDC AP 계정'
  EMAIL = 'kdy6379@hanatour.com'
  MUST_CHANGE_PASSWORD = FALSE
  TYPE = 'PERSON'
  DEFAULT_ROLE = rl_service_cdc
  COMMENT = 'CDC AP 계정 - PERSON';

ALTER USER CDCAP02 SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7kcqCxWcP7z2OaYjIihy
OjiLW1Ixvy5GQXS6+WTOcdxj5CClAqkAgXZd525hoRqOVzEOpzEBPehD8l/LN/EC
FhtnvE9CFfJFg46a5GbJHHyJ4kqpwtMB0oFPFizOmMQC1A0w4inhx9ESeqPvvdWs
hDCBd3pGEIEag5lv3zCHz9a6bDhNNaqzyMxuSu5QwBATyz81bWWK1Xy/1bBWmxUH
9QU7bSN9zbU06ezzK/A1ujcI0UVSBsZQ15B4t7qwW/yOuilm3Ctmnkc8APZZS0ye
h5kw9jZH0amcwAw3VCPFt+p7a+atefyDaXUUOJLb8ZblcOSf7ZcpdkRDvCYkazJY
gwIDAQAB';
  
-- ALTER USER CDCAP02 SET DISPLAY_NAME = 'DBT AP 계정';

GRANT ROLE rl_service_cdc TO USER CDCAP02;
-- revoke role rl_dbt_user from USER DBTAP02;
-----------------------------------------------------------------------------------------------------
CREATE USER DBTAP01
  PASSWORD = 'HanaTour123!@#'
  LOGIN_NAME = 'DBTAP01'
  DISPLAY_NAME = 'DBTAP01'
  EMAIL = 'itsme@hanatour.com'
  MUST_CHANGE_PASSWORD = FALSE
  TYPE = 'PERSON'
  DEFAULT_ROLE = rl_dbt_user
  COMMENT = 'DBT AP 계정';

ALTER USER DBTAP01 SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyDpoNEKQS5+3F0Ts01EQ
A3bfDTfelo6Y1hrh8iYRUj5yJ1KlAFQcXT388WgEmI25/05tO+E+WmLORr+QOcJJ
iw1BJn9qAXXALMclwoFV/SIS0r9d+ixBCjNIKt7+/cQFxqbyJJaVRbHZReoRM1S/
72t0dbXnfQiAZHAh4g9TZkl121pldzmu9qd7jDHevVcgOHopZu3EUla1Y93iYTnl
xHRQXENcS5oCGbclZN8IUIhPrXcg1aBBk35Adc2wlsOz7WegEp9FuhmR3gQgQCuM
H9FKd1IXTlcy1y35vHjC0WUpwCqfii4xqK4jPQ93ZEj9mbzGTw7GFdSDXWYRlrEn
FwIDAQAB';

GRANT ROLE rl_dbt_user TO USER DBTAP01;
-- revoke role rl_dbt_user from USER DBTAP01;

desc user DBTAP01;
-- -----BEGIN PUBLIC KEY-----
-- MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyDpoNEKQS5+3F0Ts01EQ
-- A3bfDTfelo6Y1hrh8iYRUj5yJ1KlAFQcXT388WgEmI25/05tO+E+WmLORr+QOcJJ
-- iw1BJn9qAXXALMclwoFV/SIS0r9d+ixBCjNIKt7+/cQFxqbyJJaVRbHZReoRM1S/
-- 72t0dbXnfQiAZHAh4g9TZkl121pldzmu9qd7jDHevVcgOHopZu3EUla1Y93iYTnl
-- xHRQXENcS5oCGbclZN8IUIhPrXcg1aBBk35Adc2wlsOz7WegEp9FuhmR3gQgQCuM
-- H9FKd1IXTlcy1y35vHjC0WUpwCqfii4xqK4jPQ93ZEj9mbzGTw7GFdSDXWYRlrEn
-- FwIDAQAB
-- -----END PUBLIC KEY-----
--------------------------------------------
CREATE USER DBTAP02
  PASSWORD = 'HanaTour123!@#'
  LOGIN_NAME = 'DBTAP02'
  DISPLAY_NAME = 'DBT AP 계정'
  EMAIL = 'shb413@hanatour.com'
  MUST_CHANGE_PASSWORD = FALSE
  TYPE = 'PERSON'
  DEFAULT_ROLE = rl_service_dbt
  DEFAULT_WAREHOUSE = dbt_execute_wh
  COMMENT = 'DBT AP 계정';

ALTER USER DBTAP02 SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuAJMze0huw/9hiEAFiDG
Rj6A6g8ZCqDgyxQWAe+Eclo+l6FX6W2EZApHMwqCxVIqTUk9Pq+5S6TGohYBZ8Tw
J2m6ZT8s1ANI09OqzfoTUsNouX9PIs8SxnzQFd0MTHdL1O4dFYUbBesD0kfAFvyO
66R6/96jf2VM/JsEuwiQhykUA1qKe1sjTlD/QXNi6+d4DPoM+1vMfk9MBglBy8kz
D8M3eNwcByl7/+M4Y56stVgm9x/FIaJ7O2ELxGB4y2saqDAKbXFKpBlKfL0Ji0OX
RRjpxUGK56iNungZkPXVX8QcgdLZjb3wRF0hoJLpcRXibXwxd0WXtB3fMI9/kpOz
DQIDAQAB';

-- ALTER USER DBTAP02 SET DISPLAY_NAME = 'DBT AP 계정';
-- ALTER USER DBTAP02 SET DEFAULT_WAREHOUSE = DBT_EXECUTE_WH;

GRANT ROLE rl_service_dbt TO USER DBTAP02;
-- revoke role rl_dbt_user from USER DBTAP02;

desc user DBTAP02;
-----------------------------------------------------------------------------------------------------
CREATE USER AFWAP03
  PASSWORD = 'HanaTour123!@#'
  LOGIN_NAME = 'AFWAP03'
  DISPLAY_NAME = 'Airflow AP 계정'
  EMAIL = 'itsme@hanatour.com'
  MUST_CHANGE_PASSWORD = FALSE
  TYPE = 'PERSON';

-- alter user AFWAP02 set password = 'HanaTour123!@#';
-- alter user AFWAP02 set disabled = false;
-- alter user AFWAP02 set mins_to_unlock = 0;

-- desc user AFWAP02;
-- show password policies;
show parameters;
show parameters like '%AGE%';
show parameters like '%PASSWORD%' IN ACCOUNT;

-- GRANT USAGE ON WAREHOUSE compute_wh TO USER AFWAP03;
-- GRANT USAGE ON DATABASE DWDM TO USER AFWAP03;
-- GRANT USAGE ON SCHEMA DWDM.INF01 TO USER AFWAP03;
-- GRANT USAGE ON DATABASE CDC TO USER AFWAP03;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE CDC TO USER AFWAP03;
-- GRANT USAGE ON DATABASE DBT_TEST TO USER AFWAP03;
-- GRANT USAGE ON ALL SCHEMAS IN DATABASE DBT_TEST TO USER AFWAP03;
-- GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE DBT_TEST TO USER AFWAP03; --x // role로 처리

-- revoke USAGE ON WAREHOUSE compute_wh from USER AFWAP03;
-- revoke USAGE ON DATABASE DWDM from USER AFWAP03;
-- revoke USAGE ON SCHEMA DWDM.INF01 from USER AFWAP03;
-- revoke USAGE ON DATABASE CDC from USER AFWAP03;
-- revoke USAGE ON ALL SCHEMAS IN DATABASE CDC from USER AFWAP03;
-- revoke USAGE ON DATABASE DBT_TEST from USER AFWAP03;
-- revoke USAGE ON ALL SCHEMAS IN DATABASE DBT_TEST from USER AFWAP03;


-- 접속 테스트를 위한 임시 설정
GRANT ROLE useradmin TO USER AFWAP03;
GRANT ROLE sysadmin TO USER AFWAP03;
-- revoke ROLE sysadmin from USER AFWAP03;
alter user AFWAP03 set DEFAULT_ROLE = rl_dbt_user;
--------------------------------------------
-- drop user AFWAP02;
CREATE USER AFWAP02
  PASSWORD = 'HanaTour123!@#'
  LOGIN_NAME = 'AFWAP02'
  DISPLAY_NAME = 'Airflow AP 계정'
  EMAIL = 'itsme@hanatour.com'
  MUST_CHANGE_PASSWORD = FALSE
  TYPE = 'PERSON'
  DEFAULT_ROLE = rl_service_airflow
  DEFAULT_WAREHOUSE = compute_wh
  COMMENT = 'Airflow AP 계정';
  
ALTER USER AFWAP02 SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvAnJ/xy5Cx6JyMRUJem8
Qagj7cZyeoqcQy2+2S8OaZ6YGFylkGdYJhEvI5jcK24op7FlnK+hQMgYVzuzCyRR
pxjp/OwNSffJbk0NLrwmikj8cyJDTVxSZrmKDyBvzg0CpJ0+qIUllptzL4vjC5FL
L7n8t5/313M7Hz/Jr6+TRnj+9VYg7xOT7kdCqE3GOWkpuWo51Qmll9fOOQBXk1N+
i2Ve49odWuzaridNou6cr+0Dcn3jV7Is8nOZN0Ul3pxv5qIU8awUr0YJW6wPFQzS
icVz9+lLTOf3eVa8eIJXzUkdeMHda8WcDtoQ8fnUcN8nGEZerYX/TZ+cxJV/RSYi
3wIDAQAB';

-- ALTER USER AFWAP02 SET DEFAULT_ROLE = rl_service_airflow;
-- ALTER USER AFWAP02 SET DEFAULT_WAREHOUSE = dbt_execute_wh;

GRANT ROLE rl_service_airflow TO USER AFWAP02;
-- revoke role rl_dbt_user from USER DBTAP02;

-----------------------------------------------------------------------------------------------------
-- alter user AFWAP02 SET DEFAULT_WAREHOUSE = 'COMPUTE_WH'; -- 테스트중
-- alter user AFWAP02 set password = 'HanaTour123!@#';
-- ALTER USER DBTAP02 SET DISPLAY_NAME = 'DBT AP 계정';
-- alter user AFWAP02 set disabled = false;
-- alter user AFWAP02 set mins_to_unlock = 0;

-- SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER AFWAP01;
-- desc user AFWAP02;
-- show password policies;
