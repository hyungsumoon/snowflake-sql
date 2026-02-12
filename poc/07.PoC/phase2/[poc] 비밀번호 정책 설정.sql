use database security;
use schema network;

use role securityadmin;
GRANT USAGE ON DATABASE security TO ROLE securityadmin;
GRANT USAGE ON SCHEMA security.network TO ROLE securityadmin;
GRANT CREATE PASSWORD POLICY ON SCHEMA security.network TO ROLE securityadmin;
GRANT CREATE SESSION POLICY ON SCHEMA security.network TO ROLE securityadmin;

--비밀번호 설정 정책 세팅
CREATE PASSWORD POLICY if not exists hanatour_password_policy
  PASSWORD_MIN_LENGTH = 8            -- 최소 글자 수 
  PASSWORD_MAX_LENGTH = 30           -- 최대 글자 수 max 256
  PASSWORD_MIN_UPPER_CASE_CHARS = 1  -- 대문자 최소 1개
  PASSWORD_MIN_LOWER_CASE_CHARS = 1  -- 소문자 최소 1개
  PASSWORD_MIN_NUMERIC_CHARS = 1     -- 숫자 최소 1개
  PASSWORD_MIN_SPECIAL_CHARS = 0     -- 특수문자는 최소 0개로 설정하여,
  PASSWORD_MIN_AGE_DAYS = 1          -- 초기/임시 비밀번호 즉시 변경(9번 조건) 및 비밀번호 변경 후 재사용 방지를 위해 최소 1일로 설정
  PASSWORD_MAX_AGE_DAYS = 90         -- 주기적인 비밀번호 변경을 위해 90일로 예시 설정
  PASSWORD_HISTORY = 5               -- 이미 사용된 비밀번호 재사용 방지 (5번 조건) / 최근 5개의 비밀번호는 재사용 불가
  PASSWORD_MAX_RETRIES = 5           -- 비밀번호 실패 횟수 제한 (보안 강화)
  -- PASSWORD_LOCKOUT_TIME_MINS = 600        -- 잠금 시간 600초 (10분)
  PASSWORD_LOCKOUT_TIME_MINS = 10        -- 비밀번호 재시도까지 잠금 시간 (분)
  COMMENT = '하나투어 비밀번호 정책';

--비밀번호 정책 적용
ALTER ACCOUNT SET PASSWORD POLICY SECURITY.NETWORK.hanatour_password_policy;

USE WAREHOUSE COMPUTE_WH;
SELECT * from snowflake.account_usage.password_policies;

--비밀번호 정책 상세정보
describe password policy SECURITY.NETWORK.hanatour_password_policy;

--사용자에게 비밀번호 정책 적용
ALTER USER SF0001 SET PASSWORD POLICY SECURITY.NETWORK.hanatour_password_policy;

SHOW PASSWORD POLICIES ON ACCOUNT;
SHOW PASSWORD POLICIES ON USER SF0001;

SELECT 1;

