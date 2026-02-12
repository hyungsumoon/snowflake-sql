use database security;
use schema network;
use role securityadmin;

CREATE SESSION POLICY if not exists hanatour_session_policy_30min_timeout
  SESSION_IDLE_TIMEOUT_MINS = 30           -- 일반 세션 유휴 시간: 30분
  SESSION_UI_IDLE_TIMEOUT_MINS = 30        -- 웹 UI 세션 유휴 시간: 30분
  ALLOWED_SECONDARY_ROLES = ( 'ALL' )      -- 모든 보조 역할 허용 (기본값)
  COMMENT = '하나투어 세션 정책 - 30분 후 자동 로그아웃'
;


CREATE SESSION POLICY if not exists hanatour_session_policy_no_timeout
  SESSION_IDLE_TIMEOUT_MINS = 720          -- 일반 세션 유휴 시간: 12시간 (최대값)
  SESSION_UI_IDLE_TIMEOUT_MINS = 720       -- 웹 UI 세션 유휴 시간: 12시간 (최대값)
  ALLOWED_SECONDARY_ROLES = ( 'ALL' )
  COMMENT = '하나투어 세션 정책 - 시스템 강제 로그아웃 방지 (최대 12시간 유지)'
;

--전체 어카운트 적용
alter account set session policy hanatour_session_policy_30min_timeout;


--사용자 레벨 적용
--사용자 레벨에 적용시 어카운트 정책대신 사용자 적용된 정책 우선 적용
alter user sf0001 set session policy hanatour_session_policy_no_timeout;
alter user sf0002 set session policy hanatour_session_policy_no_timeout;

SHOW SESSION POLICIES ON ACCOUNT;
SHOW SESSION POLICIES ON USER SF0001;

SELECT 1;