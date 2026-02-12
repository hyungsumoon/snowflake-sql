use role accountadmin;
use warehouse compute_wh;

alter account set timezone = 'Asia/Seoul';

--이상 탐지
-- 해외 IP에서의 로그인 시도 (일반적이지 않은 국가의 IP 대역)
select
    user_name,
    client_ip,
    event_timestamp
from snowflake.account_usage.login_history
where event_timestamp >= dateadd(day, -7, current_timestamp())
    and client_ip not in ('112.223.61.18',
'112.223.61.19',
'112.220.71.244',
'112.220.71.243',
'183.111.168.11')
order by event_timestamp desc;

-- 업무시간 외 로그인 (주말 또는 야간 시간대)
SELECT
    event_timestamp,
    user_name,
    client_ip,
    EXTRACT(HOUR FROM event_timestamp) as login_hour,
    EXTRACT(DOW FROM event_timestamp) as day_of_week
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY)
WHERE event_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    AND (
        EXTRACT(HOUR FROM event_timestamp) NOT BETWEEN 9 AND 18  -- 업무시간 외
        OR EXTRACT(DOW FROM event_timestamp) IN (0, 6)  -- 주말 (일요일=0, 토요일=6)
    )
    AND is_success = 'YES'
    AND user_name NOT IN ('WORKSHEETS_APP_USER')
ORDER BY event_timestamp DESC;


-- 단기간 내 다중 로그인 실패 (10분내 3회 이상 실패)
WITH failed_logins AS (
    SELECT
        user_name,
        client_ip,
        event_timestamp,
        datediff(minute, min(event_timestamp), max(event_timestamp)) as time_span,
        count(*) as failure_count
    FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
    WHERE event_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        AND is_success = 'NO'
        AND user_name NOT IN ('WORKSHEETS_APP_USER')
    group by user_name, client_ip, event_timestamp
)
SELECT *
FROM failed_logins
WHERE failure_count >= 3 and time_span <= 10
ORDER BY event_timestamp DESC;


--알람 생성 방법
--1. email notification 생성
create notification integration email_integration
    type=email
    enabled=true
    comment = '이메일 기반 보안 알림';

USE DATABASE SECURITY;
USE SCHEMA NETWORK;
create alert failed_login_attempts_alert
    warehouse = compute_wh
    schedule = '60 minute'
if (exists(
    -- 해외 IP에서의 로그인 시도 (일반적이지 않은 국가의 IP 대역)
    select
        user_name,
        client_ip,
        event_timestamp
    from snowflake.account_usage.login_history
    where event_timestamp >= dateadd(day, -7, current_timestamp())
        and client_ip not in ('112.223.61.18',
    '112.223.61.19',
    '112.220.71.244',
    '112.220.71.243',
    '183.111.168.11')
    order by event_timestamp desc
))
then call system$send_email('kyusang.lee@snowflake.com', 'email_notification', '스노우플레이크 하나투어 해외 및 비인가 ip 접속 시도 알림');