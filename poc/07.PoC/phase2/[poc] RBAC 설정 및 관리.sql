use role securityadmin;

--데이터베이스 & 웨어하우스 (compute engine) 생성 권한 제거
revoke create database on account from role public;
revoke create warehouse on account from role public;

--public 역할이 가진 권한
show grants to role public;


show roles;
--데이터플랫폼 팀 ROLE 생성
create role dataplatform_team;
grant role accountadmin to role dataplatform_team;
grant role securityadmin to role dataplatform_team;
grant role orgadmin to role dataplatform_team;
grant role sysadmin to role dataplatform_team;
grant role useradmin to role dataplatform_team;


grant role dataplatform_team to user sf0001;

