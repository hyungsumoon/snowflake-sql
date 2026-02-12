
-- 개인정보
-- pcobalt
select  KO_NM, a.*
from INF01.IWCS_MEM_M a
where 1=1 and rownum <= 10
and KO_NM is not null
;
-------------------------------------------------------------------------------------------------
-- 깨진 문자값
-- pcobalt
select * from INF01.IWAR_AIR_CUPN_PAY_X where 1=1 and rownum <= 10;

--snowflake
select * from CDC.AIR01.DCD_AIR_CUPN_PAY_X where seq = 62778 limit 10; -- pneon부터 깨진 문자값
-------------------------------------------------------------------------------------------------
select * from dwdm.INF01.IBCU_CUST_M limit 10; -- 아직 데이터 없음
select * from CDC.CST01.CST_MEM_M limit 10;
-- KO_NM, GNDR_DV_CD, BTHD, MLG_NUM, WDNG_YN, FRNR_YN
-- CUST_NUM, PSPT_ISNC_DT, INPR_ID, UPDR_ID