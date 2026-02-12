/*
-- ========================================
-- 1. Masking Policy 생성
-- ========================================

--1.1. 이름 마스킹 (첫 글자만 표시)
CREATE OR REPLACE MASKING POLICY mask_name AS 
(val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    WHEN val IS NULL THEN NULL
    ELSE CONCAT(LEFT(val, 1), '***')
  END;

-- 1.2. 전체 마스킹 (전부 ***로 표시)
CREATE OR REPLACE MASKING POLICY mask_full AS 
(val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    WHEN val IS NULL THEN NULL
    ELSE '***'
  END;

-- 1.3. 이메일 마스킹 (일부만 표시)
CREATE OR REPLACE MASKING POLICY mask_email AS 
(val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    WHEN val IS NULL THEN NULL
    ELSE CONCAT(LEFT(val, 3), '***@***.com')
  END;

-- 1.4. 전화번호 마스킹 (끝 4자리만 표시)
CREATE OR REPLACE MASKING POLICY mask_phone AS 
(val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    WHEN val IS NULL THEN NULL
    ELSE CONCAT('***-****-', RIGHT(val, 4))
  END;

-- 1.5. 부분 마스킹 (가운데 부분만 마스킹)
CREATE OR REPLACE MASKING POLICY mask_partial AS 
(val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    WHEN val IS NULL THEN NULL
    WHEN LENGTH(val) <= 2 THEN REPEAT('*', LENGTH(val))
    ELSE CONCAT(LEFT(val, 1), REPEAT('*', LENGTH(val) - 2), RIGHT(val, 1))
  END;

-- 1.6. 숫자 마스킹 (0으로 표시)
CREATE OR REPLACE MASKING POLICY mask_number AS 
(val NUMBER) RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    ELSE 0
  END;

-- 1.7. 날짜 마스킹 (연도만 표시)
CREATE OR REPLACE MASKING POLICY mask_date AS 
(val TIMESTAMP_NTZ) RETURNS TIMESTAMP_NTZ ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    WHEN val IS NULL THEN NULL
    ELSE DATE_TRUNC('YEAR', val)
  END;

-- ========================================
-- 2. 생성된 Masking Policy 확인
-- ========================================
SHOW MASKING POLICIES IN SCHEMA <데이터베이스 이름>.<스키마이름>;
*/