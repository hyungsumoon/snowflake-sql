use database stg_cdc;
use warehouse cdc_wh;


show procedures;

/*

    스키마별 테이블 1:1 관계로 MERGE PROCEDURE 생성하는 PROCEDURE 임

    STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_PKG01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HUB01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HTSOT01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HTSML01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HTSIN01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HTSBR01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HTSAC01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HTL01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HGSIN01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_CST01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_COM01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_CHL01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_AIR01_ALL_MERGE_PROCS
    STG_CDC.PUBLIC.CREATE_HCS01_ALL_MERGE_PROCS

    데이터 MERGE PROCEDURE : CDC.PUBLIC.CDC_CREATE_MERGE_PROC_SCRIPT
        >> 특정 테이블 하나에 대한 MERGE PROCEDURE 를 생성하는 PROCEDURE
        >> 사용 예시
            CALL CDC.PUBLIC.CDC_CREATE_MERGE_PROC_SCRIPT('CDC.AIR01.SAC_AIV_CLAS_C');

    위 PROCEDURE 를 한번만 수행하는 TASK 생성해서 진행 권장 (병렬 작업을 위함)
        >> TASK 생성 예시
            
            create or replace task STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS
            warehouse=COMPUTE_WH
            as 
            call STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS();

            execute task STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS;

*/


/*
    1회용 TASK 생성, 수행, 확인
*/

create or replace task STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS();
            
create or replace task STG_CDC.PUBLIC.CREATE_PKG01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_PKG01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HUB01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HUB01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HTSOT01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HTSOT01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HTSML01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HTSML01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HTSIN01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HTSIN01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HTSBR01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HTSBR01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HTSAC01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HTSAC01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HTL01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HTL01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HGSIN01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HGSIN01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_CST01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_CST01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_COM01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_COM01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_CHL01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_CHL01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_AIR01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_AIR01_ALL_MERGE_PROCS();

create or replace task STG_CDC.PUBLIC.CREATE_HCS01_ALL_MERGE_PROCS
warehouse=COMPUTE_WH
as 
call STG_CDC.PUBLIC.CREATE_HCS01_ALL_MERGE_PROCS();


show tasks;


execute task STG_CDC.PUBLIC.CREATE_AIR01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_CHL01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_COM01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_CST01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HCS01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HGSIN01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HTL01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HTSAC01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HTSBR01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HTSIN01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HTSML01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HTSOT01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_HUB01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_PKG01_ALL_MERGE_PROCS;
execute task STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS;



select query_id, name, state, error_code, error_message, scheduled_time, query_start_time, completed_time, next_scheduled_time, query_text  
from table(information_schema.task_history())
where name like '%ALL_MERGE_PROCS%'
order by name;


select * from information_schema.procedures ;

select get_ddl('procedure','STG_CDC.HCS01.UC_SYS_USER()');



/*
    데이터 MERGE PROCEDURE 수행하는 PROCEDURE DDL 문
*/

CREATE OR REPLACE PROCEDURE STG_CDC.PUBLIC.CREATE_STT01_ALL_MERGE_PROCS()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var tbl_list = [
'CDC.STT01.SET_TKT_SETT_X',
'CDC.STT01.SET_NSTG_SETT_M',
'CDC.STT01.SET_PROD_SETT_M',
'CDC.STT01.SET_AIR_APRV_D',
'CDC.STT01.SET_SETT_APRV_M',
'CDC.STT01.FSU_CLSG_X',
'CDC.STT01.FSU_COM_BSC_C',
'CDC.STT01.SET_SETT_APRV_D',
'CDC.STT01.SET_IDVL_EXPN_SETT_D',
'CDC.STT01.SET_AIR_SETT_D',
'CDC.STT01.DEP_DEPR_NM_X',
'CDC.STT01.REC_UNCL_RES_S',
'CDC.STT01.FSU_SETT_ACNT_CD_C',
'CDC.STT01.SET_TKT_SETT_FEE_X',
'CDC.STT01.SET_LND_COST_RFD_D',
'CDC.STT01.DEP_CARD_AUTH_M',
'CDC.STT01.FSU_COM_DTL_C',
'CDC.STT01.SET_LND_COST_SETT_D',
'CDC.STT01.FSU_USR_ATHR_M',
'CDC.STT01.SET_JNT_EXPN_SETT_D',
'CDC.STT01.SET_LND_RFD_SETT_X',
'CDC.STT01.DEP_UNFY_RECM_PAY_X',
'CDC.STT01.REC_UNCL_RFD_RES_S',
'CDC.STT01.DEP_CARD_AUTH_D'
];

var results = '';

for(var i = 0; i < tbl_list.length; i++){
    var tbl = tbl_list[i];
    try {
        // 1. CALL CDC_CREATE_MERGE_PROC_SCRIPT -> SQL 문자열 반환
        var call_stmt = snowflake.createStatement({
            sqlText: `CALL CDC.PUBLIC.CDC_CREATE_MERGE_PROC_SCRIPT('${tbl}')`
        });
        var rs = call_stmt.execute();
        rs.next();
        var merge_sql = rs.getColumnValue(1);  // 반환된 SQL 문자열

        // 2. 반환된 SQL 문자열 실행 -> 실제 MERGE 프로시저 생성
        var exec_stmt = snowflake.createStatement({sqlText: merge_sql});
        exec_stmt.execute();

        results += 'SUCCESS CREATE MERGE PROC: ' + tbl + '\n';
    } catch(e) {
        results += 'ERROR CREATE MERGE PROC: ' + tbl + ' -> ' + e.message + '\n';
    }
}

return results;
$$;


/*
    데이터 MERGE PROCEDURE DDL 문
*/

CREATE OR REPLACE PROCEDURE CDC.PUBLIC.CDC_CREATE_MERGE_PROC_SCRIPT("TBL" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
    // 테이블 이름 파싱: DB.SCHEMA.TABLE
    var parts = TBL.split('.');
    if(parts.length !== 3){
        return 'ERROR: Table name must be in DB.SCHEMA.TABLE format';
    }
    var db  = parts[0].toUpperCase();
    var sch = parts[1].toUpperCase();
    var tbl = parts[2].toUpperCase();

    // SHOW PRIMARY KEYS 
    var pk_rs = snowflake.createStatement({
        sqlText: "SHOW PRIMARY KEYS IN TABLE " + TBL
    }).execute();
    var pk_list = [];
    while(pk_rs.next()){
        pk_list.push(pk_rs.getColumnValue('column_name'));
    }
    if(pk_list.length === 0){
        return "ERROR: No PK found for " + TBL;
    }
    var pk_csv = pk_list.join(",");
    var pk_on  = pk_list.map(c => `t.${c} = s.${c}`).join(" AND ");

    // 컬럼 조회 
    var col_rs = snowflake.createStatement({
        sqlText: `
            SELECT COLUMN_NAME
            FROM ${db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '${sch}' AND TABLE_NAME = '${tbl}'
            ORDER BY ORDINAL_POSITION
        `
    }).execute();
    var cols = [];
    while(col_rs.next()){
        cols.push(col_rs.getColumnValue(1));
    }

    // 핵심 수정: UPDATE SET을 공백만 사용하여 한 줄로 연결
    var updateList = cols.map(c => `t.${c} = s.${c}`).join(', ');
    var insertCols = cols.join(', ');
    var insertVals = cols.map(c => `s.${c}`).join(', ');

    // SQL 프로시저 스크립트
    var merge_log_sql = `
CREATE OR REPLACE PROCEDURE STG_${db}.${sch}.${tbl}()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
'DECLARE
    v_start_time TIMESTAMP_LTZ;
    v_max_seq VARCHAR;
    
    -- 대상 테이블 정보 (로깅용)
    v_db_name VARCHAR := ''${db}'';      
    v_schema_name VARCHAR := ''${sch}'';  
    v_table_name VARCHAR := ''${tbl}'';  
    
    -- 처리 건수 변수
    v_merge_rows INT DEFAULT 0;
    v_delete_rows INT DEFAULT 0;
    v_inserted_rows INT DEFAULT 0; /*!!!!! 추가내용 !!!!!*/
    v_updated_rows INT DEFAULT 0; /*!!!!! 추가내용 !!!!!*/
    v_deleted_rows_merge INT DEFAULT 0; /*!!!!! 추가내용 !!!!!*/
    
    -- 쿼리 ID 변수 (RESULT_SCAN 사용을 위해 필요)
    v_merge_query_id VARCHAR;
    v_delete_query_id VARCHAR;
    
    -- 최종 결과 로깅용 변수
    v_result_msg VARCHAR;
    v_success_flag CHAR(1);
    v_log_time TIMESTAMP_LTZ; /*!!!!! 추가내용 !!!!!*/
    
    -- 예외 처리를 위한 변수
    v_sql_error_msg VARCHAR;  

BEGIN
    -- 0. 작업 시작 시간 기록
    v_start_time := CURRENT_TIMESTAMP();
    
    -- 1. 주 CDC 트랜잭션 시작 (Merge, Delete 포함)
    BEGIN TRANSACTION;  

    /* 1. 스테이징 테이블의 최대 변경 시퀀스 획득 */
    SELECT max(hd_change_seq)  
    INTO v_max_seq  
    FROM STG_${db}.${sch}.${tbl};
    
    -- 스테이징 테이블이 비어있을 경우 처리 (중략)
    IF (v_max_seq IS NULL) THEN
        ROLLBACK;
        v_result_msg := ''OK: STG table is empty, no MERGE performed.'';
        v_success_flag := ''Y'';
        
        -- 빈 테이블 처리 로깅 (별도 트랜잭션으로 분리)
        v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/
        BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
        
        INSERT INTO STG_${db}.PUBLIC.CDC_LOG  
            (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
        VALUES  
            (:v_db_name, :v_schema_name, :v_table_name, NULL, :v_start_time, :v_log_time, 0, 0, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
            
        COMMIT; /*!!!!! 추가내용 !!!!!*/
        
        RETURN :v_result_msg;
    END IF;

    /* 2. CDC 데이터 MERGE (대상 테이블에 변경사항 반영) */
    MERGE INTO ${db}.${sch}.${tbl} t
    USING (
        WITH RN_DATA AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY ${pk_csv}
                    ORDER BY hd_change_seq DESC, hd_timestamp DESC
                ) AS rn
            FROM STG_${db}.${sch}.${tbl}
            WHERE hd_change_seq <= :v_max_seq
        )
        SELECT *
        FROM RN_DATA
        WHERE rn = 1
    ) s
    ON ${pk_on}
    
    WHEN MATCHED AND s.hd_change_oper = ''D'' THEN  
        DELETE 
    WHEN MATCHED AND s.hd_change_oper IN (''I'',''U'') THEN
        UPDATE SET ${updateList}  -- 수정: 이 줄의 공백을 최소화 (UPDATE SET 바로 뒤에 변수 삽입)
            
    WHEN NOT MATCHED AND s.hd_change_oper IN (''I'',''U'') THEN
        INSERT (${insertCols}) 
        VALUES (${insertVals});
        
    -- MERGE 쿼리 ID 저장 및 건수 추출 
    v_merge_query_id := LAST_QUERY_ID();  
    SELECT "number of rows inserted" + "number of rows updated" + "number of rows deleted",
           "number of rows inserted", /*!!!!! 추가내용 !!!!!*/
           "number of rows updated", /*!!!!! 추가내용 !!!!!*/
           "number of rows deleted" /*!!!!! 추가내용 !!!!!*/
    INTO v_merge_rows, v_inserted_rows, v_updated_rows, v_deleted_rows_merge /*!!!!! 추가내용 !!!!!*/
    FROM TABLE(RESULT_SCAN(:v_merge_query_id));
    
    /* 3. 처리 완료된 스테이징 데이터 삭제 */
    DELETE FROM STG_${db}.${sch}.${tbl}
    WHERE hd_change_seq <= :v_max_seq;
    
    -- DELETE 쿼리 ID 저장 및 건수 추출 
    v_delete_query_id := LAST_QUERY_ID();
    SELECT "number of rows deleted"  
    INTO v_delete_rows
    FROM TABLE(RESULT_SCAN(:v_delete_query_id));
    
    -- 성공 결과 메시지 및 플래그 설정 (I/U/D 포함)
    v_result_msg := ''SUCCESS: Target Merged: '' || :v_merge_rows /*!!!!! 추가내용 !!!!!*/
                    || '' rows (I:'' || :v_inserted_rows || '', U:'' || :v_updated_rows || '', D:'' || :v_deleted_rows_merge || '')'' /*!!!!! 추가내용 !!!!!*/
                    || ''. STG Deleted: '' || :v_delete_rows || '' rows.''; /*!!!!! 추가내용 !!!!!*/
    v_success_flag := ''Y'';
    v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/
    
    -- 4. 주 CDC 트랜잭션 커밋 (CDC 변경사항 확정 및 락 즉시 해제)
    COMMIT;
    
    -- 5. 로깅 테이블(CDC_LOG)에 처리 결과 기록 (별도의 짧은 트랜잭션)
    BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
    
    INSERT INTO STG_${db}.PUBLIC.CDC_LOG  
        (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
    VALUES  
        (:v_db_name, :v_schema_name, :v_table_name, :v_max_seq, :v_start_time, :v_log_time, :v_merge_rows, :v_delete_rows, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
        
    COMMIT; /*!!!!! 추가내용 !!!!!*/
    
    -- 성공 메시지 반환
    RETURN :v_result_msg;
    
    -- 5. 모든 작업 완료 후 최종 커밋
    COMMIT;
    
    -- 성공 메시지 반환
    RETURN :v_result_msg;

-- 6. 예외 처리 블록
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        v_sql_error_msg := SQLERRM;
        ROLLBACK; -- CDC 트랜잭션 롤백 및 락 해제
        v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/
        
        -- 실패 로깅 메시지 설정
        v_result_msg := ''ERROR: Transaction rolled back due to STATEMENT_ERROR. No changes committed. Error Details: '' || v_sql_error_msg;
        v_success_flag := ''N'';

        -- 실패 로깅 (별도 트랜잭션)
        BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
        
        INSERT INTO STG_${db}.PUBLIC.CDC_LOG  
            (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
        VALUES  
            (:v_db_name, :v_schema_name, :v_table_name, :v_max_seq, :v_start_time, :v_log_time, :v_merge_rows, :v_delete_rows, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
            
        COMMIT; /*!!!!! 추가내용 !!!!!*/

        RETURN :v_result_msg;

    WHEN OTHER THEN
        v_sql_error_msg := SQLERRM;
        ROLLBACK; -- CDC 트랜잭션 롤백 및 락 해제
        v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/

        -- 치명적인 오류 로깅 메시지 설정
        v_result_msg := ''FATAL ERROR: Transaction rolled back due to unhandled error. Details: '' || v_sql_error_msg;
        v_success_flag := ''N'';

        -- 치명적인 오류 로깅 (별도 트랜잭션)
        BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
        
        INSERT INTO STG_${db}.PUBLIC.CDC_LOG  
            (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
        VALUES  
            (:v_db_name, :v_schema_name, :v_table_name, :v_max_seq, :v_start_time, :v_log_time, :v_merge_rows, :v_delete_rows, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
            
        COMMIT; /*!!!!! 추가내용 !!!!!*/

        RETURN :v_result_msg;

END;
';
`;

    return merge_log_sql;
$$;



/*
    PROCEDURE 테스트 및 확인
*/


call CDC.PUBLIC.CDC_CREATE_MERGE_PROC_SCRIPT('CDC.HCS01.UC_SYS_USER');



select count(*) from CDC.HCS01.UC_SYS_USER;

select * from STG_CDC.HCS01.UC_SYS_USER;
select * from STG_CDC.HCS01.UC_SYS_USER where hd_change_seq > '20251127063637000000000000004674661';

call STG_CDC.HCS01.UC_SYS_USER();

select * from STG_CDC.PUBLIC.CDC_LOG order by start_time desc;

-- SUCCESS: Target Merged: 842 rows (I:0, U:842, D:0). STG Deleted: 103603 rows.

CREATE OR REPLACE PROCEDURE STG_CDC.HCS01.UC_SYS_USER()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
'DECLARE
    v_start_time TIMESTAMP_LTZ;
    v_max_seq VARCHAR;
    
    -- 대상 테이블 정보 (로깅용)
    v_db_name VARCHAR := ''CDC'';      
    v_schema_name VARCHAR := ''HCS01'';  
    v_table_name VARCHAR := ''UC_SYS_USER'';  
    
    -- 처리 건수 변수
    v_merge_rows INT DEFAULT 0;
    v_delete_rows INT DEFAULT 0;
    v_inserted_rows INT DEFAULT 0; /*!!!!! 추가내용 !!!!!*/
    v_updated_rows INT DEFAULT 0; /*!!!!! 추가내용 !!!!!*/
    v_deleted_rows_merge INT DEFAULT 0; /*!!!!! 추가내용 !!!!!*/
    
    -- 쿼리 ID 변수 (RESULT_SCAN 사용을 위해 필요)
    v_merge_query_id VARCHAR;
    v_delete_query_id VARCHAR;
    
    -- 최종 결과 로깅용 변수
    v_result_msg VARCHAR;
    v_success_flag CHAR(1);
    v_log_time TIMESTAMP_LTZ; /*!!!!! 추가내용 !!!!!*/
    
    -- 예외 처리를 위한 변수
    v_sql_error_msg VARCHAR;  

BEGIN
    -- 0. 작업 시작 시간 기록
    v_start_time := CURRENT_TIMESTAMP();
    
    -- 1. 주 CDC 트랜잭션 시작 (Merge, Delete 포함)
    BEGIN TRANSACTION;  

    /* 1. 스테이징 테이블의 최대 변경 시퀀스 획득 */
    SELECT max(hd_change_seq)  
    INTO v_max_seq  
    FROM STG_CDC.HCS01.UC_SYS_USER;
    
    -- 스테이징 테이블이 비어있을 경우 처리 (중략)
    IF (v_max_seq IS NULL) THEN
        ROLLBACK;
        v_result_msg := ''OK: STG table is empty, no MERGE performed.'';
        v_success_flag := ''Y'';
        
        -- 빈 테이블 처리 로깅 (별도 트랜잭션으로 분리)
        v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/
        BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
        
        INSERT INTO STG_CDC.PUBLIC.CDC_LOG  
            (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
        VALUES  
            (:v_db_name, :v_schema_name, :v_table_name, NULL, :v_start_time, :v_log_time, 0, 0, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
            
        COMMIT; /*!!!!! 추가내용 !!!!!*/
        
        RETURN :v_result_msg;
    END IF;

    /* 2. CDC 데이터 MERGE (대상 테이블에 변경사항 반영) */
    MERGE INTO CDC.HCS01.UC_SYS_USER t
    USING (
        WITH RN_DATA AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY USER_ID
                    ORDER BY hd_change_seq DESC, hd_timestamp DESC
                ) AS rn
            FROM STG_CDC.HCS01.UC_SYS_USER
            WHERE hd_change_seq <= :v_max_seq
        )
        SELECT *
        FROM RN_DATA
        WHERE rn = 1
    ) s
    ON t.USER_ID = s.USER_ID
    
    WHEN MATCHED AND s.hd_change_oper = ''D'' THEN  
        DELETE 
    WHEN MATCHED AND s.hd_change_oper IN (''I'',''U'') THEN
        UPDATE SET t.USER_ID = s.USER_ID, t.USER_GBN_CD = s.USER_GBN_CD, t.USER_NM = s.USER_NM, t.TEAM_LCD = s.TEAM_LCD, t.TEAM_MCD = s.TEAM_MCD, t.TEAM_SCD = s.TEAM_SCD, t.GRAD_CD = s.GRAD_CD, t.OFCPS_CD = s.OFCPS_CD, t.ACD_GROUP_CD = s.ACD_GROUP_CD, t.CTI_LOGIN_ID = s.CTI_LOGIN_ID, t.INLN_NO = s.INLN_NO, t.CB_ASGN_YN = s.CB_ASGN_YN, t.ECNY_DATE = s.ECNY_DATE, t.RETIRE_DATE = s.RETIRE_DATE, t.LOGIN_YN = s.LOGIN_YN, t.USE_POSBL_YN = s.USE_POSBL_YN, t.REG_USER_ID = s.REG_USER_ID, t.REG_DT = s.REG_DT, t.CHG_USER_ID = s.CHG_USER_ID, t.CHG_DT = s.CHG_DT, t.SKILL_CD = s.SKILL_CD, t.USER_STATUS = s.USER_STATUS, t.USER_STATUS_DT = s.USER_STATUS_DT, t.CDC_LD_DTTM = s.CDC_LD_DTTM  -- 수정: 이 줄의 공백을 최소화 (UPDATE SET 바로 뒤에 변수 삽입)
            
    WHEN NOT MATCHED AND s.hd_change_oper IN (''I'',''U'') THEN
        INSERT (USER_ID, USER_GBN_CD, USER_NM, TEAM_LCD, TEAM_MCD, TEAM_SCD, GRAD_CD, OFCPS_CD, ACD_GROUP_CD, CTI_LOGIN_ID, INLN_NO, CB_ASGN_YN, ECNY_DATE, RETIRE_DATE, LOGIN_YN, USE_POSBL_YN, REG_USER_ID, REG_DT, CHG_USER_ID, CHG_DT, SKILL_CD, USER_STATUS, USER_STATUS_DT, CDC_LD_DTTM) 
        VALUES (s.USER_ID, s.USER_GBN_CD, s.USER_NM, s.TEAM_LCD, s.TEAM_MCD, s.TEAM_SCD, s.GRAD_CD, s.OFCPS_CD, s.ACD_GROUP_CD, s.CTI_LOGIN_ID, s.INLN_NO, s.CB_ASGN_YN, s.ECNY_DATE, s.RETIRE_DATE, s.LOGIN_YN, s.USE_POSBL_YN, s.REG_USER_ID, s.REG_DT, s.CHG_USER_ID, s.CHG_DT, s.SKILL_CD, s.USER_STATUS, s.USER_STATUS_DT, s.CDC_LD_DTTM);
        
    -- MERGE 쿼리 ID 저장 및 건수 추출 
    v_merge_query_id := LAST_QUERY_ID();  
    SELECT "number of rows inserted" + "number of rows updated" + "number of rows deleted",
           "number of rows inserted", /*!!!!! 추가내용 !!!!!*/
           "number of rows updated", /*!!!!! 추가내용 !!!!!*/
           "number of rows deleted" /*!!!!! 추가내용 !!!!!*/
    INTO v_merge_rows, v_inserted_rows, v_updated_rows, v_deleted_rows_merge /*!!!!! 추가내용 !!!!!*/
    FROM TABLE(RESULT_SCAN(:v_merge_query_id));
    
    /* 3. 처리 완료된 스테이징 데이터 삭제 */
    DELETE FROM STG_CDC.HCS01.UC_SYS_USER
    WHERE hd_change_seq <= :v_max_seq;
    
    -- DELETE 쿼리 ID 저장 및 건수 추출 
    v_delete_query_id := LAST_QUERY_ID();
    SELECT "number of rows deleted"  
    INTO v_delete_rows
    FROM TABLE(RESULT_SCAN(:v_delete_query_id));
    
    -- 성공 결과 메시지 및 플래그 설정 (I/U/D 포함)
    v_result_msg := ''SUCCESS: Target Merged: '' || :v_merge_rows /*!!!!! 추가내용 !!!!!*/
                    || '' rows (I:'' || :v_inserted_rows || '', U:'' || :v_updated_rows || '', D:'' || :v_deleted_rows_merge || '')'' /*!!!!! 추가내용 !!!!!*/
                    || ''. STG Deleted: '' || :v_delete_rows || '' rows.''; /*!!!!! 추가내용 !!!!!*/
    v_success_flag := ''Y'';
    v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/
    
    -- 4. 주 CDC 트랜잭션 커밋 (CDC 변경사항 확정 및 락 즉시 해제)
    COMMIT;
    
    -- 5. 로깅 테이블(CDC_LOG)에 처리 결과 기록 (별도의 짧은 트랜잭션)
    BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
    
    INSERT INTO STG_CDC.PUBLIC.CDC_LOG  
        (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
    VALUES  
        (:v_db_name, :v_schema_name, :v_table_name, :v_max_seq, :v_start_time, :v_log_time, :v_merge_rows, :v_delete_rows, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
        
    COMMIT; /*!!!!! 추가내용 !!!!!*/
    
    -- 성공 메시지 반환
    RETURN :v_result_msg;
    
    -- 5. 모든 작업 완료 후 최종 커밋
    COMMIT;
    
    -- 성공 메시지 반환
    RETURN :v_result_msg;

-- 6. 예외 처리 블록
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        v_sql_error_msg := SQLERRM;
        ROLLBACK; -- CDC 트랜잭션 롤백 및 락 해제
        v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/
        
        -- 실패 로깅 메시지 설정
        v_result_msg := ''ERROR: Transaction rolled back due to STATEMENT_ERROR. No changes committed. Error Details: '' || v_sql_error_msg;
        v_success_flag := ''N'';

        -- 실패 로깅 (별도 트랜잭션)
        BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
        
        INSERT INTO STG_CDC.PUBLIC.CDC_LOG  
            (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
        VALUES  
            (:v_db_name, :v_schema_name, :v_table_name, :v_max_seq, :v_start_time, :v_log_time, :v_merge_rows, :v_delete_rows, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
            
        COMMIT; /*!!!!! 추가내용 !!!!!*/

        RETURN :v_result_msg;

    WHEN OTHER THEN
        v_sql_error_msg := SQLERRM;
        ROLLBACK; -- CDC 트랜잭션 롤백 및 락 해제
        v_log_time := CURRENT_TIMESTAMP(); /*!!!!! 추가내용 !!!!!*/

        -- 치명적인 오류 로깅 메시지 설정
        v_result_msg := ''FATAL ERROR: Transaction rolled back due to unhandled error. Details: '' || v_sql_error_msg;
        v_success_flag := ''N'';

        -- 치명적인 오류 로깅 (별도 트랜잭션)
        BEGIN TRANSACTION; /*!!!!! 추가내용 !!!!!*/
        
        INSERT INTO STG_CDC.PUBLIC.CDC_LOG  
            (DB_NAME, SCHEMA_NAME, TABLE_NAME, HD_CHANGE_SEQ, START_TIME, MERGE_TIME, MERGE_ROWS, STG_DELETED_ROWS, RESULT, SUCCESS_FLAG)
        VALUES  
            (:v_db_name, :v_schema_name, :v_table_name, :v_max_seq, :v_start_time, :v_log_time, :v_merge_rows, :v_delete_rows, :v_result_msg, :v_success_flag); /*!!!!! 추가내용 !!!!!*/
            
        COMMIT; /*!!!!! 추가내용 !!!!!*/

        RETURN :v_result_msg;

END;
';
