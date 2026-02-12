
-- SnowPIPE 생성 시에 PARQUET 파일과 STAGE 테이블의 컬럼 지정 필수 (순서에 영향을 받지 않기 위함))


/*
    한번에 모든 스키마의 테이블에 대해 PIPE 생성 및 REFRESH 는 시간 소요가 많이 걸릴 수 있음
    SCHEMA 별로 나눠서 진행 권장

    테이블 목록 추출하고
    PROCEDURE 에 테이블 목록 넣어주고 마지막에 콤마 (,) 제거해주고
    해당 PROCEDURE 수행

    AIR01 : 테이블 70 개 / 3분 44초 
        이모지 또는 이모티콘 관련 값으로 인해 테이블 2개 수동 수행 필요
        DCD_AIR_PAY_DTRB_X, DCD_AIR_CUPN_PAY_X
    CHL01 : 테이블 44 개 / 1분 56초
    COM01 : 테이블 69 개 / 3분 3초
    CST01 : 테이블 18 개 / 1분 2초
    HCS01 : 테이블 3 개 / 11초
    HGSIN01 : 테이블 21 개 / 40초
    HTL01 : 테이블 76 개 / 2분 56초
    HTSAC01 : 테이블 9 개 / 16초
    HTSBR01 : 테이블 27 개 / 51초
    HTSIN01 : 테이블 43 개 / 1분 35초
    HTSML01 : 테이블 25 개 / 1분 14초
    HTSOT01 : 테이블 5 개 / 6초
    HUB01 : 테이블 25 개 / 56초
    PKG01 : 테이블 73 개 / 3분 6초
    STT01 : 테이블 24 개 / 55초
    
*/

use database stg_cdc;
use schema air01;

show pipes;


/*
    테이블 목록 추출
        CDC. 붙게끔 추출
*/

select '''CDC.' || table_schema || '.' || table_name || ''',' from information_schema.tables 
WHERE TABLE_SCHEMA IN ('STT01')
;



/*
    PIPE 생성 및 REFRESH 수행하는 프로시저 생성문

    테이블 목록 추출하고
    PROCEDURE 에 테이블 목록 넣어주고 마지막에 콤마 (,) 제거해주고
    해당 PROCEDURE 수행
*/


CREATE OR REPLACE PROCEDURE STG_CDC.PUBLIC.CREATE_AND_REFRESH_ALL_PIPES()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var tbl_list = [
'CDC.STT01.SET_AIR_APRV_D',
'CDC.STT01.FSU_COM_DTL_C',
'CDC.STT01.SET_JNT_EXPN_SETT_D',
'CDC.STT01.SET_SETT_APRV_D',
'CDC.STT01.SET_LND_COST_RFD_D',
'CDC.STT01.DEP_CARD_AUTH_M',
'CDC.STT01.FSU_COM_BSC_C',
'CDC.STT01.SET_NSTG_SETT_M',
'CDC.STT01.DEP_CARD_AUTH_D',
'CDC.STT01.DEP_DEPR_NM_X',
'CDC.STT01.DEP_UNFY_RECM_PAY_X',
'CDC.STT01.SET_SETT_APRV_M',
'CDC.STT01.FSU_CLSG_X',
'CDC.STT01.SET_TKT_SETT_X',
'CDC.STT01.REC_UNCL_RES_S',
'CDC.STT01.REC_UNCL_RFD_RES_S',
'CDC.STT01.SET_TKT_SETT_FEE_X',
'CDC.STT01.SET_LND_COST_SETT_D',
'CDC.STT01.SET_LND_RFD_SETT_X',
'CDC.STT01.SET_IDVL_EXPN_SETT_D',
'CDC.STT01.FSU_SETT_ACNT_CD_C',
'CDC.STT01.FSU_USR_ATHR_M',
'CDC.STT01.SET_AIR_SETT_D',
'CDC.STT01.SET_PROD_SETT_M'
];

var results = '';

for(var i = 0; i < tbl_list.length; i++){
    var tbl = tbl_list[i];
    try{
        // 1. 기존 프로시저 호출 → PIPE 생성 SQL 문자열 반환
        var gen_stmt = snowflake.createStatement({
            sqlText: `CALL CDC.PUBLIC.CDC_CREATE_PIPE_SCRIPT('${tbl}')`
        });
        var rs = gen_stmt.execute();
        rs.next();
        var pipe_sql = rs.getColumnValue(1);

        // 2. 반환된 SQL 문자열을 세미콜론으로 나눠 한 문장씩 실행
        var sqls = pipe_sql.split(';');
        for(var j = 0; j < sqls.length; j++){
            var s = sqls[j].trim();
            if(s){ // 빈 문자열 제외
                var exec_stmt = snowflake.createStatement({sqlText: s});
                exec_stmt.execute();
            }
        }

        results += 'SUCCESS: ' + tbl + '\n';
    } catch(e){
        results += 'ERROR: ' + tbl + ' -> ' + e.message + '\n';
    }
}

return results;
$$;



/*
    PIPE 생성 및 REFRESH 수행하는 PROCEDURE 수행

    해당 PROCEDURE 를 CALL 수행해도 되고, --> 끝날 때까지 세션 대기해야 함
    별도 1회성 TASK 생성해서 진행해도 무방 --> 수행하고 해당 세션에서 모니터링 가능
*/


CALL STG_CDC.PUBLIC.CREATE_AND_REFRESH_ALL_PIPES();



/*
    1회성 TASK 생성 예시
*/


CREATE OR REPLACE TASK STG_CDC.PUBLIC.CREATE_AND_REFRESH_ALL_PIPES
WAREHOUSE = CDC_WH;
AS
CALL STG_CDC.PUBLIC.CREATE_AND_REFRESH_ALL_PIPES();

EXECUTE TASK STG_CDC.PUBLIC.CREATE_AND_REFRESH_ALL_PIPES;


SHOW TASKS;

SELECT * 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
;