/* 리포트 경로 */
with object_info as (
    WITH RECURSIVE OBJECT_HIERARCHY AS (
        SELECT 
            OBJECT_ID,
            OBJECT_NAME,
            PARENT_ID,
            PROJECT_ID,
            PROJECT_ID AS ORIGINAL_PROJECT_ID,
            OBJECT_ID AS ORIGINAL_OBJECT_ID,
            1 AS LEVEL,
            CAST(OBJECT_NAME AS VARCHAR(30000)) AS OBJECT_PATH
        FROM DWDM.MSS01.DSSMDOBJINFO
        WHERE 1=1
           and PROJECT_ID = '5D4EC826B84B8CC9A7AB07BF60E7D625'
            and object_type = 3
            and subtype in (770,774,768,769)

        UNION ALL
        
        SELECT 
            P.OBJECT_ID,
            P.OBJECT_NAME,
            P.PARENT_ID,
            P.PROJECT_ID,
            C.ORIGINAL_PROJECT_ID AS ORIGINAL_PROJECT_ID,
            C.ORIGINAL_OBJECT_ID AS ORIGINAL_OBJECT_ID,
            C.LEVEL + 1,
            CAST(P.OBJECT_NAME || '\\' || C.OBJECT_PATH AS VARCHAR(30000))
        FROM DWDM.MSS01.DSSMDOBJINFO P
        INNER JOIN OBJECT_HIERARCHY C 
            ON P.OBJECT_ID = C.PARENT_ID 
            AND P.PROJECT_ID = C.PROJECT_ID
    )
    SELECT ORIGINAL_PROJECT_ID AS PROJECT_ID
         , ORIGINAL_OBJECT_ID  AS OBJECT_ID
         , OBJECT_PATH         AS OBJECT_PATH
    FROM OBJECT_HIERARCHY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ORIGINAL_OBJECT_ID ORDER BY LEVEL DESC) = 1
) ,
report_info as (
    /* 생성되어 있는 리포트 목록  */
    select project_id
         , object_id
    from DWDM.MSS01.DSSMDOBJINFO
    where 1=1
        and PROJECT_ID='5D4EC826B84B8CC9A7AB07BF60E7D625'
        and object_type = 3
        and subtype in (770,774,768,769)
)
select *
from (
    select case when obj.OBJECT_PATH like 'HNT_BI\\00.비정형분석%' then '비정형분석'
                when obj.OBJECT_PATH like 'HNT_BI\\프로파일%'      then '사용자'
                when obj.OBJECT_PATH like 'HNT_BI\\공용 개체%'     then '공용개체'
        else '기타' END as FILE_PATH
            , rep.last_access_ym as last_access_ym
            , count(1) as rep_cnt
    from object_info obj
    left join (
        select a.projectid as project_id
            , a.reportid as object_id
            , max(TO_CHAR(a.day_id,'YYYY-MM')) as last_access_ym
        from dwdm.mss01.is_report_stats a
        inner join report_info b
        on  b.project_id = a.projectid
        and b.object_id = a.reportid
        where 1=1
          -- and a.PARENTINDICATOR != 1 /* 도씨에를 통한 리포트 실행 여부 */
        group by  projectid
                , reportid
        union all 
        select a.projectid as project_id
            , a.reportid  as object_id
            , max(TO_CHAR(a.day_id,'YYYY-MM')) as last_access_ym
        from dwdm.mss01.stg_is_report_stats a
        inner join report_info b
        on  b.project_id = a.projectid
        and b.object_id = a.reportid
        where 1=1
          -- and a.PARENTINDICATOR != 1 /* 도씨에를 통한 리포트 실행 여부 */
        group by  projectid
                , reportid
    ) rep
    on  obj.project_id = rep.project_id
    and obj.object_id = rep.object_id
    group by case when OBJECT_PATH like 'HNT_BI\\00.비정형분석%' then '비정형분석'
                  when OBJECT_PATH like 'HNT_BI\\프로파일%'      then '사용자'
                  when OBJECT_PATH like 'HNT_BI\\공용 개체%'     then '공용개체'
             else '기타' END
        , last_access_ym
)
PIVOT (
   sum(rep_cnt)
   for  FILE_PATH in (ANY)
)
order by 1
;


select AGT_DV_CD
from DWDM.INF01.IBSA_ONLN_QTSH_M
;