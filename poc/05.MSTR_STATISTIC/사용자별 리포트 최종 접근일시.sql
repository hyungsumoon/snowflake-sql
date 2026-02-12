
/* 사용자 리포트 최종 사용 이력 */
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
         , OBJECT_NAME 
         , owner_id
    from DWDM.MSS01.DSSMDOBJINFO
    where 1=1
        and PROJECT_ID='5D4EC826B84B8CC9A7AB07BF60E7D625'
        and object_type = 3
        and subtype in (770,774,768,769)
),
user_info as (
    /* 사용자 개체 정보 */
    select object_id
         , object_name
         , ABBREVIATION
         , object_uname 
    from dwdm.mss01.DSSMDOBJINFO
    where 1=1
    and object_type = 34
)
, access_info as (
    select a.project_id      as project_id 
        , a.object_id      as object_id
        , a.user_id      as user_id
        , a.OBJECT_NAME as  OBJECT_NAME
        , max(a.last_access_ym) as last_access_ym
    from (
        select a.projectid as project_id
            , a.reportid as object_id
            , a.userid as user_id
            , b.OBJECT_NAME as  OBJECT_NAME 
            , max(TO_CHAR(a.day_id,'YYYY-MM')) as last_access_ym
        from dwdm.mss01.is_report_stats a
        inner join report_info b
        on  b.project_id = a.projectid
        and b.object_id = a.reportid
        group by  a.projectid
                , a.reportid
                , a.userid
                , b.OBJECT_NAME 
        union all 
        select a.projectid as project_id
            , a.reportid  as object_id
            , a.userid as user_id
            , b.OBJECT_NAME as  OBJECT_NAME 
            , max(TO_CHAR(a.day_id,'YYYY-MM')) as last_access_ym
        from dwdm.mss01.stg_is_report_stats a
        inner join report_info b
        on  b.project_id = a.projectid
        and b.object_id = a.reportid
        group by  a.projectid
                , a.reportid
                , a.userid
                , b.OBJECT_NAME
    ) a
    group by a.project_id
        , a.object_id
        , a.user_id
        , a.OBJECT_NAME
)
select 
       b.object_name
    ,  b.ABBREVIATION
    ,  a.OBJECT_NAME
    ,  c.object_path
    ,  a.last_access_ym
from access_info  a
left join user_info b
  on a.user_id = b.object_id
left join object_info c
   on c.project_id = a.project_id
   and c.object_id = a.object_id
where 1=1
  and b.object_name not in ('Administrator','BI_admin')
order by b.object_name, a.last_access_ym
;

