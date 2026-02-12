
/* report */
with prj_list as (
    select $1 as project_id
    from values(
        ('5D4EC826B84B8CC9A7AB07BF60E7D625') /* HNT_BI */
        )
),
f_path as (
    WITH RECURSIVE OBJECT_HIERARCHY AS (
        SELECT 
            a.OBJECT_ID,
            a.OBJECT_NAME,
            a.PARENT_ID,
            a.PROJECT_ID,
            a.PROJECT_ID AS ORIGINAL_PROJECT_ID,
            a.OBJECT_ID  AS ORIGINAL_OBJECT_ID,
            1 AS LEVEL,
            CAST(a.OBJECT_NAME AS VARCHAR(30000)) AS f_path
        FROM DWDM.MSS01.DSSMDOBJINFO a
        inner join prj_list pl
           on pl.project_id = a.project_id

        UNION ALL
        
        SELECT 
            P.OBJECT_ID,
            P.OBJECT_NAME,
            P.PARENT_ID,
            P.PROJECT_ID,
            C.ORIGINAL_PROJECT_ID AS ORIGINAL_PROJECT_ID,
            C.ORIGINAL_OBJECT_ID  AS ORIGINAL_OBJECT_ID,
            C.LEVEL + 1,
            CAST(P.OBJECT_NAME || '\\' || C.f_path AS VARCHAR(30000))
        FROM DWDM.MSS01.DSSMDOBJINFO P
        INNER JOIN OBJECT_HIERARCHY C 
            ON P.OBJECT_ID = C.PARENT_ID 
            AND P.PROJECT_ID = C.PROJECT_ID
    )
    SELECT ORIGINAL_PROJECT_ID AS PROJECT_ID
         , ORIGINAL_OBJECT_ID  AS OBJECT_ID
         , f_path         AS f_path
    FROM OBJECT_HIERARCHY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ORIGINAL_OBJECT_ID ORDER BY LEVEL DESC) = 1
) ,
user_info as (
    /* 사용자 개체 정보 */
    select object_id
         , object_name
         , ABBREVIATION
         , object_uname 
    from dwdm.mss01.DSSMDOBJINFO
    where 1=1
    and object_type = 34
) ,
result as (
    /* 리포트 추출 */
    select obj.project_id          as project_id
         , obj.object_id           as object_id
         , obj.object_type         as object_type
         , obj.subtype             as subtype     
         , obj.object_name         as object_name
         , obj.owner_id            as owner_id
         , owner_info.object_name  as obj_owner_name
         , owner_info.ABBREVIATION as obj_owner_id
         , repo.user_id            as user_id
         , user_info.object_name   as access_user_name
         , user_info.ABBREVIATION  as access_user_id
         , repo.day_yyyymm         as day_yyyymm
    from DWDM.MSS01.DSSMDOBJINFO obj
    inner join prj_list pl on pl.project_id = obj.project_id
    left join (
        select projectid  as project_id
            , reportid   as object_ID
            , userid     as user_id
            , max(day_yyyymm) as day_yyyymm
        from (
            select projectid,reportid,userid,to_char(day_id,'YYYYMM') as day_yyyymm
            from dwdm.mss01.is_report_stats a
            inner join prj_list pl on pl.project_id = a.projectid
            union all 
            select projectid,reportid,userid,to_char(day_id,'YYYYMM') as day_yyyymm
            from dwdm.mss01.stg_is_report_stats a
            inner join prj_list pl on pl.project_id = a.projectid
        )
        group by projectid
            , reportid
            , userid
    ) repo
      on obj.project_id = repo.project_id
      and obj.object_ID = repo.object_ID
    left join user_info owner_info
      on owner_info.object_ID = obj.owner_id
    left join user_info user_info
      on owner_info.object_ID = repo.user_id
    where 1=1
      and obj.object_type = 3 -- 리포트
    QUALIFY row_number() over(
            partition by obj.project_id,obj.object_id
            order by repo.day_yyyymm desc 
            ) = 1 
    union all 
    /* 도씨에 추출 */
    select obj.project_id          as project_id
         , obj.object_id           as object_id
         , obj.object_type         as object_type
         , obj.subtype             as subtype     
         , obj.object_name         as object_name
         , obj.owner_id            as owner_id
         , owner_info.object_name  as obj_owner_name
         , owner_info.ABBREVIATION as obj_owner_id
         , repo.user_id            as user_id
         , user_info.object_name   as access_user_name
         , user_info.ABBREVIATION  as access_user_id
         , repo.day_yyyymm         as day_yyyymm
    from DWDM.MSS01.DSSMDOBJINFO obj 
    inner join prj_list pl on pl.project_id = obj.project_id
    left join (
        select projectid  as project_id
            , documentid   as object_ID
            , userid     as user_id
            , max(day_yyyymm) as day_yyyymm
        from (
            select projectid,documentid,userid,to_char(day_id,'YYYYMM') as day_yyyymm
            from dwdm.mss01.is_document_stats a
            inner join prj_list pl on pl.project_id = a.projectid
            union all 
            select projectid,documentid,userid,to_char(day_id,'YYYYMM') as day_yyyymm
            from dwdm.mss01.stg_is_document_stats a
            inner join prj_list pl on pl.project_id = a.projectid
        )
        group by projectid
            , documentid
            , userid
    ) repo
      on obj.project_id = repo.project_id
      and obj.object_ID = repo.object_ID
    left join user_info owner_info
      on owner_info.object_ID = obj.owner_id
    left join user_info user_info
      on owner_info.object_ID = repo.user_id
    where 1=1
      and obj.object_type = 55 -- 리포트
    QUALIFY row_number() over(
            partition by obj.project_id,obj.object_id
            order by repo.day_yyyymm desc 
            ) = 1 
)
/* 리포트,도씨에별 최종 접속 일자 */
-- select -- project_id
--     --  , object_id
--        case when object_type = 3 then '그리드/큐브'
--             when object_type = 55 then '도씨에'
--             else '기타' end as object_type_name
--      , case when subtype = 770   then 'report_sql'
--             when subtype = 774   then 'report_그래프'
--             when subtype = 768   then 'report_그리드'
--             when subtype = 769   then '그래프'
--             when subtype = 779   then '엑셀_큐브'
--             when subtype = 776   then '인텔리전스큐브'
--             when subtype = 777   then '점증적_새로_고침'
--             when subtype = 780   then '인텔리전스큐브'
--             when subtype = 14081 then '도큐먼트'           
--          else cast(subtype as string) end as subtype_name     
--      , object_name
--      , b.f_path
--     --  , owner_id
--      , obj_owner_name
--      , obj_owner_id
--     --  , user_id
--      , access_user_name
--      , access_user_id
--      , day_yyyymm as last_connect_yyyymm
-- from result  a
-- left join f_path b
--   on  a.project_id = b.PROJECT_ID
--   and a.object_id = b.object_id
-- where a.day_yyyymm is null 

/* 경로별 리포트/도씨에 개수 */
select last_connect_yyyymm
     , object_type_name
     -- , obj_owner_name
     -- , access_user_name
     , zeroifnull("공용개체") as "공용개체"
     , zeroifnull("사용자") as "사용자"
     , zeroifnull("비정형분석") as "비정형분석"
     , zeroifnull("기타") as "기타"
from (
    select 
       r.day_yyyymm as last_connect_yyyymm
     , case when r.object_type = 3 then '그리드/큐브'
            when r.object_type = 55 then '도씨에'
            else '기타' end as object_type_name
    --  , case when r.subtype = 770   then 'report_sql'
    --         when r.subtype = 774   then 'report_그래프'
    --         when r.subtype = 768   then 'report_그리드'
    --         when r.subtype = 769   then '그래프'
    --         when r.subtype = 779   then '엑셀_큐브'
    --         when r.subtype = 776   then '인텔리전스큐브'
    --         when r.subtype = 777   then '점증적_새로_고침'
    --         when r.subtype = 780   then '인텔리전스큐브'
    --         when r.subtype = 14081 then '도큐먼트' 
    --         else '기타' end as subtype_name
    -- , obj_owner_name
    -- , access_user_name
    , case when fp.f_path like 'HNT_BI\\00.비정형분석%' then '비정형분석'
           when fp.f_path like 'HNT_BI\\프로파일%'      then '사용자'
           when fp.f_path like 'HNT_BI\\공용 개체%'     then '공용개체'
        else '기타' END as FILE_PATH
    , count(1) as obj_cnt
    from result r
    left join f_path fp
    on  r.project_id = fp.PROJECT_ID
    and r.object_id  = fp.object_id
    group by r.day_yyyymm
        , r.object_type
        --    , r.subtype
        -- , obj_owner_name
        -- , access_user_name
        , case when fp.f_path like 'HNT_BI\\00.비정형분석%' then '비정형분석'
            when fp.f_path like 'HNT_BI\\프로파일%'        then '사용자'
            when fp.f_path like 'HNT_BI\\공용 개체%'       then '공용개체'
            else '기타' END
)
pivot (
   sum(obj_cnt)
   for  FILE_PATH in (
    '공용개체'    as "공용개체"  ,
    '사용자'     as "사용자"  ,
    '비정형분석'  as "비정형분석"  ,
    '기타'      as "기타"
   )
)
order by last_connect_yyyymm desc , object_type_name
;

 