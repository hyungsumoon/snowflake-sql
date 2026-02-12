/* 사용자별 생성한 리포트 개수 */
with user_info as (
    /* 사용자 개체 정보 */
    select object_id
        , object_name
        , ABBREVIATION
        , object_uname 
    from dwdm.mss01.DSSMDOBJINFO
    where 1=1
    and object_type = 34
) ,
report_info as (
    /* 생성되어 있는 리포트 목록  */
    select project_id
         , object_id
         , owner_id
    from DWDM.MSS01.DSSMDOBJINFO
    where 1=1
        -- and PROJECT_ID='5D4EC826B84B8CC9A7AB07BF60E7D625'
        and object_type = 3
        and subtype in (770,774,768,769)
)
select u.object_name
     , u.ABBREVIATION
     , case when rep_info.project_id = '5D4EC826B84B8CC9A7AB07BF60E7D625' then 'HNT_BI' 
            when rep_info.project_id = '250F9F6C7E466D9DBF87328C7506EA5B' then 'HNT_BI_STG'
            else rep_info.project_id end as project
     , count(rep_info.object_id) as rep_cnt
from user_info u
left join report_info rep_info
on rep_info.owner_id = u.object_id
where rep_info.project_id is not null 
group by u.object_name
     , u.ABBREVIATION
     , rep_info.project_id
order by 2 desc ,3 desc 
;