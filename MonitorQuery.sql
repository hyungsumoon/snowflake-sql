use role accountadmin;

SELECT 
        query_id
        , query_text
        , user_name
        , start_time
        , execution_status
        , current_timestamp()
        , current_timestamp() - interval '1 day' as tt
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE 1=1
-- and start_time > current_time() - 1
-- AND query_text ILIKE '%GRANT%'
AND query_text ILIKE '%STAGE%'
-- and query_text ILIKE '%RL%DBT%'
ORDER BY start_time DESC;
