/*
    Snow PIPE 비용 체계

    변경 전 : serverless_copmute(측정불가) +  0.06 Credits per 1000 files
    변경 후 : 1GB = 0.0037 credits (1TB = 3.7 credits)
*/


use warehouse compute_wh;
use role accountadmin;

SELECT
    DATE_TRUNC('HOUR', START_TIME) AS LOAD_HOUR,
    ROUND(SUM(bytes_inserted) / 1024 / 1024 / 1024, 2) AS gb_inserted,
    ROUND(SUM(bytes_billed) / 1024 / 1024 / 1024, 2) AS gb_billed,
    SUM(files_inserted) AS files_inserted,
    SUM(credits_used) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
-- WHERE start_time > '2025-12-17 00:00:00'
GROUP BY DATE_TRUNC('HOUR', START_TIME)
ORDER BY 1 desc;