select system$allowlist();

use warehouse compute_wh;
create or replace warehouse dbt_execute_wh
    warehouse_size = 'xsmall'
    auto_suspend = 600
    max_cluster_count=2
    min_cluster_count=1;

grant usage on warehouse dbt_execute_wh to user sf0001;
grant usage on warehouse dbt_execute_wh to user sf0002;

use warehouse dbt_execute_wh;
--compute_wh (xs): 10s
--dbt_execute_wh (m): 10s
EXECUTE DBT PROJECT DBT_TEST.DEPLOY.DBT_TEST args='run -s DBT_EXECUTE' ;

select current_version(); 
