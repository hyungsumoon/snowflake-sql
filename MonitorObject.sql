-- DB
SELECT  a.*
FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES a
where 1=1
and deleted is null
and type = 'STANDARD'
and database_owner not in ('ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN')
order by database_name
;

-------------------------------------------------------------------------------
-- SCHEMA
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA
WHERE 1=1
and catalog_name = 'DWDM'
ORDER BY schema_name
;

-- SELECT
--     catalog_name   AS database_name,
--     schema_name,
--     schema_owner
-- FROM dwdm.information_schema.schemata
-- ORDER BY schema_name
-- ;
-------------------------------------------------------------------------------
-- TABLE
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE 1=1
and table_catalog = 'DWDM'
AND table_schema = 'INF01'
ORDER BY table_name
;

-- SELECT *
-- FROM dwdm.information_schema.tables
-- WHERE table_schema = 'INF01'
-- ORDER BY table_name
-- ;
-------------------------------------------------------------------------------
-- DBT Project
SELECT
    table_schema,
    table_name,
    comment
FROM dwdm.information_schema.tables
WHERE comment ILIKE '%dbt%'
ORDER BY table_schema, table_name;

SELECT DISTINCT
    project_name
FROM metadata.dbt_artifacts.manifest
ORDER BY project_name;

SELECT
    project_name,
    name        AS model_name,
    schema,
    database
FROM metadata.dbt_artifacts.manifest
WHERE resource_type = 'model'
ORDER BY project_name, model_name;
