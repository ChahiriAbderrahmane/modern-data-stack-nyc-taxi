{{ config(
    materialized='table'
) }}

WITH distinct_vendors AS (
    SELECT 1 as vendorid, 'Creative Mobile Technologies, LLC' as vendor_name 
    UNION ALL
    SELECT 2, 'Curb Mobility, LLC'
    UNION ALL
    SELECT 6, 'Myle Technologies Inc'
    UNION ALL
    SELECT 7, 'Helix'
    UNION ALL
    SELECT 99, 'Other'
)

SELECT
    vendorid,
    vendor_name
FROM distinct_vendors