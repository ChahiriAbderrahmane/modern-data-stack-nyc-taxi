{{ config(
    materialized='table'
) }}

WITH distinct_codes AS (
    SELECT 1 as RatecodeID, 'Standard rate' as rate_code_description 
    UNION ALL
    SELECT 2, 'JFK'
    UNION ALL
    SELECT 3, 'Newark'
    UNION ALL
    SELECT 4, 'Nassau or Westchester'
    UNION ALL
    SELECT 5, 'Negotiated fare'
    UNION ALL
    SELECT 6, 'Group ride'
    UNION ALL
    SELECT 99, 'Unknown'
)

SELECT
    RatecodeID,
    rate_code_description
FROM distinct_codes