{{ config(materialized='table') }}

WITH distinct_vendors_from_silver AS (
    SELECT DISTINCT 
        vendorid, 
        vendor_name
    FROM {{ ref('nyc_tripdata_2024_v2') }}
    WHERE vendorid IS NOT NULL
),

tabley AS (
    SELECT 
        99 as vendorid, 
        'Missing/Invalid' as vendor_name
),

final_list AS (
    SELECT * FROM distinct_vendors_from_silver
    UNION ALL
    SELECT * FROM tabley
    WHERE NOT EXISTS (SELECT 1 FROM distinct_vendors_from_silver WHERE vendorid = 99)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendorid']) }} as vendor_key,
    vendorid,
    vendor_name,
    NOW() as _ingestion_timestamp

FROM final_list