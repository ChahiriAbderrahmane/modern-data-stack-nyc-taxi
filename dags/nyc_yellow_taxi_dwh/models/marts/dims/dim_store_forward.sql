{{ config(
    materialized='table'
) }}

WITH static_values AS (
    SELECT 'Y' as store_and_fwd_flag, 'Store and forward trip' as description
    UNION ALL
    SELECT 'N', 'Not a store and forward trip'
    UNION ALL
    SELECT 'Unknown', 'Unknown' 
),

final_data AS (
    SELECT 
        store_and_fwd_flag,
        description,
        NOW() as _ingestion_timestamp
    FROM static_values
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['store_and_fwd_flag']) }} as store_forward_key,
    store_and_fwd_flag,
    description as store_and_fwd_description,
    _ingestion_timestamp
FROM final_data