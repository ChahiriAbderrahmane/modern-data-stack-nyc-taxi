{{ config(
    materialized='incremental',
    unique_key='rate_code_key',
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['rate_code_key']}]
) }}

WITH source_data AS (
    SELECT DISTINCT 
        ratecodeid_clean,
        _ingestion_timestamp
    FROM {{ ref('nyc_tripdata_2024_v2') }} 
    
    {% if is_incremental() %}
        WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

unique_rate_codes AS (
    SELECT 
        ratecodeid_clean,
        MAX(_ingestion_timestamp) as _ingestion_timestamp
    FROM source_data
    GROUP BY 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ratecodeid_clean']) }} as rate_code_key,
    ratecodeid_clean as ratecodeid,
    CASE
    WHEN ratecodeid_clean = 1 THEN 'Standard rate'
    WHEN ratecodeid_clean = 2 THEN 'JFK'
    WHEN ratecodeid_clean = 3 THEN 'Newark'
    WHEN ratecodeid_clean = 4 THEN 'Nassau or Westchester'
    WHEN ratecodeid_clean = 5 THEN 'Negotiated fare'
    WHEN ratecodeid_clean = 6 THEN 'Group ride'
    WHEN ratecodeid_clean = 99 OR ratecodeid_clean IS NULL THEN 'Unknown'
    ELSE 'Other'
    END AS rate_code_description,
    _ingestion_timestamp
FROM unique_rate_codes