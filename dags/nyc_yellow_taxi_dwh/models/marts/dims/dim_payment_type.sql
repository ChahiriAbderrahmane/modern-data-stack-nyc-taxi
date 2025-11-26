{{ config(
    materialized='incremental',
    unique_key='payment_type_key',
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['payment_type_key']}]
) }}

WITH source_data AS (
    SELECT DISTINCT 
        payment_type,
        _ingestion_timestamp
    FROM {{ ref('nyc_tripdata_2024_v2') }} 
    
    {% if is_incremental() %}
        WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

unique_payments AS (
    SELECT 
        payment_type,
        MAX(_ingestion_timestamp) as _ingestion_timestamp
    FROM source_data
    GROUP BY 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_key,
    payment_type,
    CASE payment_type
        WHEN 0 THEN 'Flex Fare trip'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END AS payment_type_description,
    _ingestion_timestamp

FROM unique_payments