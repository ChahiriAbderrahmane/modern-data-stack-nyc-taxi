{{ config(
    materialized='incremental',
    unique_key='datetime_key', 
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['datetime_key']}]
) }}

WITH new_data_source AS (
    SELECT 
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        _ingestion_timestamp
    FROM {{ ref('nyc_tripdata_2024_v2') }}
    
    {% if is_incremental() %}
        WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

unioned_dates AS (
    SELECT 
        tpep_pickup_datetime AS full_datetime,
        _ingestion_timestamp
    FROM new_data_source
    WHERE tpep_pickup_datetime IS NOT NULL

    UNION ALL 

    SELECT 
        tpep_dropoff_datetime AS full_datetime,
        _ingestion_timestamp
    FROM new_data_source
    WHERE tpep_dropoff_datetime IS NOT NULL
),

unique_dates AS (
    SELECT 
        full_datetime,
        MAX(_ingestion_timestamp) as _ingestion_timestamp
    FROM unioned_dates
    GROUP BY 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['full_datetime']) }} AS datetime_key,
    full_datetime,
    DATE(full_datetime) AS date,
    CAST(EXTRACT(YEAR FROM full_datetime) AS INT) AS year,
    CAST(EXTRACT(MONTH FROM full_datetime) AS INT) AS month,
    CAST(EXTRACT(DAY FROM full_datetime) AS INT) AS day,
    CAST(EXTRACT(HOUR FROM full_datetime) AS INT) AS hour,
    CAST(EXTRACT(MINUTE FROM full_datetime) AS INT) AS minute,
    CAST(EXTRACT(SECOND FROM full_datetime) AS INT) AS second,
    CAST(EXTRACT(ISODOW FROM full_datetime) AS INT) AS day_of_week_num,
    TRIM(TO_CHAR(full_datetime, 'Day')) AS day_of_week_name,
    CAST(EXTRACT(WEEK FROM full_datetime) AS INT) AS week_of_year,
    CAST(EXTRACT(QUARTER FROM full_datetime) AS INT) AS quarter,
    CASE 
        WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    CASE 
        WHEN EXTRACT(ISODOW FROM full_datetime) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,

    _ingestion_timestamp

FROM unique_dates