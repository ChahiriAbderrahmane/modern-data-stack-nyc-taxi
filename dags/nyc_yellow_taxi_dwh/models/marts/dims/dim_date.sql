{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2026-01-01' as date)"
    ) }}
)
SELECT
    -- Clé primaire (Format YYYYMMDD, ex: 20240101)
    CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT) as date_id,
    CAST(date_day AS DATE) as full_date,
    -- Attributs extraits
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(QUARTER FROM date_day) as quarter,
    EXTRACT(MONTH FROM date_day) as month,
    TO_CHAR(date_day, 'Month') as month_name,
    EXTRACT(DAY FROM date_day) as day_of_month,
    CAST(EXTRACT(ISODOW FROM date_day) AS INT) as day_of_week_num,
    TO_CHAR(date_day, 'Day') as day_name,
    CASE WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END as day_type
FROM date_spine