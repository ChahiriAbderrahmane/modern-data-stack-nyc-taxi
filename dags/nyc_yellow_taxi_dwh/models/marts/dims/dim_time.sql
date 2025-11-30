{{ config(materialized='table') }}

WITH generate_minutes AS (
    SELECT generate_series(0, 1439) as minute_of_day
)
SELECT
    -- Clé primaire (HHMM)
    (FLOOR(minute_of_day / 60) * 100) + MOD(minute_of_day, 60) as time_id,
    
    ((minute_of_day || ' minutes')::interval)::time AS time_value,
    
    FLOOR(minute_of_day / 60)::INT as hour,
    MOD(minute_of_day, 60)::INT as minute,
    
    -- Moment de la journée 
    CASE 
        WHEN FLOOR(minute_of_day / 60) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,

    CASE 
        WHEN FLOOR(minute_of_day / 60) IN (7, 8, 9) THEN 'Morning Peak'
        WHEN FLOOR(minute_of_day / 60) IN (17, 18, 19) THEN 'Evening Peak'
        ELSE 'Off-Peak'
    END AS traffic_peak_period

FROM generate_minutes