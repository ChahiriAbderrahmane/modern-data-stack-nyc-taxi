{{ config(
    materialized='table'
) }}

WITH distance_domains AS (
    SELECT 'Short (< 1 mile)' AS distance_category UNION ALL
    SELECT 'Medium (1-5 miles)' UNION ALL
    SELECT 'Long (5-10 miles)' UNION ALL
    SELECT 'Very Long (> 10 miles)' UNION ALL
    SELECT 'Unknown'
),

duration_domains AS (
    SELECT 'Very Short (< 5 min)' AS duration_category UNION ALL
    SELECT 'Short (5-15 min)' UNION ALL
    SELECT 'Medium (15-30 min)' UNION ALL
    SELECT 'Long (30-60 min)' UNION ALL
    SELECT 'Very Long (> 60 min)' UNION ALL
    SELECT 'Invalid'
),

fare_domains AS (
    SELECT 'Low (< $10)' AS fare_category UNION ALL
    SELECT 'Medium ($10-$25)' UNION ALL
    SELECT 'High ($25-$50)' UNION ALL
    SELECT 'Very High (> $50)' UNION ALL
    SELECT 'Invalid'
),

quality_domains AS (
    SELECT 'Valid' AS data_quality_flag UNION ALL
    SELECT 'Invalid - Negative duration' UNION ALL
    SELECT 'Invalid - Zero duration' UNION ALL
    SELECT 'Suspicious - Over 8h' UNION ALL
    SELECT 'Suspicious - No distance' UNION ALL
    SELECT 'Invalid - Passenger count' UNION ALL
    SELECT 'Invalid - Negative fare' UNION ALL
    SELECT 'Invalid - Negative total'
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'q.data_quality_flag', 
        'd.distance_category', 
        't.duration_category', 
        'f.fare_category'
    ]) }} as trip_category_key,

    q.data_quality_flag,
    d.distance_category,
    t.duration_category,
    f.fare_category

FROM distance_domains d
CROSS JOIN duration_domains t
CROSS JOIN fare_domains f
CROSS JOIN quality_domains q