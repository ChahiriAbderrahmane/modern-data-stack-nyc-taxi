{{ config(
    materialized='table'
) }}

WITH distinct_payment_types AS (
    SELECT 0 as payment_type, 'Flex Fare trip' as payment_type_description 
    UNION ALL
    SELECT 1, 'Credit card'
    UNION ALL
    SELECT 2, 'Cash'
    UNION ALL
    SELECT 3, 'No charge'
    UNION ALL
    SELECT 4, 'Dispute'
    UNION ALL
    SELECT 5, 'Unknown'
    UNION ALL
    SELECT 6, 'Voided trip'
    UNION ALL
    SELECT 99, 'Unknown'
)

SELECT
    payment_type as payment_type_key,
    payment_type_description
FROM distinct_payment_types