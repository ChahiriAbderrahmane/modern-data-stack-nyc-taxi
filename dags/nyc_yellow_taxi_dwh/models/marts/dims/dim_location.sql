{{ config(
    materialized='table'
) }}

select * from {{ source('bronze_data', 'taxi_zone_lookup_data') }}