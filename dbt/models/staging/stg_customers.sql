{{ config(materialized='view') }}

select
    customer_id                         as customer_id,
    name                                as customer_name,
    lower(trim(email))                  as email,
    initcap(country)                    as country,
    signup_date::timestamp              as signup_date,
    current_timestamp                   as _loaded_at
from {{ source('raw_layer', 'customers') }}