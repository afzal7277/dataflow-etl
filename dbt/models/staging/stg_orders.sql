{{ config(materialized='view') }}

select
    order_id                            as order_id,
    customer_id                         as customer_id,
    customer_name                       as customer_name,
    product                             as product,
    quantity::integer                   as quantity,
    unit_price::numeric(10,2)           as unit_price,
    total_amount::numeric(10,2)         as total_amount,
    lower(trim(status))                 as status,
    order_date::timestamp               as order_date,
    initcap(country)                    as country,
    current_timestamp                   as _loaded_at
from {{ source('raw_layer', 'orders') }}