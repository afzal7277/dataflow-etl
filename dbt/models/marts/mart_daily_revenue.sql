{{ config(materialized='table') }}

select
    order_date::date            as order_date,
    country,
    product,
    count(order_id)             as total_orders,
    sum(total_amount)           as total_revenue,
    avg(total_amount)           as avg_order_value
from {{ ref('stg_orders') }}
where status != 'cancelled'
group by 1, 2, 3
order by 1 desc