{{ config(materialized='table') }}

select
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email,
    c.country,
    o.product,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.order_date
from {{ ref('stg_orders') }}       o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id