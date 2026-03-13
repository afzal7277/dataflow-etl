-- Fails if any row has zero or negative revenue
select
    order_date,
    total_revenue
from {{ ref('mart_daily_revenue') }}
where total_revenue <= 0