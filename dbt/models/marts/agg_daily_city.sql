select
    order_date,
    city,
    count() as orders,
    countIf(status = 'delivered') as delivered_orders,
    countIf(status = 'cancelled') as cancelled_orders,
    countIf(status not in ('delivered', 'cancelled')) as in_progress_orders,
    round(countIf(status = 'cancelled') / count(), 4) as cancellation_rate,
    sum(gmv) as gmv,
    sumIf(tip_amount, status = 'delivered') as tips,
    round(avgIf(total_minutes, status = 'delivered'), 1) as avg_total_minutes,
    round(avgIf(toFloat64(is_on_time), status = 'delivered'), 4) as on_time_rate
from {{ ref('fct_orders') }}
group by order_date, city
