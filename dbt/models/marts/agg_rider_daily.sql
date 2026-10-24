select
    f.order_date as order_date,
    f.rider_id as rider_id,
    any(r.full_name) as rider_name,
    any(r.city) as rider_city,
    any(r.vehicle) as vehicle,
    countIf(f.status = 'delivered') as deliveries,
    countIf(f.cancel_reason = 'delivery_failed') as failed_deliveries,
    round(avgIf(f.delivery_minutes, f.status = 'delivered'), 1) as avg_delivery_minutes,
    round(avgIf(toFloat64(f.is_on_time), f.status = 'delivered'), 4) as on_time_rate,
    sumIf(f.distance_km, f.status = 'delivered') as km_delivered
from {{ ref('fct_orders') }} as f
inner join {{ ref('dim_riders') }} as r on f.rider_id = r.rider_id
where f.rider_id is not null
group by f.order_date, f.rider_id
