select
    f.restaurant_id as restaurant_id,
    any(d.restaurant_name) as restaurant_name,
    any(d.cuisine) as cuisine,
    any(d.city) as city,
    count() as orders,
    countIf(f.cancel_reason = 'restaurant_rejected') as rejected_orders,
    round(countIf(f.cancel_reason = 'restaurant_rejected') / count(), 4) as rejection_rate,
    round(avg(f.prep_minutes), 1) as avg_prep_minutes,
    sum(f.gmv) as gmv,
    argMin(f.restaurant_rating_at_order, f.placed_at) as rating_at_first_order,
    any(d.current_rating) as current_rating
from {{ ref('fct_orders') }} as f
inner join {{ ref('dim_restaurants') }} as d on f.restaurant_id = d.restaurant_id
group by f.restaurant_id
