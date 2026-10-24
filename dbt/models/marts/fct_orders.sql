{{ config(query_settings={'join_use_nulls': 1}) }}

-- One row per order with delivery timings, money and the restaurant rating in effect when the
-- order was placed (point-in-time ASOF join on the rating history).
with o as (select * from {{ ref('stg_orders') }}),
d as (select * from {{ ref('stg_deliveries') }}),
r as (select restaurant_id, restaurant_name, cuisine, city from {{ ref('stg_restaurants') }}),
h as (select restaurant_id, valid_from, rating from {{ ref('int_restaurant_rating_history') }})

select
    o.order_id as order_id,
    o.customer_id as customer_id,
    o.customer_id is null as is_anonymised,
    o.restaurant_id as restaurant_id,
    r.restaurant_name as restaurant_name,
    r.cuisine as cuisine,
    r.city as city,
    o.status as status,
    o.cancel_reason as cancel_reason,
    d.rider_id as rider_id,
    d.distance_km as distance_km,
    o.subtotal as subtotal,
    o.delivery_fee as delivery_fee,
    o.tip_amount as tip_amount,
    if(o.status = 'delivered', o.subtotal + o.delivery_fee, toDecimal64(0, 2)) as gmv,
    o.placed_at as placed_at,
    {{ local_date('o.placed_at') }} as order_date,
    toHour(toTimeZone(o.placed_at, '{{ var("local_tz") }}')) as order_hour,
    {{ minutes_between('o.accepted_at', 'o.ready_at') }} as prep_minutes,
    {{ minutes_between('d.picked_up_at', 'd.delivered_at') }} as delivery_minutes,
    {{ minutes_between('o.placed_at', 'd.delivered_at') }} as total_minutes,
    if(o.status = 'delivered', d.delivered_at <= d.promised_at, null) as is_on_time,
    h.rating as restaurant_rating_at_order,
    o.accepted_at as accepted_at,
    o.ready_at as ready_at,
    d.picked_up_at as picked_up_at,
    d.delivered_at as delivered_at,
    d.promised_at as promised_at,
    o.cancelled_at as cancelled_at
from o
left join d on o.order_id = d.order_id
left join r on o.restaurant_id = r.restaurant_id
asof left join h on o.restaurant_id = h.restaurant_id and o.placed_at >= h.valid_from
