select order_id
from {{ ref('fct_orders') }}
where accepted_at < placed_at
   or ready_at < accepted_at
   or delivered_at < picked_up_at
   or (status = 'delivered' and delivered_at is null)
