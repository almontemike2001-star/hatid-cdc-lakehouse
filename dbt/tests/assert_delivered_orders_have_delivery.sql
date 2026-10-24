select order_id
from {{ ref('fct_orders') }}
where status in ('picked_up', 'delivered') and rider_id is null
