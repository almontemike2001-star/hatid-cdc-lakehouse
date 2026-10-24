select delivery_id, order_id, rider_id, status, distance_km, promised_at, assigned_at,
       picked_up_at, delivered_at, updated_at
from {{ source('bronze', 'deliveries') }} final
where _deleted = 0
