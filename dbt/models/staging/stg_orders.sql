select
    order_id,
    customer_id,
    restaurant_id,
    status,
    cancel_reason,
    subtotal,
    delivery_fee,
    {{ optional_column(source('bronze', 'orders'), 'tip_amount', 'Decimal(10, 2)') }} as tip_amount,
    placed_at,
    accepted_at,
    ready_at,
    cancelled_at,
    updated_at
from {{ source('bronze', 'orders') }} final
where _deleted = 0
