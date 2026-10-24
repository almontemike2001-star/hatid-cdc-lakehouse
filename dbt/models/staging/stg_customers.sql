select customer_id, full_name, email, city, created_at, updated_at
from {{ source('bronze', 'customers') }} final
where _deleted = 0
