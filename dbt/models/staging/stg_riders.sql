select rider_id, full_name, city, vehicle, is_active, created_at, updated_at
from {{ source('bronze', 'riders') }} final
where _deleted = 0
