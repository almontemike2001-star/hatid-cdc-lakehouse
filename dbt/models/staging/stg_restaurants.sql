select restaurant_id, name as restaurant_name, cuisine, city, rating, is_active, created_at, updated_at
from {{ source('bronze', 'restaurants') }} final
where _deleted = 0
