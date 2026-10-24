select rider_id, full_name, city, vehicle, is_active
from {{ ref('stg_riders') }}
