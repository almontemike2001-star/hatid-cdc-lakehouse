select restaurant_id, restaurant_name, cuisine, city, rating as current_rating, is_active
from {{ ref('stg_restaurants') }}
