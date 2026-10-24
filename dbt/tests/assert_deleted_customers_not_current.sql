-- A customer whose account was deleted in Postgres must not appear in current dimensions.
select c.customer_id
from {{ ref('dim_customers') }} as c
where c.customer_id in (
    select customer_id from {{ source('bronze', 'customers_changelog') }} where _deleted = 1
)
