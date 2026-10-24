with versions as (
    select customer_id, count() as versions
    from {{ ref('dim_customers_history') }}
    group by customer_id
)

select c.customer_id as customer_id, c.full_name as full_name, c.city as city,
       c.created_at as created_at, v.versions as versions
from {{ ref('stg_customers') }} as c
inner join versions as v on c.customer_id = v.customer_id
