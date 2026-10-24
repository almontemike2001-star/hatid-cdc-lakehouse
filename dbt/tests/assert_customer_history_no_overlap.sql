-- Type 2 intervals for a customer must not overlap, and at most one version is current.
select customer_id
from {{ ref('dim_customers_history') }}
group by customer_id
having countIf(is_current) > 1
    or countIf(valid_to is not null and valid_to < valid_from) > 0
