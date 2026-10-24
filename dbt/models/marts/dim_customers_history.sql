-- Type 2 history of customers rebuilt from the CDC changelog (no snapshots needed):
-- one row per version, valid_from/valid_to in business time, deleted accounts flagged.
with changes as (
    select customer_id, full_name, email, city, updated_at, _lsn, _deleted
    from {{ source('bronze', 'customers_changelog') }}
    order by customer_id, _lsn
    limit 1 by customer_id, _lsn
),

deleted as (
    select distinct customer_id from changes where _deleted = 1
),

versions as (
    select
        customer_id, full_name, email, city, _lsn,
        updated_at as valid_from,
        leadInFrame(toNullable(updated_at)) over (
            partition by customer_id order by _lsn
            rows between unbounded preceding and unbounded following
        ) as valid_to
    from changes
    where _deleted = 0
)

select
    v.customer_id as customer_id,
    v.full_name as full_name,
    v.email as email,
    v.city as city,
    v.valid_from as valid_from,
    v.valid_to as valid_to,
    v.valid_to is null and v.customer_id not in (select customer_id from deleted) as is_current,
    v.customer_id in (select customer_id from deleted) as is_deleted_in_source
from versions as v
