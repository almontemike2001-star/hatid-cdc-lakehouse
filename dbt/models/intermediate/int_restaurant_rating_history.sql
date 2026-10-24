-- Every rating a restaurant has had, with its validity window in business time.
-- The changelog is at-least-once (connector restarts replay events), so dedupe on (key, LSN).
with versions as (
    select restaurant_id, rating, updated_at, _lsn
    from {{ source('bronze', 'restaurants_changelog') }}
    where _deleted = 0
    order by restaurant_id, _lsn
    limit 1 by restaurant_id, _lsn
)

select
    restaurant_id,
    rating,
    updated_at as valid_from,
    leadInFrame(toNullable(updated_at)) over (
        partition by restaurant_id order by _lsn
        rows between unbounded preceding and unbounded following
    ) as valid_to
from versions
