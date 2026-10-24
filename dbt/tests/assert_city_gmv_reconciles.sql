-- The daily city mart must neither lose nor double-count GMV relative to the order fact.
select f.gmv_fact, a.gmv_mart
from (select sum(gmv) as gmv_fact from {{ ref('fct_orders') }}) as f
cross join (select sum(gmv) as gmv_mart from {{ ref('agg_daily_city') }}) as a
where f.gmv_fact != a.gmv_mart
