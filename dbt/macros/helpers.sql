{% macro local_date(ts) -%}
    toDate(toTimeZone({{ ts }}, '{{ var("local_tz") }}'))
{%- endmacro %}

{% macro minutes_between(a, b) -%}
    round(dateDiff('second', {{ a }}, {{ b }}) / 60, 1)
{%- endmacro %}

{#- Columns added to the source later (schema evolution) may not exist yet in every
    environment; select them when present, NULL otherwise. -#}
{% macro optional_column(relation, column, cast_type) -%}
    {%- set cols = adapter.get_columns_in_relation(relation) | map(attribute='name') | list -%}
    {%- if column in cols -%} {{ column }} {%- else -%} cast(null, 'Nullable({{ cast_type }})') {%- endif %}
{%- endmacro %}
