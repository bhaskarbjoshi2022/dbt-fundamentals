{{
    config(
      transient=false,
      materialized = 'incremental',
      unique_key='order_key'
    )
}}

{#- Define the different Type column names we need -#}
{%- set t1_cols = ['no_of_items'] -%}
{%- set t2_cols = ['order_id','color','status','order_date'] -%}


{#- Views to compute the surrogate key for Type 1 changes -#}
with v_orders as
(
    select *,
           {{ dbt_utils.surrogate_key(t1_cols) }} as t1_key
    from {{ ref('hard_deletes_source') }}
)
{#- Only creates the view for incremental runs -#}
{% if is_incremental() -%}
, v_this as
(
    select *,
           {{ dbt_utils.surrogate_key(t1_cols) }} as t1_key
    from {{ this }}
)
{%- endif %}

select t2.order_key,
    {#- Reflect Type 2 changes from snapshot -#}
    {%- for col in t2_cols -%}
      t2.{{col}} as {{col}},
    {% endfor -%}
    {#- Handle Type 1 changes -#}
      {{ dim_type_1_cols(t1_cols,'order','d','t2','s') }},
    {#- Updating updated_at timestamp if there is a type 1 change  -#}
      {{ dim_update_timestamp('order','d','t2','s') }},
    {#- Converting dbt timestamps to AEST time as dbt default is UTC -#}
      convert_timezone('America/New_York',t2.dbt_valid_from::timestamp) as dbt_valid_from,
      convert_timezone('America/New_York',t2.dbt_valid_to::timestamp) as dbt_valid_to
from {{ ref('hard_deletes__snapshot') }} t2
    left join v_orders s
        on t2.order_id = s.order_id
    {# Checks previous records on it self for incremental runs -#}
    {% if is_incremental() -%}
    left join v_this d
        on t2.order_key = d.order_key
    {%- endif %}