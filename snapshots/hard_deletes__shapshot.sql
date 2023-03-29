{% snapshot hard_deletes__snapshot %}

{{
    config(
      transient=false,
      target_database=target.database,
      target_schema=target.schema,

      unique_key='order_id',
      check_cols=['color', 'status', 'ORDER_DATE'],
      strategy='check',
      invalidate_hard_deletes=True,
    )
}}

{%- set t1_cols = ['no_of_items'] -%}


select order_id::varchar || '-' || to_char(convert_timezone('America/New_York',current_timestamp::timestamp),'YYYYMMDDHH24MISS') as order_key
     , {{ dbt_utils.surrogate_key(t1_cols) }} as t1_key
     , * 
from {{ ref('hard_deletes_source') }}

{% endsnapshot %}