select
    id as customer_id,
    first_name,
    last_name,
    current_timestamp::timestamp_tz as ingest_ts
--from dbt_raw.jaffle_shop.customers
from {{ source('jaffle_shop', 'customers') }}