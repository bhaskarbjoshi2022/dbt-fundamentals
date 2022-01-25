select
    id as customer_id,
    first_name,
    last_name
--from dbt_raw.jaffle_shop.customers
from {{ source('jaffle_shop', 'customers') }}