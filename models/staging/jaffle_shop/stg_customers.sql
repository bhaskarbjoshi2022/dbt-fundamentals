select id as customer_id
     , first_name
     , last_name
     , source
     , ingest_ts
from {{ source('jaffle_shop', 'customers') }}