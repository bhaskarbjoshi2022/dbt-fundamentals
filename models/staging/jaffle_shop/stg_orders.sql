select id as order_id
     , user_id as customer_id
     , order_date
     , status
     , source
     , ingest_ts
from {{ source('jaffle_shop', 'orders') }}
