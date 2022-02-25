{{
  config(
    materialized = 'incremental',
    unique_key = 'id_hash'
  )
}}
select {{dbt_utils.surrogate_key(['START_STATION_ID','END_STATION_ID','BIKEID','TRIPDURATION','STARTTIME','STOPTIME'])}} as id_hash
     , * 
  from {{ source('citi_bikes','citi_bikes_ext') }}
  qualify row_number() over(partition by id_hash order by START_STATION_ID) = 1