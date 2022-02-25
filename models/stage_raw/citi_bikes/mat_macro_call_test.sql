{{
  config(
    materialized='my_macro'
  )
}}
select * from {{ this }}