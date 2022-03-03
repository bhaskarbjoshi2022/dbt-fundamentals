{{ config(materialized='incremental')  }}

{%- set source_model = "v_stg_customers" -%}
{%- set src_pk = "CUSTOMER_HK" -%}
{%- set src_start_date = "VALID_FROM_DATE" -%}
{%- set src_end_date = "VALID_TO_DATE"     -%}

{%- set src_eff = "EFFECTIVE_FROM"    -%}
{%- set src_ldts = "LOAD_DATETIME"    -%}
{%- set src_source = "SOURCE"  -%}

{{ dbtvault.eff_sat(src_pk=src_pk, src_dfk=src_dfk, src_sfk=src_sfk,
                    src_start_date=src_start_date, 
                    src_end_date=src_end_date,
                    src_eff=src_eff, src_ldts=src_ldts, 
                    src_source=src_source,
                    source_model=source_model) }}