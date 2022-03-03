{{
  config(
    materialized = 'incremental',
    transient = False,
    tags = ['daily']
  )
}}

{%- set yaml_metadata -%}
source_model: 
  - v_stg_customers

src_pk: CUSTOMER_HK
src_nk: ID
src_ldts: LOAD_DATETIME
src_source: SOURCE
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ dbtvault.hub(src_pk=metadata_dict["src_pk"],
                src_nk=metadata_dict["src_nk"], 
                src_ldts=metadata_dict["src_ldts"],
                src_source=metadata_dict["src_source"],
                source_model=metadata_dict["source_model"]) }}