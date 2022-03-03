{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'CUSTOMER_HK',
    on_schema_change = 'fail',
    transient = False
  )
}}

{%- set yaml_metadata -%}
source_model: "v_stg_customers"
src_pk: "CUSTOMER_HK"
src_hashdiff: 
  source_column: "CUSTOMER_HASHDIFF"
  alias: "HASHDIFF"
src_payload:
  - "FIRST_NAME"
  - "LAST_NAME"
  - "VALID_FROM_DATE"
  - "VALID_TO_DATE"
src_eff: "EFFECTIVE_FROM"
src_ldts: "LOAD_DATETIME"
src_source: "SOURCE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ dbtvault.sat(src_pk=metadata_dict["src_pk"],
                src_hashdiff=metadata_dict["src_hashdiff"],
                src_payload=metadata_dict["src_payload"],
                src_eff=metadata_dict["src_eff"],
                src_ldts=metadata_dict["src_ldts"],
                src_source=metadata_dict["src_source"],
                source_model=metadata_dict["source_model"])   }}