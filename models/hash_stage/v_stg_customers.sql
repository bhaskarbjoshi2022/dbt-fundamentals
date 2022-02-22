{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: 
  jaffle_shop: "customers"
derived_columns:
  LOAD_DATETIME: current_timestamp::timestamp_tz
  EFFECTIVE_FROM: current_date
  START_DATE: current_date
  END_DATE: "TO_DATE('9999-31-12','YYYY-DD-MM')"
hashed_columns:
  CUSTOMER_HK: "ID"

  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - "ID"
      - "FIRST_NAME"
      - "LAST_NAME"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ dbtvault.stage(include_source_columns=true,
                  source_model=metadata_dict['source_model'],
                  derived_columns=metadata_dict['derived_columns'],
                  hashed_columns=metadata_dict['hashed_columns'],
                  ranked_columns=none) }}