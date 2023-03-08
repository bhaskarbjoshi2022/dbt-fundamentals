{%- macro sat_scd2(src_npk, src_pk, src_hashdiff, src_payload, src_eff, src_exp, src_ldts, src_source, source_model, incremental_ts=None) -%}

{{- dbtvault.check_required_parameters(src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
                                       src_ldts=src_ldts, src_source=src_source,
                                       source_model=source_model) -}}

{%- set src_npk = dbtvault.escape_column_names(src_npk) -%}
{%- set src_pk = dbtvault.escape_column_names(src_pk) -%}
{%- set src_hashdiff = dbtvault.escape_column_names(src_hashdiff) -%}
{%- set src_payload = dbtvault.escape_column_names(src_payload) -%}
{%- set src_ldts = dbtvault.escape_column_names(src_ldts) -%}
{%- set src_source_= dbtvault.escape_column_names(src_source) -%}

{%- set source_cols = dbtvault.expand_column_list(columns=[src_pk, sc_hashdiff, src_payload, src_eff, src_exp, src_ldts, src_source]) -%}
{%- set rank_cols = dbtvault.expand_column_list(columns=[src_pk, src_hashdiff, src_ldts]) -%}
{%- set pk_cols = dbtvault.expand_column_list(columns=[src_pk]) -%}

{{ dbtvault.prepend_generated_by() }}

WITH source_DATA AS (
   SELECT {{ dbtvault.prefix (source_cols, 'a', alias_target='source') }}
    FROM {{ ref (source_model) }} as a
   WHERE {{ dbtvault.multikey(src_pk, condition='IS NOT NULL') }}
   {%- if is_incremental() and incremental_ts != None %}
     AND {{ incremental_ts }} > (select nvl(max({{ incremental_ts }}), to date('01/01/1900','mm/dd/yyyy')) from {{ this }})
   {% endif %}


{%- if is_incremental() and incremental_ts != None %}

, TARGET_LOOKUP AS
(
SELECT --Retun only Key column from Target
       DISTINCT
       TGT.{{ src_npk| replace("\"","") }}
     , TGT.{{ src_pk|replace("\"", "") }}
     , {%- if src_hashdiff is mapping -%}
       TGT.{{ src_hashdiff.source_column|replace("\"" ,"") }}
       {%- else %}
       TGT.{{ src_hashdiff|replace("\"" ,"") }}
       {%-  endif %}
     , TGT.{{ src_eff| replace("\"","") }}
  FROM {{ this }} TGT JOIN
       source_DATA SRC
    ON SRC.{{ src_pk }} = TGT.{{ src_pk }}
 WHERE TGT.{{ src_exp }} = TO DATE('12/31/9999', 'MM/DD/YVVY')
) 
{% endif %}

, SOURCE_AND_TARGET_COMBINED AS
(
SELECT {{ dbtvault.prefix(source_cols, 'a', alias_target='source') }}
FROM source_data a
{%- if is_incremental() and incremental_ts != None %}
UNION
SELECT {%- for col in source_cols %}
       {%- if col == src_pk or col == src_npk or col == src_eff %}
          {{ col }} as {{ col }}
       {%- else %}
          {%- if col is mapping and col != src_hashdiff -%}
             null as {{ col.source_column }}
          {%- elif col is mapping and col == src_hashdiff -%}
             {{ col.source_column }} as {{ col.source_column }}
          {%- else %}
             null as {{ col }}
          {%- endif %}
       {%- endif %}
       {%- if not loop.last %},
       {%- endif %}
{%- endfor %}
FROM TARGET LOOKUP
{% endif %}
)
, delta_filter AS
(
   --Only send from incoming data
   --1) If there is a Change in Payload between consecutive rows
   --or
   --2) Brand new record
   SELECT *
   FROM (
        SELECT *
             , LAG({{ src_hashdiff.source_column }}) OVER (PARTITION BY {{ src_pk }} ORDER BY {{ src_eff }}) as PREV_{{ src_hashdiff.source_column |replace("\"", "")}}
          FROM SOURCE_AND_TARGET_COMBINED
        )
WHERE ( {{ src_hashdiff.source_column }} <> PREV_{{ src_hashdiff.source_column |replace("\"", "")}}
   OR (PREV_{{ src_hashdiff.source_column |replace("\"", "")}} IS NULL AND {{ src_exp }} IS NULL)
     )
)
SELECT {%- for col in source_cols %}
       {%- if col == src_exp %}
       NVL(LEAD( {{ src_eff }} ) OVER(PARTITION BY {{ src_pk }} ORDER BY {{ src_eff }} -1, {{ src_exp }} as {{ src_exp }}
       {%- elif col is mapping -%}
       {{ col.source_column }} as {{ col.source_column }}
       {%- else %}
       {{ col }} as {{ col }}
       {%- endif %}
       {%- if not loop.last %},
       {%- endif %}
       {%- endfor %}
FROM delta_filter

{%- endmacro -%}
