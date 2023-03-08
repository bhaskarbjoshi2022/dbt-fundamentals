{% macro dbt_snowflake_validate_get_incremental_strategy(config)  %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get ("incremental_strategy", default="merge") -%}

  {% set invalid_strategy_msg -%}
   Invalid incremental strategy provided: {{ strategy }}
   Expected one of: 'merge', 'delete+insert', 'merge scd2'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert', 'merge_scd2'] %}
    {%do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return (strategy) %}
{% endmacro %}


{% macro dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  {%if strategy == 'merge' %}
    {% do return(get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
   {% do return(get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'merge_scd2' %}
   {% do return(get_merge_sql_scd2(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% else %}
   {% do exceptions.raise_compiler_error('invalid_strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}



{% macro get_merge_sql_scd2(target, source, unique_key, dest_columns, predicates=None) -%}
   {%- set predicates = [] if predicates is none else [] + predicates -%}
   {%- set dest_cols_csv = get_quoted_csv(dest_columns | map (attribute="name")) -%}
   {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map (attribute="quoted") | list) -%}
   {%- set sql_header = config.get('sql_header', none) -%}
   {%- set expire_scd2_columns = config.get('expire_scd2_ columns') -%}

   {% if unique_key %}
       {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {%endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}   
        {% else %}
           {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
           {% endset %}
           {% do predicates.append(unique_key_match) %}  
       {% endif %}
   {% else %}
      {% do predicates.append(' FALSE') %}
   {% endif %}

   {{ sql_header if sql_header is not none }}
   
   merge into {{ target }} as DBT_INTERNAL_DEST
   using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}

   {% if unique_key %}
   when matched then update set
      {% for col in expire_scd2_columns %}
         {{ col }} = DBT_INTERNAL_SOURCE.{{ col }}
         {% if not loop.last %},
         {% endif %}
      {% endfor %}
   {% endif %}
   when not matched then insert
      ({{ dest_cols_csv }})
   values
      ({{ dest_cols_csv }})
{% endmacro %}
