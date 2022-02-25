{% materialization my_macro, adapter='snowflake' -%}

    {% set original_query_tag = set_query_tag() %}
    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database, type='table') -%}

  -- build model
  {% call statement('main') -%}
    {{ log("original_query_tag: " ~ original_query_tag) }}
    {{ log("identifier: " ~ identifier) }}
    {{ log("old_relation: " ~ old_relation) }}
    {{ log("target_relation: " ~ target_relation) }}
    {{ log("sql: " ~ sql) }}
    select current_date
  {%- endcall %}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}