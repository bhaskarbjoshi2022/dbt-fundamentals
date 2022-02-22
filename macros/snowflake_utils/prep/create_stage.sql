{#    
     To run this macro as an operation as a dry run, just use the following command:
        dbt run-operation create_stage --args '{database_name: my_database, schema_name: my_schema, stage_name: my_file_format, storage_integration: my_storage_integration, stage_url: my_url}'

     To fully execute this macro and run the commands in snowflake, use the following command instead:
        dbt run-operation create_stage --args '{database_name: my_database, schema_name: my_schema, stage_name: my_stage, storage_integration: my_storage_integration, stage_url: my_url, dry_run: False}'

    Read more about running macros as operations here:
        https://docs.getdbt.com/reference/commands/run-operation/

    Args:
        - dry_run -- Default to True - dry run will output the SQL to the logs but won't execute any of it.
        - database_name -- Name of the Database in Snowflake where the file format will be created.
        - schema_name -- Name of the Schema in Snowflake where the file format will be created.
        - stage_name -- Name of the file format to be created in Snowflake.
        - storage_ingegration -- Name of storage integration to be used for Stage creation.
        - stage_url -- URL of the object store of the Cloud Provider.
#}
{% macro create_stage(database_name, schema_name, stage_name, storage_ingegration, stage_url, dry_run=True) %}
    
    {% set stage_query %}
    
        CREATE OR REPLACE STAGE {{database_name}}.{{schema_name}}.{{stage_name}} URL = {{stage_url}};

    {% endset %}
   
    {% if dry_run %}
        {% do log('Dry run of creation of stage :\n' ~ stage_query, True) %}
    {% else %}          
        {% do run_query(stage_query) %}        
    {% endif %}
{% endmacro %}