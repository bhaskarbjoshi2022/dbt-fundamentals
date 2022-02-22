{#    
     To run this macro as an operation as a dry run, just use the following command:
        dbt run-operation create_file_format --args '{database_name: my_database, schema_name: my_schema, file_format_name: my_file_format}'

     To fully execute this macro and run the commands in snowflake, use the following command instead:
        dbt run-operation create_file_format --args '{database_name: my_database, schema_name: my_schema, file_format_name: my_file_format, dry_run: False}'

    Read more about running macros as operations here:
        https://docs.getdbt.com/reference/commands/run-operation/

    Args:
        - dry_run -- Default to True - dry run will output the SQL to the logs but won't execute any of it.
        - database_name -- Name of the Database in Snowflake where the file format will be created.
        - schema_name -- Name of the Schema in Snowflake where the file format will be created.
        - file_format_name -- Name of the file format to be created in Snowflake.
#}
{% macro create_file_format(database_name, schema_name, file_format_name, dry_run=True) %}
    
    {% set file_format_query %}
    
        CREATE OR REPLACE FILE FORMAT {{database_name}}.{{schema_name}}.{{file_format_name}}
        TYPE = 'CSV'
        COMPRESSION = 'AUTO'
        FIELD_DELIMITER = ','
        RECORD_DELIMITER = '\n'
        SKIP_HEADER = 0
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
        TRIM_SPACE = FALSE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        ESCAPE = 'NONE'
        ESCAPE_UNENCLOSED_FIELD = '\134'
        DATE_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'AUTO'
        NULL_IF = ('');
    {% endset %}
   
    {% if dry_run %}
        {% do log('Dry run of creation of file format :\n' ~ file_format_query, True) %}
    {% else %}          
        {% do run_query(file_format_query) %}        
    {% endif %}
{% endmacro %}