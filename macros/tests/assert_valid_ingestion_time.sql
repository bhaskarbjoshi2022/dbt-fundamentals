{% test assert_valid_ingestion_time(model, column_name) %}
 
    select {{ column_name }}
      from {{ model }}
     where not is_timestamp_tz( {{ column_name }}::variant )
        or {{ column_name }} > current_timestamp

{% endtest %}