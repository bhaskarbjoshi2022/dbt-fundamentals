{{
  config(
    materialized='copy_into',
    transient = False,
    tags = ['high_freq']
  )
}}

SELECT T.$1::INT AS TRIPDURATION
     , T.$2::DATETIME AS STARTTIME
     , T.$3::DATETIME AS STOPTIME
     , T.$4::INT AS START_STATION_ID
     , T.$5::STRING AS START_STATION_NAME
     , T.$6::FLOAT AS START_STATION_LATITUDE
     , T.$7::FLOAT AS START_STATION_LONGITUDE
     , T.$8::INTEGER AS END_STATION_ID
     , T.$9::STRING AS END_STATION_NAME
     , T.$10::FLOAT AS END_STATION_LATITUDE
     , T.$11::FLOAT AS END_STATION_LONGITUDE
     , T.$12::INT AS BIKEID
     , T.$13::STRING AS MEMBERSHIP_TYPE
     , T.$14::STRING AS USERTYPE
     , T.$15::INT AS BIRTH_YEAR
     , T.$16::INT AS GENDER
FROM @{{ source('citi_bikes_manual', 'citibike_trips') }}
     (FILE_FORMAT => {{ ref('csv_file_format') }}) T