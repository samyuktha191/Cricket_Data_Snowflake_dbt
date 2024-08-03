
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

select 
        x.$1:meta::variant as meta,
        x.$1:info::variant as info,
        x.$1:innings::variant as innings,
        metadata$filename as STG_FILE_NAME,
        metadata$file_row_number as STG_FILE_ROW_NUMBER,
        metadata$file_content_key  as STG_FILE_HASHKEY,
        metadata$file_last_modified as STG_MODIFIED_TS  

FROM @my_stg_land/cricket/json/1384430.json (file_format => 'my_json_format') x

)

select *
from source_data

/*
*/

