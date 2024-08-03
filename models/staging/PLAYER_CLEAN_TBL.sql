
/*PLAYER_CLEAN_TBL */
{{ config(materialized='table') }}

select 
    raw.info:match_type_number::int as match_type_number, 
    k.key::text as country,
    v.value::text as player_name,

    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
    
from {{ ref('CRICMATCH_RAW_TBL') }} raw ,
lateral flatten (input=>raw.info:players) k,
lateral flatten (input=>k.value) v