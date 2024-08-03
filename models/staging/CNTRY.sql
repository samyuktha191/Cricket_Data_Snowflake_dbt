
{{ config(materialized='table') }}

select 
    raw.info:match_type_number::int as match_type_number, 
    k.key::text as country,
    v.value::text as player_name
from {{ ref('CRICMATCH_RAW_TBL') }} raw ,
lateral flatten (input=>raw.info:players) k,
lateral flatten (input=>k.value) v  

/*select 
    raw.info:match_type_number::int as match_type_number,
    p.key::text as country
FROM {{ ref('CRICMATCH_RAW_TBL') }} raw,
lateral flatten (input=>raw.info:players) p  */