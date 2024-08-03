/*DELIVERY_CLEAN_TBL */
{{ config(materialized='table') }}
SELECT
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name ,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total ,
	d.value:wickets[0].fielders[0].name::text as fielders_name,
    d.value:wickets[0].kind::text as kind_of_out,
    d.value:wickets[0].player_out::text as player_out,
	e.key::text as extra_type,
    e.value::number as extra_runs,
    w.value:player_out::text as palyer_out,
    w.value:kind::text as palyer_out_kind,
    w.value:fielders::text as palyer_out_fielders,
    
    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
FROM {{ ref('CRICMATCH_RAW_TBL') }} m,
lateral flatten (input=>m.innings) i,
lateral flatten (input=>i.value:overs) o,
lateral flatten (input=>o.value:deliveries) d,
lateral flatten (input=>d.value:extras,outer => TRUE) e,
lateral flatten (input=>d.value:wickets,outer => TRUE) w