/*
    we configure models directly within SQL files.
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
{{ config(materialized='table') }}

SELECT
    info:match_type_number::int as match_type_number,
    info:event.name::text as event_name,
    case 
        when info:match_number::text is not null then info:event.number::text
        when info:event.stage::text is not null then info:event.stage::text
    else 'NA'
    end as match_stage,
    info:dates[0]::date as event_date,
    date_part('year',info:dates[0]::date) as event_year,
    date_part('month',info:dates[0]::date) as event_month,
    date_part('day',info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue,
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'NO Result'
    else info:outcome.result
    end as match_result,    
    case 
        when info:outcome.winner is not null then info:outcome.winner
    else 'NA'
    end as winner,
    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,
    info:officials.match_referees[0]::text as match_referees,
    info:officials.reserve_umpires[0]::text as reserve_umpires,
    info:officials.tv_umpires[0]::text as tv_umpires,
    info:officials.umpires[0]::text as first_umpire,
    info:officials.umpires[1]::text as second_umpire,
    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
    
FROM {{ ref('CRICMATCH_RAW_TBL') }}