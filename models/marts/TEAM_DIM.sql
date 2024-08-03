
{{ config(materialized='table'
 ) }}


select ROW_NUMBER() OVER (ORDER BY a.team_name) AS team_id, 
a.team_name 
from (
select first_team as team_name
from {{ ref('MATCH_DETAIL_CLEAN')}}
UNION all
SELECT second_team as team_name 
from {{ ref('MATCH_DETAIL_CLEAN')}}
) a 
GROUP BY a.team_name 
order by team_name


