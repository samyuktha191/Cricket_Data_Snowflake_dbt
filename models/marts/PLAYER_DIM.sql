select 
ROW_NUMBER() OVER (ORDER BY team_id) AS player_id,team_id,player_name 
FROM (
SELECT b.team_id, a.player_name 
from {{ ref('PLAYER_CLEAN_TBL')}} a
join {{ ref('TEAM_DIM')}} b
on a.country=b.team_name
group by 1,2) a