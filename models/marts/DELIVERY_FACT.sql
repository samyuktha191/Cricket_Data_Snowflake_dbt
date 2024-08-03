select 
    d.match_type_number as match_id,
    td.team_id,
    bpd.player_id as bower_id, 
    spd.player_id batter_id, 
    nspd.player_id as non_stricker_id,
    d.over,
    d.runs,
    case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
    case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
    case when d.player_out is null then 'None' else d.player_out end as player_out,
    case when d.kind_of_out is null then 'None' else d.kind_of_out end as player_out_kind
from 
    {{ ref('DELIVERY_CLEAN_TBL')}} d
    join {{ ref('TEAM_DIM')}} td on d.team_name = td.team_name
    join {{ ref('PLAYER_DIM')}} bpd on d.bowler = bpd.player_name
    join {{ ref('PLAYER_DIM')}} spd on d.batter = spd.player_name
    join {{ ref('PLAYER_DIM')}} nspd on d.non_striker = nspd.player_name

where 
    d.match_type_number is not null
    and td.team_id is not null
    and bpd.player_id is not null
    and spd.player_id is not null
    and nspd.player_id is not null
    and d.over is not null
    and d.runs is not null