select 
    m.match_type_number as match_id,
    dd.DateID as DateID,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.team_name = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    round(sum(case when d.team_name = m.first_team then  d.extras else 0 end )) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    round((sum(case 
            when d.team_name = m.first_team 
            then  d.runs else 0 end ) + 
            sum(case 
                    when d.team_name = m.first_team 
                    then  d.extra_runs else 0 end ) 
                    )) as total_runs_scored_BY_TEAM_A,
    sum(case 
            when d.team_name = m.first_team and d.player_out is not null 
            then  1 else 0 end 
            ) as wicket_lost_by_team_a,    
    
    max(case when d.team_name = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    round(sum(case when d.team_name = m.second_team then  d.extras else 0 end )) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (round(sum(case when d.team_name = m.second_team then  d.runs else 0 end ) + sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end )) ) as total_runs_scored_by_team_b,
    sum(case when d.team_name = m.second_team and d.player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.MATCH_RESULT as match_result,
    mw.team_id as winner_team_id

FROM {{ ref('MATCH_DETAIL_CLEAN')}} m
    join {{ ref('DATE_DIM')}} dd on m.event_date = dd.fulldate
    join {{ ref('TEAM_DIM')}} ftd on m.first_team = ftd.team_name 
    join {{ ref('TEAM_DIM')}} std on m.second_team = std.team_name 
    join {{ ref('MATCH_TYPE_DIM')}} mtd on m.match_type = mtd.match_type
    join {{ ref('VENUE_DIM')}} vd on m.city = vd.city
    join {{ ref('DELIVERY_CLEAN_TBL')}} d  on d.match_type_number = m.match_type_number 
    join {{ ref('TEAM_DIM')}} tw on m.toss_winner = tw.team_name 
    join {{ ref('TEAM_DIM')}} mw on m.winner= mw.team_name 
    group by
        m.match_type_number,
        DateID,
        referee_id,
        first_team_id,
        second_team_id,
        match_type_id,
        venue_id,
        total_overs,
        toss_winner_team_id,
        toss_decision,
        match_result,
        winner_team_id