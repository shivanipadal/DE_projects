{{ config(materialized='table') }}


 select 
    m.id,
    m.matchbtw,
    m.city,
    m.date,
    m.player_of_match,
    m.venue,
    m.neutral_venue,
    m.team1,
    m.team2,
    m.toss_winner,
    m.toss_decision,
    m.winner,
    m.result,
    m.is_toss_winner_winner,
    extract(year from date) as year,
	r.total_runs_by_both_team,
    


from {{ ref('stg_ipl_match') }}  m 
left outer join 
{{ ref('stg_no_of_runs') }}  r
on m.id=r.id