{{ config(materialized='view') }}


select 
    id, inning, batting_team, bowling_team, sum(total_runs)  as total_runs
from {{ source('staging','ball_by_ball') }}
group by id, inning, batting_team, bowling_team
 