{{ config(materialized='view') }}


select 
    id, sum(total_runs)  as total_runs_by_both_team
from {{ source('staging','ball_by_ball') }}
group by id
 