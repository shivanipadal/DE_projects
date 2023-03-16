{{ config(materialized='view') }}

select
    -- identifiers
    id,
    case when lower(team1) > lower(team2)
         then concat(team2, ' vs ', team1)
    else 
        concat(team1, ' vs ', team2) 
    end as matchbtw,
    city,
    cast(date as date) as date,
    player_of_match,
    venue,
    neutral_venue,
    team1,
    team2,
    toss_winner,
    toss_decision,
    winner,
    result,
    case when toss_winner = winner 
    then 'yes'
    else
    'no'
    end as is_toss_winner_winner ,


    from {{ source('staging','match_data') }}