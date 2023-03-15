{{ config(materialized='view') }}

select
    -- identifiers
    id,
    concat(team1, ' vs ', team2) as matchbtw,
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
    result

    from {{ source('staging','match_data') }}