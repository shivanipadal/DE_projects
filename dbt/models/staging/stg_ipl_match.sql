{{ config(materialized='table') }}

select
    -- identifiers
    id,
    {{ dbt_utils.surrogate_key(['team1', 'team2']) }} as matchbtw,
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