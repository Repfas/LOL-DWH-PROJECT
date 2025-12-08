{{
  config(
    materialized='table',
    schema='dim'
  )
}}

SELECT DISTINCT
    gen_random_uuid() AS sk_match_id ,
    match_history,
    season,
    'year',
    game_length_min,
    CASE 
        WHEN b_result = 1 THEN TRUE
        WHEN b_result = 0 THEN False
    END AS "is_blue_team_win?",
    CASE 
        WHEN b_result = 1 THEN 'blue'
        WHEN b_result = 0 THEN 'red'
    END AS "winner_team"
FROM {{ ref('stg_match') }}
