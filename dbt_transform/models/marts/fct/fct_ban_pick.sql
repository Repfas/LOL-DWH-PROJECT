{{
  config(
    materialized='table',
    schema='fct'
  )
}}

WITH ban_pick AS (

    -- BAN 1
    SELECT 
        gen_random_uuid() AS sk_banpick_id,
        bp.sk_match_id,
        bp.sk_team_id,
        dc.sk_champion_id,
        bp.is_blue_team AS is_blue_side,
        1 AS ban_order
    FROM {{ ref('br_players_teams') }} AS bp
    JOIN {{ ref('stg_ban') }} AS b ON bp.match_history = b.match_history
    JOIN {{ ref('dim_champions') }} AS dc ON b.ban_1 = dc.champion_name

    UNION ALL

    -- BAN 2
    SELECT 
        gen_random_uuid(),
        bp.sk_match_id,
        bp.sk_team_id,
        dc.sk_champion_id,
        bp.is_blue_team,
        2 AS ban_order
    FROM {{ ref('br_players_teams') }} AS bp
    JOIN {{ ref('stg_ban') }} AS b ON bp.match_history = b.match_history
    JOIN {{ ref('dim_champions') }} AS dc ON b.ban_2 = dc.champion_name

    UNION ALL

    -- BAN 3
    SELECT 
        gen_random_uuid(),
        bp.sk_match_id,
        bp.sk_team_id,
        dc.sk_champion_id,
        bp.is_blue_team,
        3 AS ban_order
    FROM {{ ref('br_players_teams') }} AS bp
    JOIN {{ ref('stg_ban') }} AS b ON bp.match_history = b.match_history
    JOIN {{ ref('dim_champions') }} AS dc ON b.ban_3 = dc.champion_name

    UNION ALL

    -- BAN 4
    SELECT 
        gen_random_uuid(),
        bp.sk_match_id,
        bp.sk_team_id,
        dc.sk_champion_id,
        bp.is_blue_team,
        4 AS ban_order
    FROM {{ ref('br_players_teams') }} AS bp
    JOIN {{ ref('stg_ban') }} AS b ON bp.match_history = b.match_history
    JOIN {{ ref('dim_champions') }} AS dc ON b.ban_4 = dc.champion_name

    UNION ALL

    -- BAN 5
    SELECT 
        gen_random_uuid(),
        bp.sk_match_id,
        bp.sk_team_id,
        dc.sk_champion_id,
        bp.is_blue_team,
        5 AS ban_order
    FROM {{ ref('br_players_teams') }} AS bp
    JOIN {{ ref('stg_ban') }} AS b ON bp.match_history = b.match_history
    JOIN {{ ref('dim_champions') }} AS dc ON b.ban_5 = dc.champion_name
)

SELECT distinct *
FROM ban_pick
WHERE sk_champion_id IS NOT NULL
