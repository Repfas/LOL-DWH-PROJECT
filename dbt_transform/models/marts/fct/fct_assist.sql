{{
  config(
    materialized='table',
    schema='fct'
  )
}}

WITH assist_union AS (
    SELECT match_history, time, "Assist_1" AS assist_player
    FROM {{ ref("stg_death") }}
    UNION ALL
    SELECT match_history, time, "Assist_2" AS assist_player
    FROM {{ ref("stg_death") }}
    UNION ALL
    SELECT match_history, time, "Assist_3" AS assist_player
    FROM {{ ref("stg_death") }}
    UNION ALL
    SELECT match_history, time, "Assist_4" AS assist_player
    FROM {{ ref("stg_death") }}
),
assist_clean AS (
    SELECT
        match_history,
        time,
        TRIM(split_part(assist_player, ' ', 1)) AS assist_team_tag,
        TRIM(split_part(assist_player, ' ', 2)) AS assist_player_name
    FROM assist_union
    WHERE assist_player IS NOT NULL
      AND split_part(assist_player, ' ', 2) IS NOT NULL
),
assist_fact AS (
    SELECT DISTINCT
        bpt.sk_match_id,
        ac.match_history,
        ac.time AS event_time,
        bpt.sk_team_id,
        bpt.sk_player_id,
        bpt.sk_champion_id,
        bpt.sk_role_id
    FROM assist_clean ac
    JOIN {{ ref("br_players_teams") }} bpt
      ON ac.match_history = bpt.match_history
     AND LOWER(ac.assist_team_tag) = LOWER(bpt.team_tag)
     AND ac.assist_player_name = bpt.player_username
)
SELECT * FROM assist_fact