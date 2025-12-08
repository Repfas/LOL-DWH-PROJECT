{{
  config(
    materialized = 'table',
    schema = 'fct'
  )
}}

WITH combat_killer AS (
    SELECT 
        bpt.sk_match_id,
        bpt.match_history,
        sb.time,
        bpt.sk_team_id        AS killer_team_id,
        bpt.sk_player_id      AS killer_player_id,
        bpt.sk_champion_id    AS killer_champion_id,
        bpt.sk_role_id        AS killer_role_id,
        split_part(sb."Killer", ' ', 2) AS killer_name,
        split_part(sb."Killer", ' ', 1) AS killer_team_tag
    FROM {{ ref("br_players_teams") }} bpt
    JOIN {{ ref("stg_death") }} sb 
      ON bpt.match_history = sb.match_history
    WHERE LOWER(bpt.team_tag) = LOWER(split_part(sb."Killer", ' ', 1))
      AND bpt.player_username = split_part(sb."Killer", ' ', 2)
      AND split_part(sb."Killer", ' ', 2) IS NOT NULL
),

combat_victim AS (
    SELECT 
        bpt.sk_match_id,
        bpt.match_history,
        sb.time,
        bpt.sk_team_id        AS victim_team_id,
        bpt.sk_player_id      AS victim_player_id,
        bpt.sk_champion_id    AS victim_champion_id,
        bpt.sk_role_id        AS victim_role_id,
        split_part(sb."Victim", ' ', 2) AS victim_name,
        split_part(sb."Victim", ' ', 1) AS victim_team_tag
    FROM {{ ref("br_players_teams") }} bpt
    JOIN {{ ref("stg_death") }} sb 
      ON bpt.match_history = sb.match_history
    WHERE LOWER(bpt.team_tag) = LOWER(split_part(sb."Victim", ' ', 1))
      AND bpt.player_username = split_part(sb."Victim", ' ', 2)
      AND split_part(sb."Victim", ' ', 2) IS NOT NULL
)

SELECT DISTINCT
    k.sk_match_id,
    k.match_history,
    k.time AS event_time,
    -- killer side
    k.killer_team_id,
    k.killer_player_id,
    k.killer_champion_id,
    k.killer_role_id,
    -- victim side
    v.victim_team_id,
    v.victim_player_id,
    v.victim_champion_id,
    v.victim_role_id
FROM combat_killer k
JOIN combat_victim v 
  ON k.match_history = v.match_history
 AND k.time = v.time
