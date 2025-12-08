{{
  config(
    materialized='table',
    schema='fct'
  )
}}

SELECT distinct
   gen_random_uuid() AS sk_draftpick_id,
   sk_match_id,
   sk_team_id,
   sk_player_id,
   sk_champion_id,
   sk_role_id,
   is_blue_team as is_blue_side
FROM 
    {{ref("br_players_teams")}} 

