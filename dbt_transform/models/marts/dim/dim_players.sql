{{
  config(
    materialized='table',
    schema='dim'
  )
}}
WITH combined_players AS (
    SELECT 
        blue_top as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        blue_jungle as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        blue_middle as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        blue_adc as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        blue_support as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        red_top as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        red_jungle as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        red_middle as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        red_adc as player_username
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        red_support as player_username
    FROM {{ref('stg_match')}}
)
SELECT DISTINCT
    gen_random_uuid() AS sk_player_id,
    player_username
FROM combined_players
WHERE player_username is not null
