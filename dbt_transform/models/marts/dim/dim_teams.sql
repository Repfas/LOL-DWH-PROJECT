{{
  config(
    materialized='table',
    schema='dim'
  )
}}
WITH combined_data_teams AS (
    SELECT 
        blue_team_tag as team_tag
    FROM {{ref('stg_match')}}
    UNION
    SELECT 
        red_team_tag as team_tag
    FROM {{ref('stg_match')}}
)
SELECT DISTINCT
    gen_random_uuid() AS sk_team_id,
    team_tag
FROM combined_data_teams
WHERE team_tag is not null

