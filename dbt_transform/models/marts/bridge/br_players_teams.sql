{{
  config(
    materialized='table',
    schema='bridge'
  )
}}

WITH role_mapping AS (
  SELECT role, sk_role_id FROM {{ref('dim_role')}}
),

bridge_player AS (
  -- BLUE TEAM
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.blue_top as player_username,
    dp.sk_player_id,
    m.blue_top_champ as champion_use,
    dc.sk_champion_id,
    'top' AS player_role,
    rm.sk_role_id,
    m.blue_team_tag as team_tag,
    TRUE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.blue_top = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.blue_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'top'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.blue_top_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.blue_jungle as player_username,
    dp.sk_player_id,
    m.blue_jungle_champ as champion_use,
    dc.sk_champion_id,
    'jungle' AS player_role,
    rm.sk_role_id,
    m.blue_team_tag as team_tag,
    TRUE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.blue_jungle = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.blue_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'jungle'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.blue_jungle_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.blue_middle as player_username,
    dp.sk_player_id,
    m.blue_middle_champ as champion_use,
    dc.sk_champion_id,
    'middle' AS player_role,
    rm.sk_role_id,
    m.blue_team_tag as team_tag,
    TRUE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.blue_middle = dp.player_username 
  JOIN {{ref('dim_teams')}} dt ON m.blue_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'middle'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.blue_middle_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.blue_adc as player_username,
    dp.sk_player_id,
    m.blue_adc_champ as champion_use,
    dc.sk_champion_id,
    'adc' AS player_role,
    rm.sk_role_id,
    m.blue_team_tag as team_tag,
    TRUE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.blue_adc = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.blue_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'adc'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.blue_adc_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.blue_support as player_username,
    dp.sk_player_id,
    m.blue_support_champ as champion_use,
    dc.sk_champion_id,
    'support' AS player_role,
    rm.sk_role_id,
    m.blue_team_tag as team_tag,
    TRUE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.blue_support = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.blue_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'support'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.blue_support_champ
  
  -- RED TEAM
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.red_top as player_username,
    dp.sk_player_id,
    m.red_top_champ as champion_use,
    dc.sk_champion_id,
    'top' AS player_role,
    rm.sk_role_id,
    m.red_team_tag as team_tag,
    FALSE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.red_top = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.red_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'top'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.red_top_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.red_jungle as player_username,
    dp.sk_player_id,
    m.red_jungle_champ as champion_use,
    dc.sk_champion_id,
    'jungle' AS player_role,
    rm.sk_role_id,
    m.red_team_tag as team_tag,
    FALSE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.red_jungle = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.red_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'jungle'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.red_jungle_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.red_middle as player_username,
    dp.sk_player_id,
    m.red_middle_champ as champion_use,
    dc.sk_champion_id,
    'middle' AS player_role,
    rm.sk_role_id,
    m.red_team_tag as team_tag,
    FALSE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.red_middle = dp.player_username 
  JOIN {{ref('dim_teams')}} dt ON m.red_team_tag = dt.team_tag  
  JOIN role_mapping rm ON rm.role = 'middle'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.red_middle_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.red_adc as player_username,
    dp.sk_player_id,
    m.red_adc_champ as champion_use,
    dc.sk_champion_id,
    'adc' AS player_role,
    rm.sk_role_id,
    m.red_team_tag as team_tag,
    FALSE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.red_adc = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.red_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'adc'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.red_adc_champ
  
  UNION ALL
  
  SELECT 
    gen_random_uuid() AS bridge_player_id,
    m.match_history,
    dm.sk_match_id,
    dt.sk_team_id,
    m.red_support as player_username,
    dp.sk_player_id,
    m.red_support_champ as champion_use,
    dc.sk_champion_id,
    'support' AS player_role,
    rm.sk_role_id,
    m.red_team_tag AS team_tag,
    FALSE as is_blue_team
  FROM {{ref('stg_match')}} m 
  JOIN {{ref('dim_match')}} dm ON m.match_history = dm.match_history
  JOIN {{ref('dim_players')}} dp ON m.red_support = dp.player_username
  JOIN {{ref('dim_teams')}} dt ON m.red_team_tag = dt.team_tag 
  JOIN role_mapping rm ON rm.role = 'support'
  JOIN {{ref('dim_champions')}} dc ON dc.champion_name = m.red_support_champ
)

SELECT * FROM bridge_player