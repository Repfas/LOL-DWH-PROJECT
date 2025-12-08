{{
  config(
    materialized='table',
    schema='dim'
  )
}}

SELECT DISTINCT
    gen_random_uuid() AS sk_champion_id ,
    "champion_name" as champion_name,
    "class" AS champion_class,
    "playstyle" AS champion_playstyle,
    "champion_title" AS champion_desc
FROM {{ ref('stg_champ') }}
