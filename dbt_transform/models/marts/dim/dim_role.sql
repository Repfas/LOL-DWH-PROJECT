{{
  config(
    materialized='table',
    schema='dim'
  )
}}

SELECT DISTINCT
    gen_random_uuid() AS sk_role_id ,
    role_id AS nk_role_id,
    role
FROM {{ ref('stg_role') }}
