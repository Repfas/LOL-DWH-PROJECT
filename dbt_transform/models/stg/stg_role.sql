SELECT
    role_id,
    role 
from {{source('raw','role')}}