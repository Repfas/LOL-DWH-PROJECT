SELECT 
    "MatchHistory" as match_history,
    "TeamColor" as team_color,
    "ban_1",
    "ban_2",
    "ban_3",
    "ban_4",
    "ban_5"
from {{source('raw','ban_data')}}