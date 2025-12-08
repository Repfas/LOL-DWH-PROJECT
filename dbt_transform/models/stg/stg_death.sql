SELECT 
    "MatchHistory" AS match_history,
    "TeamColor" as team_color,
    "Time" as "time",
    "Victim",
    "Killer",
    "Assist_1",
    "Assist_2",
    "Assist_3",
    "Assist_4"
from {{source('raw','death_data')}}