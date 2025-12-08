SELECT 
    "champion_name",
    "champion_title",
    "class",
    "playstyle"
FROM 
    {{source('raw','champ')}}