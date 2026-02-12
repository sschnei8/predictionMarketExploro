import duckdb

con = duckdb.connect()

print("\n=== Query 7: SPORTS ===")
result7 = con.execute("""
WITH CHECK123 AS (
    SELECT CASE WHEN EVENT_TICKER ILIKE '%NCAA%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXHEISMAN%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXPSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXLSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%NFL%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFCONGAME%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXNFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%KXTEAMSINSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%NBA%' THEN 'NBA'
                WHEN EVENT_TICKER ILIKE '%BOXING%' THEN 'BOXING'
                WHEN EVENT_TICKER ILIKE '%KXUFCFIGHT%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%NHL%' THEN 'NHL'
                WHEN EVENT_TICKER ILIKE '%MLB%' THEN 'MLB'
                WHEN EVENT_TICKER ILIKE '%KXMARMAD%' THEN 'March Madness'
                WHEN EVENT_TICKER ILIKE '%KXWMARMAD%' THEN 'March Madness'
                WHEN EVENT_TICKER ILIKE '%KXWMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXWTAMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXUSOPEN%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXUSOWOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXUSOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXFOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXWWOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXFOMEN%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXAOMEN%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXDAVISCUPMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPDOUBLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPCHALLENGERMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXMASTERS%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXTHEOPEN%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPGA%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPREMIERLEAGUE%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEPL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUEFACL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXESPSUPERCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSAUDIPLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLALIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEUROLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGUE1GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBUNDESLIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXFACUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCLUBWCGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSERIEAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEFLCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXFIFAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEFLCHAMPIONSHIPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCOPADELREYGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXMLSGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEUROCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUNITEDCUPMATCH%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGAMXGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUCL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXMVESPORTSMULTIGAMEEXTENDED%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXUELGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCSGOGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXFOWOMENSINGLES%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXINDY500%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXF1%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXMENWORLDCUP%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXNBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXALEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXKBLGAME%' THEN 'Other Sports'
                ELSE 'LIKELY NOT SPORTS' END AS MKT_TYPE
        , 34087000000::FLOAT AS NET_TOTAL_VOLUME
        , SUM(volume) as total_volume_per_market
        , COUNT(1) as NUM_MKTS
                    
               
                      
    FROM 'kalshi_markets.parquet'
    GROUP BY 1,2
    ORDER BY total_volume_per_market DESC
)
                      
SELECT MKT_TYPE
     , total_volume_per_market::DECIMAL AS total_volume_per_market
     , total_volume_per_market::FLOAT / NET_TOTAL_VOLUME AS VOLUME_PCT
FROM CHECK123 
""").df()
print(result7)

print("\n=== Query 8: SPORTS ===")
result8 = con.execute("""
COPY (
WITH CHECK123 AS (
    SELECT EVENT_TICKER
         , CASE WHEN EVENT_TICKER ILIKE '%NCAA%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXHEISMAN%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXPSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXLSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%NFL%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFCONGAME%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXNFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%KXTEAMSINSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%NBA%' THEN 'NBA'
                WHEN EVENT_TICKER ILIKE '%BOXING%' THEN 'BOXING'
                WHEN EVENT_TICKER ILIKE '%KXUFCFIGHT%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%NHL%' THEN 'NHL'
                WHEN EVENT_TICKER ILIKE '%MLB%' THEN 'MLB'
                WHEN EVENT_TICKER ILIKE '%KXMARMAD%' THEN 'March Madness'
                WHEN EVENT_TICKER ILIKE '%KXWMARMAD%' THEN 'March Madness'
                WHEN EVENT_TICKER ILIKE '%KXWMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXWTAMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXUSOPEN%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXUSOWOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXUSOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXFOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXWWOMENSINGLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXFOMEN%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXAOMEN%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXDAVISCUPMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPDOUBLES%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPCHALLENGERMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXMASTERS%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXTHEOPEN%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPGA%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPREMIERLEAGUE%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEPL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUEFACL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXESPSUPERCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSAUDIPLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLALIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEUROLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGUE1GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBUNDESLIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXFACUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCLUBWCGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSERIEAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEFLCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXFIFAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEFLCHAMPIONSHIPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCOPADELREYGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXMLSGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEUROCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUNITEDCUPMATCH%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGAMXGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUCL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXMVESPORTSMULTIGAMEEXTENDED%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXUELGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCSGOGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXFOWOMENSINGLES%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXINDY500%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXF1%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXMENWORLDCUP%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXNBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXALEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXKBLGAME%' THEN 'Other Sports'
                ELSE 'LIKELY NOT SPORTS' END AS MKT_TYPE
        , 34087000000::FLOAT AS NET_TOTAL_VOLUME
        , SUM(volume) as total_volume_per_market
        , COUNT(1) as NUM_MKTS
                    
               
                      
    FROM 'kalshi_markets.parquet'
    GROUP BY 1,2,3
)
                      
SELECT EVENT_TICKER
     , total_volume_per_market
FROM CHECK123 
WHERE MKT_TYPE = 'LIKELY NOT SPORTS'
ORDER BY total_volume_per_market DESC 
LIMIT 1000) TO non_sports.csv (HEADER, DELIMITER ',');
""").df()
print(result8)



con.close()