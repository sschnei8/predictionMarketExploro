import duckdb

con = duckdb.connect()

# 'ticker'
# 'event_ticker'
# 'result'
# 'status'
# 'volume'
# 'open_time'
# 'close_time'
# 'liquidity'
# 'market_type' - Always Binary

print("\n===  Query 1: Total Volume by Market Type ===")
result1 = con.execute("""
    SELECT
        market_type,
        COUNT(*) as market_count,
        SUM(volume) as total_volume
    FROM 'kalshi_markets.parquet'
    GROUP BY market_type
    ORDER BY total_volume DESC
""").df()
print(result1)


print("\n=== Query 2: Markets Over Time ===")
result2 = con.execute("""
    SELECT
        date_trunc('MONTH', OPEN_TIME::TIMESTAMP)::DATE AS open_month,
        COUNT(*) as markets_opened,
        SUM(volume) as total_volume
    FROM 'kalshi_markets.parquet'
    GROUP BY open_month
    ORDER BY open_month ASC
""").df()
print(result2)

print("\n=== Query 3: Results ===")
result3 = con.execute("""
    SELECT
        ticker,
        event_ticker,
        open_time,
        close_time,
        SUM(volume) as total_volume
    FROM 'kalshi_markets.parquet'
    GROUP BY 1,2,3,4
    ORDER BY total_volume DESC
    LIMIT 10
""").df()
print(result3)

print("\n === === === === Volume distribution === === === ===\n")
result4 = con.execute("""
    SELECT
        CASE WHEN volume = 0 THEN 'a. 0 volume'
             WHEN volume > 0 AND volume < 1000 THEN 'b. <1000 volume'
             WHEN volume >= 1000 AND volume < 10000 THEN 'c. 1000-10,000 volume'
             WHEN volume >= 10000 AND volume < 100000 THEN 'd. 10,000-100,000 volume'
             WHEN volume >= 100000 AND volume < 1000000 THEN 'e. 100,000-1,000,000 volume'
             WHEN volume >= 1000000 AND volume < 10000000 THEN 'f. 1,000,000-10,000,000 volume'
             WHEN volume >= 10000000  THEN 'g. >= 10,000,000 volume'
            ELSE 'WTF' END AS VOLUME_BANDS
        , COUNT(1) AS NUMBER_OF_MARKETS
        --, ratio_to_report(NUMBER_OF_MARKETS) OVER(PARTITION BY VOLUME_BANDS) AS RESULT_PCT
    FROM 'kalshi_markets.parquet'
    GROUP BY 1
    ORDER BY NUMBER_OF_MARKETS DESC
""").df()
print(result4)

print("\n=== Query 5: MKT NO RESULT ===")
result5 = con.execute("""
    SELECT *
    FROM 'kalshi_markets.parquet'
    WHERE result = ''
      AND volume >= 10000000

""").df()
print(result5)

print("\n=== Query 6: Resolving Ratio ===")
result6 = con.execute("""
WITH CREATE_BANDS AS (
    SELECT   
            CASE WHEN volume = 0 THEN 'a. 0 volume'
             WHEN volume > 0 AND volume < 1000 THEN 'b. <1000 volume'
             WHEN volume >= 1000 AND volume < 10000 THEN 'c. 1000-10,000 volume'
             WHEN volume >= 10000 AND volume < 100000 THEN 'd. 10,000-100,000 volume'
             WHEN volume >= 100000 AND volume < 1000000 THEN 'e. 100,000-1,000,000 volume'
             WHEN volume >= 1000000 AND volume < 10000000 THEN 'f. 1,000,000-10,000,000 volume'
             WHEN volume >= 10000000  THEN 'g. >= 10,000,000 volume'
            ELSE 'WTF' END AS VOLUME_BANDS
        , result

        , COUNT(1) AS NUMBER_OF_MARKETS
    FROM 'kalshi_markets.parquet'
    WHERE result <> '' -- Remove active/mkts that ended in tie/no resolution
    GROUP BY 1,2
)
SELECT VOLUME_BANDS    
    , RESULT
    , NUMBER_OF_MARKETS 
     , SUM(NUMBER_OF_MARKETS) OVER (PARTITION BY VOLUME_BANDS) AS TOTAL_PER_BAND
    , NUMBER_OF_MARKETS::FLOAT / TOTAL_PER_BAND AS RATIO_PCT
FROM CREATE_BANDS
                      ORDER BY 1,2 DESC

""").df()
print(result6)


print("\n=== Query 7: SPORTS ===")
result7 = con.execute("""
    SELECT CASE WHEN EVENT_TICKER ILIKE '%KXMVESPORTSMULTIGAMEEXTENDED%' THEN 'Parlay'
                WHEN EVENT_TICKER ILIKE '%NCAA%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXHEISMAN%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXPSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXLSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXGAMEDAYMENTION%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%NFL%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFCONGAME%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXNFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXTRUMPFOOTBALL%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%SUPERBOWL%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%KXTEAMSINSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%NBA%' THEN 'NBA'
                WHEN EVENT_TICKER ILIKE '%BOXING%' THEN 'BOXING'
                WHEN EVENT_TICKER ILIKE '%KXUFCFIGHT%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCMOV%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCROUNDS%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCVICROUND%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCTITLE%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXTRUMPUFC%' THEN 'UFC'
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
                WHEN EVENT_TICKER ILIKE '%KXWTACHALLENGERMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPEXACTMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXMASTERS%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXTHEOPEN%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXLIVTOUR%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXTGLMATCH%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPGA%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPREMIERLEAGUE%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDENSUPERLIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEKSTRAKLASAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSCOTTISHPREMGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXKLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSWISSLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXHNLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXKNVBCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBUNDESLIGA2GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDIMAYORGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLALIGA2GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUECLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXARGPREMDIVGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBELGIANPLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDFBPOKALGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXJLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXINTLFRIENDLYGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXFRASUPERCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCOUPEDEFRANCEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCOPPAITALIAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGAPORTUGALGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXTACAPORTGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSUPERLIGGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEPL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUEFACL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXESPSUPERCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXINTERCONCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSAUDIPLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLALIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEUROLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGUE1GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBUNDESLIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXKNVBCUPGAME%' THEN 'Soccer'
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
                WHEN EVENT_TICKER ILIKE '%KXBRASILEIROGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSLGREECEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEREDIVISIEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDOTA2GAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCODGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXUELGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCSGOGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXLEAGUEWORLDS%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXTORONTOULTRACHAMPIONSHIP%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXVALORANTGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXLOLGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXLOLMAP%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCS2GAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXFOWOMENSINGLES%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXINDY500%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXF1%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXMENWORLDCUP%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXNBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXALEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXKBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%COACH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%HOCKEY%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%CURL%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXWO-GOLD-26%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXT20MATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXT20WORLDCUP%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%CHESS%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCRICKETT20IMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXNASCAR%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXFIBACHAMPLEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCBAGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXLNBELITEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXJBLEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXFIBAECUPGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXBSLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXSIXNATIONSMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCRICKETTESTMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCRICKETODIMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%REESKI%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXBBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXABAGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXVTBGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXWPLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXBBSERIEAGAME%' THEN 'Other Sports'
                ELSE 'LIKELY NOT SPORTS' END AS MKT_TYPE
        , 34087000000::FLOAT AS NET_TOTAL_VOLUME
        , SUM(volume) as total_volume_per_market
        , COUNT(1) as NUM_MKTS
                    
               
                      
    FROM 'kalshi_markets.parquet'
    GROUP BY 1,2
    ORDER BY total_volume_per_market DESC
""").df()
print(result7)


print("\n=== Query 8: $0 markets ===")
result8 = con.execute("""
    -- Vast majority of $0 volume markets are Parlays because each parlay creates a market. Others are weird stuff + events that haven't occurred yet
    SELECT EVENT_TICKER,
        CASE WHEN EVENT_TICKER ILIKE '%KXMVE%' THEN 'Parlay'
                WHEN EVENT_TICKER ILIKE '%NCAA%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXHEISMAN%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXPSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXLSUCOACH%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%KXGAMEDAYMENTION%' THEN 'NCAA'
                WHEN EVENT_TICKER ILIKE '%NFL%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFCONGAME%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXAFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXNFC%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXTRUMPFOOTBALL%' THEN 'NFL'
                WHEN EVENT_TICKER ILIKE '%KXSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%SUPERBOWL%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%KXTEAMSINSB%' THEN 'SuperBowl'
                WHEN EVENT_TICKER ILIKE '%NBA%' THEN 'NBA'
                WHEN EVENT_TICKER ILIKE '%BOXING%' THEN 'BOXING'
                WHEN EVENT_TICKER ILIKE '%KXUFCFIGHT%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCMOV%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCROUNDS%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCVICROUND%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXUFCTITLE%' THEN 'UFC'
                WHEN EVENT_TICKER ILIKE '%KXTRUMPUFC%' THEN 'UFC'
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
                WHEN EVENT_TICKER ILIKE '%KXWTACHALLENGERMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXATPEXACTMATCH%' THEN 'Tennis'
                WHEN EVENT_TICKER ILIKE '%KXMASTERS%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXTHEOPEN%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXLIVTOUR%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXTGLMATCH%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPGA%' THEN 'Golf'
                WHEN EVENT_TICKER ILIKE '%KXPREMIERLEAGUE%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDENSUPERLIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEKSTRAKLASAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSCOTTISHPREMGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXKLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSWISSLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXHNLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXKNVBCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBUNDESLIGA2GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDIMAYORGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLALIGA2GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUECLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXARGPREMDIVGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBELGIANPLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDFBPOKALGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXJLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXINTLFRIENDLYGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXFRASUPERCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCOUPEDEFRANCEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXCOPPAITALIAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGAPORTUGALGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXTACAPORTGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSUPERLIGGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEPL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXUEFACL%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXESPSUPERCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXINTERCONCUPGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSAUDIPLGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLALIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEUROLEAGUEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXLIGUE1GAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXBUNDESLIGAGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXKNVBCUPGAME%' THEN 'Soccer'
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
                WHEN EVENT_TICKER ILIKE '%KXBRASILEIROGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXSLGREECEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXEREDIVISIEGAME%' THEN 'Soccer'
                WHEN EVENT_TICKER ILIKE '%KXDOTA2GAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCODGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXUELGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCSGOGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXLEAGUEWORLDS%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXTORONTOULTRACHAMPIONSHIP%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXVALORANTGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXLOLGAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXLOLMAP%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXCS2GAME%' THEN 'E-Sports'
                WHEN EVENT_TICKER ILIKE '%KXFOWOMENSINGLES%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXINDY500%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXF1%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXMENWORLDCUP%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXNBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXALEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXKBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%COACH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%HOCKEY%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%CURL%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXWO-GOLD-26%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXT20MATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXT20WORLDCUP%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%CHESS%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCRICKETT20IMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXNASCAR%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXFIBACHAMPLEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCBAGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXLNBELITEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXJBLEAGUEGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXFIBAECUPGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXBSLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXSIXNATIONSMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCRICKETTESTMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXCRICKETODIMATCH%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%REESKI%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXBBLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXABAGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXVTBGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXWPLGAME%' THEN 'Other Sports'
                WHEN EVENT_TICKER ILIKE '%KXBBSERIEAGAME%' THEN 'Other Sports'
                ELSE 'LIKELY NOT SPORTS' END AS MKT_TYPE
      , COUNT(1) as num_markets
    FROM 'kalshi_markets.parquet'
    WHERE volume = 0
      AND MKT_TYPE = 'LIKELY NOT SPORTS'
    GROUP BY 1,2
""").df()
print(result8)

con.close()

