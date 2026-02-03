import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

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

print("\n=== Query 4: Volume distribution ===")
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
        , result

        , COUNT(1) AS NUMBER_OF_MARKETS
        --, ratio_to_report(NUMBER_OF_MARKETS) OVER(PARTITION BY VOLUME_BANDS) AS RESULT_PCT
    FROM 'kalshi_markets.parquet'
    GROUP BY 1,2
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

con.close()
