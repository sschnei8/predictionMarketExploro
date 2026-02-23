import duckdb

con = duckdb.connect()

print("\n===  Overall Stats ===")

con.sql("""
COPY (
WITH TOTAL_TRADES_DATA AS (
    SELECT -- date_trunc('WEEK', created_time::TIMESTAMP)::DATE AS WEEK
        1 AS JOIN_NUM
         , COUNT(1) AS TOTAL_TRADES
         , SUM(count) AS TOTAL_VOLUME
         , TOTAL_VOLUME / TOTAL_TRADES::FLOAT AS AVG_CONTRACTS_PER_TRADE
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
   GROUP BY 1
),
TOTAL_MARKETS_DATA AS (
    SELECT 1 AS JOIN_NUM
        , COUNT(*) as MARKET_COUNT
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_markets.parquet'
    GROUP BY 1
)   
SELECT T.TOTAL_TRADES
     , T.TOTAL_VOLUME 
     , T.AVG_CONTRACTS_PER_TRADE
     , M.MARKET_COUNT
FROM TOTAL_TRADES_DATA T
       LEFT JOIN TOTAL_MARKETS_DATA M USING(JOIN_NUM)

) TO overall_stats_data.csv (HEADER, DELIMITER ',');                
""")

print("\n===  Overall Stats ===")

con.sql("""
COPY (
    SELECT date_trunc('WEEK', created_time::TIMESTAMP)::DATE AS WEEK
         , COUNT(1) AS TOTAL_TRADES
         , SUM(count) AS TOTAL_VOLUME
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
   WHERE WEEK > '2023-01-01'
   GROUP BY 1
   ORDER BY 1 DESC
) TO weekly_overall_stats_data.csv (HEADER, DELIMITER ',');                
""")

con.close()