import duckdb

con = duckdb.connect()

print("\n===  TOTAL FEE CALC QUERY ===")
result1 = con.execute("""
    SELECT ticker, count, yes_price_dollars
        -- TOTAL FEE FOR ANY CONTRACT IS .0875 * C * P(1=P) where we don't care which side maker/taker are on
         , .07 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS TAKER_FEE
         , .0175 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS MAKER_FEE
         ,  CAST(CEIL(TAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as TAKER_FEE_WC
         ,  CAST(CEIL(MAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as MAKER_FEE_WC
         ,  TAKER_FEE_WC + MAKER_FEE_WC AS TOTAL_FEE
         -- , CASE WHEN TICKER ILIKE '%KXDOED%' THEN 'No Fee'
         --        WHEN TICKER ILIKE '%KXGAMBLINGREPEAL%' THEN 'No Fee'
         --        WHEN TICKER ILIKE '%KXGREENLAND%' THEN 'No Fee'
         --        WHEN TICKER ILIKE '%KXINX%' THEN 'Take Fee Half'
         --        WHEN TICKER ILIKE '%KXINXMAXY%' THEN 'Take Fee Half'                      
         --        WHEN TICKER ILIKE '%KXINXMINY%' THEN 'Take Fee Half'                      
         --        WHEN TICKER ILIKE '%KXINXPOS%' THEN 'Take Fee Half'
         --        WHEN TICKER ILIKE '%KXINXU%' THEN 'Take Fee Half'                      
         --        WHEN TICKER ILIKE '%KXNASDAQ100%' THEN 'Take Fee Half'
         --        WHEN TICKER ILIKE '%KXNASDAQ100U%' THEN 'Take Fee Half'                      
         --        WHEN TICKER ILIKE '%KXINXY%' THEN 'Maker & Take Half'
         --        WHEN TICKER ILIKE '%KXNASDAQ100Y%' THEN 'Maker & Take Half'                      
         --        ELSE 'NORMAL FEES'
         --      END AS TICKER_FEE_SEGMENTATION
        -- , SUM(COUNT) AS TOTAL_CONTRACTS
 --        date_trunc('MONTH', created_time::TIMESTAMP)::DATE AS MONTH,
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
    LIMIT 10
""").df()
print(result1)

print("\n===  TOTAL FEE CALC QUERY ===")

result2 = con.sql("""
    --TOTAL = 545,213,087.0606397
    SELECT SUM(.0875 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT)) AS TOTAL_FEE
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
""").show()

print("\n===  TOTAL FEE CALC QUERY WITH PROPER ROUNDING ===")

con.sql("""
WITH TRADE_FEE AS (
    -- TOTAL = 547,221,252.78 
    SELECT ticker, count, yes_price_dollars
        -- TOTAL FEE FOR ANY CONTRACT IS .0875 * C * P(1=P) where we don't care which side maker/taker are on
         , .07 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS TAKER_FEE
         , .0175 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS MAKER_FEE
         ,  CAST(CEIL(TAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as TAKER_FEE_WC
         ,  CAST(CEIL(MAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as MAKER_FEE_WC
         ,  TAKER_FEE_WC + MAKER_FEE_WC AS TOTAL_FEE
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
)
SELECT SUM(TOTAL_FEE)
FROM TRADE_FEE
                      
""").show()

print("\n===  TOTAL FEE CALC QUERY WITH PROPER ROUNDING AND HANDLING OF FEE ADJUSTMENTS https://kalshi.com/fee-schedule ===")

con.sql("""
WITH TRADE_FEE AS (
    -- TOTAL = 545,673,949.01
    SELECT ticker, count, yes_price_dollars
        -- TOTAL FEE FOR ANY CONTRACT IS .0875 * C * P(1=P) where we don't care which side maker/taker are on
         , .07 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS TAKER_FEE
         , .0175 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS MAKER_FEE
         ,  CAST(CEIL(TAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as TAKER_FEE_WC
         ,  CAST(CEIL(MAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as MAKER_FEE_WC
         ,  TAKER_FEE_WC + MAKER_FEE_WC AS TOTAL_FEE
         , CASE WHEN TICKER ILIKE '%KXDOED%' THEN 0
                WHEN TICKER ILIKE '%KXGAMBLINGREPEAL%' THEN 0
                WHEN TICKER ILIKE '%KXGREENLAND%' THEN 0
                WHEN TICKER ILIKE '%KXINX%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))
                WHEN TICKER ILIKE '%KXINXMAXY%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))                     
                WHEN TICKER ILIKE '%KXINXMINY%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))                     
                WHEN TICKER ILIKE '%KXINXPOS%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))
                WHEN TICKER ILIKE '%KXINXU%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))                    
                WHEN TICKER ILIKE '%KXNASDAQ100%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))
                WHEN TICKER ILIKE '%KXNASDAQ100U%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))                  
                WHEN TICKER ILIKE '%KXINXY%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))   
                WHEN TICKER ILIKE '%KXNASDAQ100Y%' THEN CAST(CEIL((TAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))                    
                ELSE TAKER_FEE_WC
                END AS TAKER_FEE_WITH_ADJ
         , CASE WHEN TICKER ILIKE '%KXDOED%' THEN 0
                WHEN TICKER ILIKE '%KXGAMBLINGREPEAL%' THEN 0
                WHEN TICKER ILIKE '%KXGREENLAND%' THEN 0                
                WHEN TICKER ILIKE '%KXINXY%' THEN CAST(CEIL((MAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))   
                WHEN TICKER ILIKE '%KXNASDAQ100Y%' THEN CAST(CEIL((MAKER_FEE / 2::FLOAT) * 100) / 100 AS DECIMAL(10, 2))                    
                ELSE MAKER_FEE_WC
                END AS MAKER_FEE_WITH_ADJ
         , TAKER_FEE_WITH_ADJ + MAKER_FEE_WITH_ADJ AS TOTAL_FEE_ADJ
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
)
SELECT SUM(TOTAL_FEE_ADJ)
FROM TRADE_FEE
                      
""").show()

print("\n===  LARGEST TOTAL FEE ===")

con.sql("""
WITH TRADE_FEE AS (
    -- ALL TRUMP HARRIS 1,000,000 contract trades with fees of 21735.00 and 21315.02
    SELECT ticker, count, yes_price_dollars
        -- TOTAL FEE FOR ANY CONTRACT IS .0875 * C * P(1=P) where we don't care which side maker/taker are on
         , .07 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS TAKER_FEE
         , .0175 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS MAKER_FEE
         ,  CAST(CEIL(TAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as TAKER_FEE_WC
         ,  CAST(CEIL(MAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as MAKER_FEE_WC
         ,  TAKER_FEE_WC + MAKER_FEE_WC AS TOTAL_FEE
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
)
SELECT * 
FROM TRADE_FEE
WHERE TOTAL_FEE > 20000
                      
""").show()

print("\n===  OVERALL TRADE IMPLIED ODDS FOR FEES ===")

con.sql("""
        -- We need to scale the yes price to either 0 - 0.5 or .5 - 1
        -- This is because we are indifferent whether odds are .2 or .8 but when we try and get an avg across all contracts they cancel eachother out 
        -- If you allow odds to exists between 0-1 you will never understand the true structure that is impacting fees.
        -- Here I force all odds < .5 to take the same odds as their counterpart, this will give us a good indication of how fees are derived between .5 and 1
WITH NORM AS (
SELECT CASE WHEN yes_price_dollars::FLOAT > .5 THEN 1-yes_price_dollars::FLOAT ELSE yes_price_dollars::FLOAT END AS IMP_ODDS_NORM
      , COUNT
FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
WHERE TICKER = 'PRES-2024-KH'
)
        
SELECT SUM(IMP_ODDS_NORM * COUNT)::FLOAT / SUM(COUNT) AS IMP_ODDS
FROM NORM
                      
""").show()

con.sql("""
WITH TOTALS AS (
    SELECT ticker, count, yes_price_dollars
        -- TOTAL FEE FOR ANY CONTRACT IS .0875 * C * P(1=P) where we don't care which side maker/taker are on
         , .07 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS TAKER_FEE
         , .0175 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS MAKER_FEE
         ,  CAST(CEIL(TAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as TAKER_FEE_WC
         ,  CAST(CEIL(MAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as MAKER_FEE_WC
         ,  TAKER_FEE_WC + MAKER_FEE_WC AS TOTAL_FEE
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
WHERE TICKER = 'PRES-2024-KH'
        )
SELECT *
FROM TOTALS
ORDER BY COUNT DESC
LIMIT 10
""").show()

#  0.23711489
#  3,433,294.57 
#  273,312,857 

# Whenever we use an agg prob the fee rev is 20% higher

#print("\n===  Query 2: bucketed prob by month ===")
#con.execute("""
#COPY(
#
#        ) TO fee_rev_data.csv (HEADER, DELIMITER ',');
#""")

con.close()