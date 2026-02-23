import duckdb

con = duckdb.connect()

print("\n===  TOTAL FEE CALC QUERY WITH PROPER ROUNDING AND HANDLING OF FEE ADJUSTMENTS https://kalshi.com/fee-schedule ===")

con.execute("""
COPY (
WITH TRADE_FEE AS (
    -- TOTAL = 545,673,949.01
    SELECT ticker
        , count
        , yes_price_dollars
        , created_time
        -- TOTAL FEE FOR ANY CONTRACT IS .0875 * C * P(1=P) where we don't care which side maker/taker are on
         , .07 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS TAKER_FEE
         , .0175 * COUNT * yes_price_dollars::FLOAT * (1-yes_price_dollars::FLOAT) AS MAKER_FEE
         ,  CAST(CEIL(TAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as TAKER_FEE_WC
         ,  CAST(CEIL(MAKER_FEE * 100) / 100 AS DECIMAL(10, 2)) as MAKER_FEE_WC
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
         , TAKER_FEE_WITH_ADJ + MAKER_FEE_WITH_ADJ AS TOTAL_FEE
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
)
SELECT date_trunc('MONTH', created_time::TIMESTAMP)::DATE AS MONTH
     , SUM(TOTAL_FEE) AS TOTAL_REV_FROM_FEES
FROM TRADE_FEE
WHERE MONTH >= '2024-01-01'
GROUP BY 1    
ORDER BY 1 DESC) TO total_fee_data.csv (HEADER, DELIMITER ',');                
""")

con.close()