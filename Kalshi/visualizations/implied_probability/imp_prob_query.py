import duckdb

con = duckdb.connect()

#print("\n===  Query 1: Implied prob by month avg/median ===")
#result1 = con.execute("""
#    SELECT
#        date_trunc('MONTH', created_time::TIMESTAMP)::DATE AS MONTH,
#        MEDIAN(yes_price_dollars::FLOAT) median_yes_unweighted,
#        SUM(yes_price_dollars::FLOAT * count)/ sum(count) AS avg_yes_price,
#        SUM(count) total_contracts,
#    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
#    GROUP BY MONTH
#    ORDER BY MONTH ASC
#""").df()
#print(result1)

print("\n===  Query 2: bucketed prob by month ===")
con.execute("""
COPY(
WITH FILTER_BAND AS (
    SELECT
             date_trunc('MONTH', created_time::TIMESTAMP)::DATE AS MONTH 
           , CASE
               WHEN yes_price_dollars::FLOAT < 0.20 THEN 'a. 0-20%'
               WHEN yes_price_dollars::FLOAT < 0.40 THEN 'b. 20-40%'
               WHEN yes_price_dollars::FLOAT < 0.60 THEN 'c. 40-60%'
               WHEN yes_price_dollars::FLOAT < 0.80 THEN 'd. 60-80%'
               ELSE 'e. 80-100%' END AS implied_prod_band
            , SUM(count) total_contracts
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
    WHERE MONTH >= '2024-01-01'::DATE
    GROUP BY 1,2
    ORDER BY 1 DESC
)
SELECT  MONTH
      , implied_prod_band  
      , total_contracts
      , ROUND((total_contracts / sum(total_contracts) OVER(PARTITION BY MONTH)) * 100, 2) || '%' AS band_pct_by_month
FROM FILTER_BAND 

        ) TO imp_prob_data.csv (HEADER, DELIMITER ',');
""")

con.close()