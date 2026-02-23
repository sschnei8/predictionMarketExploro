import duckdb

con = duckdb.connect()

print("\n === === === === Volume distribution === === === ===\n")
con.sql("""
COPY (
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
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_markets.parquet'
    GROUP BY 1
    ORDER BY NUMBER_OF_MARKETS DESC
) TO mkt_band_data.csv (HEADER, DELIMITER ',');    
""")

print("\n === === === === Volume distribution YES/NO === === === ===\n")
con.sql("""
COPY (
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
        , RESULT

        , COUNT(1) AS NUMBER_OF_MARKETS
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_markets.parquet'
    WHERE result <> '' -- Remove active/mkts that ended in tie/no resolution
      AND result <> 'scalar'
    GROUP BY 1,2
)
SELECT VOLUME_BANDS    
    , RESULT
    , NUMBER_OF_MARKETS 
     , SUM(NUMBER_OF_MARKETS) OVER (PARTITION BY VOLUME_BANDS) AS TOTAL_PER_BAND
    , NUMBER_OF_MARKETS::FLOAT / TOTAL_PER_BAND AS RATIO_PCT
FROM CREATE_BANDS
                      ORDER BY 1,2 DESC
) TO yes_no_mkt_band_data.csv (HEADER, DELIMITER ',');   

""")

con.sql("""
COPY (
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
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_markets.parquet'
    GROUP BY 1
    ORDER BY NUMBER_OF_MARKETS DESC
) TO mkt_band_data.csv (HEADER, DELIMITER ',');    
""")

print("\n === === === === Volume distribution YES/NO === === === ===\n")
con.sql("""
SELECT COUNT(1) AS NUM_MAKRETS
FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_markets.parquet'
WHERE result = 'scalar'
  AND status = 'finalized'
""").show()