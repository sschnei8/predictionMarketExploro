import duckdb

con = duckdb.connect()

print("\n===  Query 1: Total Volume by Market Type ===")
result1 = con.execute("""
COPY (
    SELECT
        date_trunc('MONTH', created_time::TIMESTAMP)::VARCHAR(100) AS MONTH,
        SUM(count)::FLOAT / COUNT(*) AS contracts_per_trade,
        MEDIAN(count) median_contracts_per_trade
    FROM '/Users/iamsam/WorkingFiles/PredictionMarkets/Kalshi/summaryStats/kalshi_trades.parquet'
    GROUP BY MONTH
    ORDER BY MONTH ASC
) TO contracts_per_trade_data.csv (HEADER, DELIMITER ',');
""").df()
print(result1)

con.close()



