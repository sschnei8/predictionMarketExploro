import duckdb

con = duckdb.connect()

print("\n===  Query 1: Total Volume by Market Type ===")
result1 = con.execute("""
    SELECT
        date_trunc('MONTH', created_time::TIMESTAMP)::DATE AS MONTH,
        MEDIAN(count) median_contracts_per_trade,
        COUNT(*) as trade_count,
        SUM(count) AS total_contracts,
        SUM(yes_price_dollars::FLOAT * count)/ sum(count) AS avg_yes_price,
        total_contracts::FLOAT / trade_count as contracts_per_trade,
        MEDIAN(count) median_contracts_per_trade,
    FROM 'kalshi_trades.parquet'
    GROUP BY MONTH
    ORDER BY MONTH ASC
""").df()
print(result1)

con.close()

