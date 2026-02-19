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
        date_trunc('WEEK', created_time::TIMESTAMP)::DATE AS week,
        COUNT(*) as trade_count,
        SUM(count) AS total_contracts,
        SUM(yes_price_dollars::FLOAT * count)/ sum(count) AS avg_yes_price,
        total_contracts::FLOAT / trade_count as contracts_per_trade,
    FROM 'kalshi_merged_trades.parquet'
    GROUP BY week
    ORDER BY WEEK ASC
""").df()
print(result1)

result2 = con.execute("""
    SELECT
        MAX(created_time::TIMESTAMP)
    FROM 'kalshi_merged_trades.parquet'
""").df()
print(result2)

con.close()

