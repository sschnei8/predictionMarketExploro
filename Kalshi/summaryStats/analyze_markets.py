import duckdb

# Connect to DuckDB (in-memory database)
con = duckdb.connect()

# Read the parquet file (more efficient than CSV)
# You can also read CSV with: con.execute("SELECT * FROM 'kalshi_markets.csv'")
#df = con.execute("SELECT * FROM 'kalshi_markets.parquet'").df()

#print("=== Dataset Overview ===")
#print(f"Total markets: {len(df)}")
#print(f"\nColumns: {df.columns.tolist()}")
#print(f"\nData types:\n{df.dtypes}")

# Example queries
# print("\n=== Example Query 1: Distinct markets ===")
# result = con.execute("""
#     SELECT ticker, event_ticker, COUNT(*) as countup
#     FROM 'kalshi_markets.parquet'
#     GROUP BY ticker, event_ticker
#     HAVING countup > 1
# """).df()
# print(result)

print("\n=== Example Query 2: Total Volume by Market Type ===")
result = con.execute("""
    SELECT
        market_type,
        COUNT(*) as market_count,
        SUM(volume) as total_volume,
        AVG(volume) as avg_volume,
        SUM(liquidity) as total_liquidity
    FROM 'kalshi_markets.parquet'
    GROUP BY market_type
    ORDER BY total_volume DESC
""").df()
print(result)

con.close()


# print("\n=== Example Query 3: Markets Over Time (by month) ===")
# result = con.execute("""
#     SELECT
#         DATE_TRUNC('month', CAST(open_time AS TIMESTAMP)) as month,
#         COUNT(*) as markets_opened,
#         SUM(volume) as total_volume
#     FROM 'kalshi_markets.parquet'
#     WHERE open_time IS NOT NULL
#     GROUP BY month
#     ORDER BY month DESC
#     LIMIT 12
# """).df()
# print(result)
# 
# print("\n=== Example Query 4: Top Events by Volume ===")
# result = con.execute("""
#     SELECT
#         event_ticker,
#         COUNT(*) as market_count,
#         SUM(volume) as total_volume,
#         AVG(liquidity) as avg_liquidity
#     FROM 'kalshi_markets.parquet'
#     WHERE event_ticker IS NOT NULL
#     GROUP BY event_ticker
#     ORDER BY total_volume DESC
#     LIMIT 10
# """).df()
# print(result)
# 
# print("\n=== Example Query 5: Settled Markets with Results ===")
# result = con.execute("""
#     SELECT
#         result,
#         COUNT(*) as count,
#         SUM(volume) as total_volume
#     FROM 'kalshi_markets.parquet'
#     WHERE result IS NOT NULL AND result != ''
#     GROUP BY result
#     ORDER BY count DESC
# """).df()
# print(result)
# 
# print("\n=== Example Query 6: Market Lifecycle Duration ===")
# result = con.execute("""
#     SELECT
#         market_type,
#         AVG(EPOCH(CAST(close_time AS TIMESTAMP) - CAST(open_time AS TIMESTAMP)) / 86400) as avg_days_open,
#         COUNT(*) as market_count
#     FROM 'kalshi_markets.parquet'
#     WHERE open_time IS NOT NULL
#       AND close_time IS NOT NULL
#       AND close_time > open_time
#     GROUP BY market_type
#     ORDER BY avg_days_open DESC
# """).df()
# print(result)
# 
# # You can also save query results
# print("\n=== Saving Query Results ===")
# con.execute("""
#     COPY (
#         SELECT
#             event_ticker,
#             COUNT(*) as market_count,
#             SUM(volume) as total_volume
#         FROM 'kalshi_markets.parquet'
#         GROUP BY event_ticker
#         ORDER BY total_volume DESC
#     ) TO 'event_summary.csv' (HEADER, DELIMITER ',')
# """)
# print("Saved event summary to event_summary.csv")
# 
# con.close()
