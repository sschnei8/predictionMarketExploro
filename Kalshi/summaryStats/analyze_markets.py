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
        result,
        status,
        COUNT(*) as num_markets,
        SUM(volume) as total_volume
    FROM 'kalshi_markets.parquet'
    GROUP BY result, status
""").df()
print(result3)

con.close()
