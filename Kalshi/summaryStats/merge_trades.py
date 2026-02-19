import duckdb

file1_path = 'kalshi_trades.parquet'
file2_path = 'kalshi_current_trades.parquet'
merged_file_path = 'kalshi_merged_trades.parquet'

# Create a DuckDB connection
con = duckdb.connect()

# Construct the SQL query
query = f"""
COPY (
    SELECT * FROM '{file1_path}'
    UNION ALL
    SELECT * FROM '{file2_path}'
) TO '{merged_file_path}' (FORMAT 'parquet')
"""
# Execute the COPY query first to create the merged file
con.execute(query)

result1 = con.execute("""
    SELECT COUNT(1)
    FROM 'kalshi_merged_trades.parquet'
""").df()
print(result1)

con.close()
