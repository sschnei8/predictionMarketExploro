from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import os
from dotenv import load_dotenv
load_dotenv()

DUNE_KEY_ID = os.getenv('DUNE_KEY_TOKEN')
QUERY_ID = 6696909

# Read in query from SQL file
file_name = 'pnl_query.sql'
with open(file_name, 'r') as file:
    sql = file.read()

print(f"Read SQL query from {file_name}")

# Initialize Dune client
dune = DuneClient(DUNE_KEY_ID)

# Try to get latest results without updating (avoids 403)
print(f"Fetching latest results from query {QUERY_ID}...")
query_result = dune.get_latest_result(QUERY_ID)
print(query_result)

# Can't update queries via API without the analyst plan... big sad... $65 per month

# print(f"Updating query {QUERY_ID}...")
# dune.update_query(
#     QUERY_ID,
#     query_sql=sql
# )



# Save CSV to file
#output_file = 'pnl_results.csv'
#with open(output_file, 'w') as f:
#    f.write(results_csv)

#print(f"Results saved to {output_file}")