from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import os
from dotenv import load_dotenv
load_dotenv()

DUNE_KEY_ID=os.getenv('DUNE_KEY_TOKEN')

dune = DuneClient(DUNE_KEY_ID)
query_result = dune.get_latest_result(6696909)
print(query_result)


# query = QueryBase(
#     name="Polymarket user P&L",
#     query_id=6696909
# )
# print("Results available at", query.url())
# 
# dune = DuneClient()
# results = dune.run_query(query)

# or as CSV
# results_csv = dune.run_query_csv(query)

# or as Pandas Dataframe
# results_df = dune.run_query_dataframe(query)