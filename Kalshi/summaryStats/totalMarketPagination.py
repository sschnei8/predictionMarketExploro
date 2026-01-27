import requests
import pandas as pd
import time
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq


def get_all_markets_batched(filename='kalshi_markets.parquet', batch_size=10000):
    """Fetch all markets from Kalshi API with pagination, rate limiting, and batch writing to disk"""
    batch = []
    cursor = None
    base_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    limit = 1000
    request_count = 0
    start_time = time.time()
    total_markets = 0
    writer = None
    schema = None

    # Remove existing file if it exists to start fresh
    if os.path.exists(filename):
        os.remove(filename)
        print(f"Removed existing {filename}")

    try:
        while True:
            url = f"{base_url}?limit={limit}" # Can add status=open qualifier here ~159K markets
            if cursor:
                url += f"&cursor={cursor}"

            # Rate limiting: max 20 requests per second
            if request_count > 0 and request_count % 20 == 0:
                elapsed = time.time() - start_time
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                start_time = time.time()

            response = requests.get(url)
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break

            data = response.json()
            request_count += 1

            # Extract only the fields we need
            for market in data['markets']:
                batch.append({
                    'ticker': market.get('ticker'),
                    'event_ticker': market.get('event_ticker'),
                    'result': market.get('result'),
                    'status': market.get('status'),
                    'volume': market.get('volume'),
                    'open_time': market.get('open_time'),
                    'close_time': market.get('close_time'),
                    'liquidity': market.get('liquidity'),
                    'market_type': market.get('market_type')
                })

            total_markets += len(data['markets'])
            print(f"Fetched {len(data['markets'])} markets, total: {total_markets}, in batch: {len(batch)}, requests: {request_count}")

            # Write batch to disk when it reaches batch_size
            if len(batch) >= batch_size:
                writer, schema = write_batch_to_parquet(batch, filename, writer, schema)
                batch = []  # Clear the batch after writing

            # Check if there are more pages
            cursor = data.get('cursor')
            if not cursor:
                break

        # Write any remaining markets in the final batch
        if batch:
            writer, schema = write_batch_to_parquet(batch, filename, writer, schema)

    finally:
        # Close the writer if it was opened
        if writer is not None:
            writer.close()
            print(f"Closed parquet writer for {filename}")

    return total_markets


def write_batch_to_parquet(batch, filename, writer=None, schema=None):
    """Append a batch of markets to the parquet file using PyArrow for efficient streaming writes"""
    df = pd.DataFrame(batch)
    table = pa.Table.from_pandas(df)

    # Initialize writer on first batch
    if writer is None:
        schema = table.schema
        writer = pq.ParquetWriter(filename, schema)
        print(f"Created {filename} with initial batch of {len(batch)} markets")
    else:
        print(f"Appended {len(batch)} markets to {filename}")

    writer.write_table(table)
    return writer, schema


if __name__ == "__main__":
    print("Starting to fetch all Kalshi markets...")
    start = time.time()

    filename = 'kalshi_markets.parquet'
    total_markets = get_all_markets_batched(filename=filename, batch_size=10000)

    print(f"\nCompleted in {time.time() - start:.2f} seconds")
    print(f"Total markets fetched: {total_markets}")

    # Load and display sample
    df = pd.read_parquet(filename)
    print(f"Total markets in file: {len(df)}")
    print("\nFirst 5 markets:")
    print(df.head())