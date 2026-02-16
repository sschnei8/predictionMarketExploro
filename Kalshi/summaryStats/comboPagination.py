import requests
import pandas as pd
import time
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq
import json


def save_cursor(cursor, cursor_file='pagination_combo_cursor.json'):
    """Save the current cursor to a file for recovery"""
    with open(cursor_file, 'w') as f:
        json.dump({'cursor': cursor, 'timestamp': datetime.now().isoformat()}, f)


def load_cursor(cursor_file='pagination_combo_cursor.json'):
    """Load the last saved cursor if it exists"""
    if os.path.exists(cursor_file):
        with open(cursor_file, 'r') as f:
            data = json.load(f)
            print(f"Resuming from saved cursor (saved at {data['timestamp']})")
            return data['cursor']
    return None


def save_last_run_metadata(filename='kalshi_markets_combo_metadata.json'):
    """Save metadata about the last successful run"""
    metadata = {
        'last_run_timestamp': datetime.now().isoformat(),
        'last_run_unix': int(time.time())
    }
    with open(filename, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved run metadata: {metadata['last_run_timestamp']}")


def load_last_run_metadata(filename='kalshi_markets_combo_metadata.json'):
    """Load metadata from the last successful run"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            metadata = json.load(f)
            print(f"Last run was at {metadata['last_run_timestamp']}")
            return metadata
    return None


def make_request_with_retry(url, max_retries=5, initial_backoff=1.0):
    """Make an API request with exponential backoff retry logic for 502 errors"""
    backoff = initial_backoff

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)

            # Success case
            if response.status_code == 200:
                return response

            # 502 Bad Gateway - retry with backoff
            elif response.status_code == 502:
                if attempt < max_retries - 1:
                    print(f"502 Bad Gateway (attempt {attempt + 1}/{max_retries}). Retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                    backoff *= 2  # Exponential backoff
                    continue
                else:
                    raise Exception(f"Persistent 502 Bad Gateway after {max_retries} attempts. API may be down.")

            # Other HTTP errors - fail immediately
            else:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"Request timeout (attempt {attempt + 1}/{max_retries}). Retrying in {backoff:.1f}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                raise Exception(f"Request timeout after {max_retries} attempts.")

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request error: {e} (attempt {attempt + 1}/{max_retries}). Retrying in {backoff:.1f}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                raise Exception(f"Request failed after {max_retries} attempts: {e}")

    raise Exception(f"Failed to complete request after {max_retries} attempts")


def get_all_markets_batched(filename='kalshi_combo_markets.parquet', batch_size=10000, cursor_file='pagination_combo_cursor.json',
                           resume=False, incremental=False, metadata_file='kalshi_markets_combo_metadata.json'):
    """Fetch all combo markets from the Kalshi API with pagination, rate limiting, and batch writing to disk

    Args:
        filename: Output parquet file name
        batch_size: Number of markets to batch before writing to disk
        cursor_file: File to store pagination cursor for recovery
        resume: Resume from last saved cursor after an error
        incremental: Only fetch markets created since last run (uses min_created_ts API filter)
        metadata_file: File to store metadata about last run timestamp
    """
    batch = []
    cursor = None
    base_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    mve_filter='only'
    limit = 1000
    request_count = 0
    start_time = time.time()
    total_markets = 0
    writer = None
    schema = None
    min_created_ts = None
    append_mode = False

    # Handle incremental mode - only fetch markets created since last run
    if incremental:
        metadata = load_last_run_metadata(metadata_file)
        if metadata:
            min_created_ts = metadata['last_run_unix']
            append_mode = True
            print(f"Incremental mode: fetching markets created after Unix timestamp {min_created_ts}")
            if not os.path.exists(filename):
                print(f"Warning: {filename} doesn't exist, will create new file")
                append_mode = False
        else:
            print(f"No previous run found, fetching all markets")
            incremental = False

    # Handle resume vs fresh start
    if resume:
        cursor = load_cursor(cursor_file)
        if cursor:
            print(f"Resuming from cursor: {cursor[:50]}...")
        else:
            print("No saved cursor found, starting fresh")
    elif not incremental:
        # Remove existing files to start fresh (only if not in incremental mode)
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Removed existing {filename}")
        if os.path.exists(cursor_file):
            os.remove(cursor_file)
            print(f"Removed existing {cursor_file}")


    try:
        while True:
            url = f"{base_url}?limit={limit}&mve_filter={mve_filter}" #Add MVE filter
            if cursor:
                url += f"&cursor={cursor}"
            if min_created_ts:
                url += f"&min_created_ts={min_created_ts}"

            # Rate limiting: max 20 requests per second
            if request_count > 0 and request_count % 20 == 0:
                elapsed = time.time() - start_time
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                start_time = time.time()

            # Make request with retry logic
            response = make_request_with_retry(url)
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
                writer, schema = write_batch_to_parquet(batch, filename, writer, schema, append_mode)
                batch = []  # Clear the batch after writing

            # Check if there are more pages
            cursor = data.get('cursor')
            if cursor:
                # Save cursor after processing each page for recovery
                save_cursor(cursor, cursor_file)
            else:
                break

        # Write any remaining markets in the final batch
        if batch:
            writer, schema = write_batch_to_parquet(batch, filename, writer, schema, append_mode)

        # In append mode, merge the temporary file with the existing file
        if append_mode and writer is not None:
            # Close writer before merging
            writer.close()
            writer = None

            temp_filename = filename.replace('.parquet', '_temp.parquet')
            if os.path.exists(temp_filename):
                print(f"Merging new markets with existing file...")
                # Read both files
                existing_df = pd.read_parquet(filename)
                new_df = pd.read_parquet(temp_filename)

                # Concatenate and remove duplicates (in case of overlap)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['ticker'], keep='last')

                # Write combined data back
                combined_df.to_parquet(filename, index=False)

                # Clean up temp file
                os.remove(temp_filename)
                print(f"Merged {len(new_df)} new markets. Total markets: {len(combined_df)}")

        # Save metadata for next incremental run
        save_last_run_metadata(metadata_file)

        # Clean up cursor file on successful completion
        if os.path.exists(cursor_file):
            os.remove(cursor_file)
            print(f"Completed successfully - removed cursor file")

    except Exception as e:
        # Save cursor on error for potential resume
        if cursor:
            save_cursor(cursor, cursor_file)
            print(f"Error occurred: {e}")
            print(f"Cursor saved to {cursor_file}. You can resume by setting resume=True")
        raise

    finally:
        # Close the writer if it was opened
        if writer is not None:
            writer.close()
            print(f"Closed parquet writer for {filename}")

    return total_markets


def write_batch_to_parquet(batch, filename, writer=None, schema=None, append_mode=False):
    """Append a batch of markets to the parquet file using PyArrow for efficient streaming writes

    Args:
        batch: List of market dictionaries to write
        filename: Target parquet file (in append mode, writes to temp file first)
        writer: Existing ParquetWriter instance (for streaming writes)
        schema: PyArrow schema from previous batches
        append_mode: If True, writes to temporary file that will be merged later
    """
    df = pd.DataFrame(batch)
    table = pa.Table.from_pandas(df)

    # In append mode, use a temporary file that will be merged at the end
    if append_mode and writer is None:
        temp_filename = filename.replace('.parquet', '_temp.parquet')
        schema = table.schema
        writer = pq.ParquetWriter(temp_filename, schema)
        print(f"Created temporary file {temp_filename} for new markets (batch: {len(batch)})")
    # Initialize writer on first batch (normal mode)
    elif writer is None:
        schema = table.schema
        writer = pq.ParquetWriter(filename, schema)
        print(f"Created {filename} with initial batch of {len(batch)} markets")
    else:
        print(f"Appended {len(batch)} markets to file")

    writer.write_table(table)
    return writer, schema


if __name__ == "__main__":
    print("Starting to fetch Kalshi markets...")
    start = time.time()

    filename = 'kalshi_combo_markets.parquet'

    # Three modes of operation:
    # 1. Fresh start (resume=False, incremental=False) - fetches all markets from scratch
    # 2. Resume from error (resume=True) - continues from last saved cursor after an error
    # 3. Incremental update (incremental=True) - only fetches markets created since last run

    total_markets = get_all_markets_batched(
        filename=filename,
        batch_size=10000,
        resume=True,        # Set to True to resume after an error
        incremental=False    # Set to True to only fetch new markets since last run
    )

    print(f"\nCompleted in {time.time() - start:.2f} seconds")
    print(f"Total markets fetched: {total_markets}")

    # Load and display sample
    df = pd.read_parquet(filename)
    print(f"Total markets in file: {len(df)}")
    print("\nFirst 5 markets:")
    print(df.head())