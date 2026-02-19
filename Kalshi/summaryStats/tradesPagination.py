import requests
import time
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq
import json
from concurrent.futures import ThreadPoolExecutor, Future


# Example Response:
#
# {
#   "trades": [
#     {
#       "trade_id": "<string>",
#       "ticker": "<string>",
#       "price": 123,
#       "count": 123,
#       "count_fp": "10.00",
#       "yes_price": 123,
#       "no_price": 123,
#       "yes_price_dollars": "0.5600",
#       "no_price_dollars": "0.5600",
#       "taker_side": "yes",
#       "created_time": "2023-11-07T05:31:56Z"
#     }
#   ],
#   "cursor": "<string>"
# }
#
# Fields we keep:
#   trade_id, ticker, count, yes_price_dollars, taker_side, created_time

BASE_URL   = "https://api.elections.kalshi.com/trade-api/v2/markets/trades"
LIMIT      = 1000
# Kalshi allows 20 req/s; leave a small buffer
MIN_REQUEST_INTERVAL = 1.0 / 18  # ~18 req/s

SCHEMA = pa.schema([
    ('trade_id',          pa.string()),
    ('ticker',            pa.string()),
    ('count',             pa.int64()),
    ('yes_price_dollars', pa.string()),
    ('taker_side',        pa.string()),
    ('created_time',      pa.string()),
])


# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------

def save_cursor(cursor, cursor_file='trades_pagination_cursor.json'):
    with open(cursor_file, 'w') as f:
        json.dump({'cursor': cursor, 'timestamp': datetime.now().isoformat()}, f)


def load_cursor(cursor_file='trades_pagination_cursor.json'):
    if os.path.exists(cursor_file):
        with open(cursor_file, 'r') as f:
            data = json.load(f)
            print(f"Resuming from saved cursor (saved at {data['timestamp']})")
            return data['cursor']
    return None


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def build_session():
    """Create a requests.Session with connection pooling and keep-alive."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1,
        pool_maxsize=4,
    )
    session.mount('https://', adapter)
    return session


def fetch_page(session, cursor, max_retries=5, initial_backoff=1.0):
    """Fetch one page of trades, retrying on transient errors."""
    url = f"{BASE_URL}?limit={LIMIT}&min_ts=1770608928" UPDATE TIME
    if cursor:
        url += f"&cursor={cursor}"

    backoff = initial_backoff
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)

            if response.status_code == 200:
                return response.json()

            if response.status_code == 502:
                if attempt < max_retries - 1:
                    print(f"502 Bad Gateway (attempt {attempt + 1}/{max_retries}). Retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise Exception(f"Persistent 502 after {max_retries} attempts.")

            raise Exception(f"HTTP {response.status_code}: {response.text}")

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"Timeout (attempt {attempt + 1}/{max_retries}). Retrying in {backoff:.1f}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise Exception(f"Timeout after {max_retries} attempts.")

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request error: {e} (attempt {attempt + 1}/{max_retries}). Retrying in {backoff:.1f}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise Exception(f"Request failed after {max_retries} attempts: {e}")

    raise Exception(f"Failed after {max_retries} attempts")


# ---------------------------------------------------------------------------
# Parquet writing (runs in a background thread)
# ---------------------------------------------------------------------------

def build_table(batch):
    """Convert a list of trade dicts to a PyArrow table"""
    return pa.table(
        {
            'trade_id':          [t['trade_id']          for t in batch],
            'ticker':            [t['ticker']            for t in batch],
            'count':             [t['count']             for t in batch],
            'yes_price_dollars': [t['yes_price_dollars'] for t in batch],
            'taker_side':        [t['taker_side']        for t in batch],
            'created_time':      [t['created_time']      for t in batch],
        },
        schema=SCHEMA,
    )


def flush_batch(batch, filename, writer_state):
    """Write a batch to the parquet file. Runs in the background thread pool.

    writer_state is a one-element list so the background thread can share
    the writer handle back to the main thread without a class.
    """
    table = build_table(batch)
    if writer_state[0] is None:
        writer_state[0] = pq.ParquetWriter(filename, SCHEMA)
        print(f"Created {filename} (initial batch: {len(batch):,} trades)")
    else:
        print(f"Flushed {len(batch):,} trades to disk")
    writer_state[0].write_table(table)


def _stream_merge_parquet(base_file, append_file, batch_size=100_000):
    """Append append_file onto base_file without loading either fully into memory.

    Writes to a temporary merge file first, then atomically replaces base_file
    so the original is untouched if the merge crashes.
    """
    merge_tmp = base_file + ".merge_tmp"
    try:
        writer = pq.ParquetWriter(merge_tmp, SCHEMA)
        for path in (base_file, append_file):
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=batch_size):
                writer.write_batch(batch)
        writer.close()
        os.replace(merge_tmp, base_file)   # atomic on POSIX
    except Exception:
        if os.path.exists(merge_tmp):
            os.remove(merge_tmp)
        raise


# ---------------------------------------------------------------------------
# Main fetch loop
# ---------------------------------------------------------------------------

def get_all_trades_batched(filename='kalshi_trades.parquet', batch_size=100_000,
                           cursor_file='trades_pagination_cursor.json', resume=False):
    """Fetch all trades from Kalshi with pagination, writing to parquet in batches.

    Args:
        filename:    Output parquet file.
        batch_size:  Trades to buffer in memory before flushing to disk.
        cursor_file: File for crash-recovery cursor.
        resume:      If True, continue from the last saved cursor.
    """
    temp_filename = filename + ".resume_tmp"

    if resume:
        cursor = load_cursor(cursor_file)
        if cursor:
            print(f"Resuming from cursor: {cursor[:60]}...")
        else:
            print("No saved cursor found, starting fresh")
            cursor = None

        # If the output file already exists, write this session's data to a
        # temp file and merge afterwards — avoids overwriting existing data.
        if cursor and os.path.exists(filename):
            write_to = temp_filename
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
                print(f"Removed leftover temp file {temp_filename}")
            print(f"Will write resumed data to {temp_filename}, then merge into {filename}")
        else:
            write_to = filename
    else:
        cursor = None
        write_to = filename
        for f in [filename, cursor_file, temp_filename]:
            if os.path.exists(f):
                os.remove(f)
                print(f"Removed existing {f}")

    batch        = []
    total_trades = 0
    request_count = 0
    writer_state = [None]          # shared mutable writer handle
    pending_write: Future = None   # outstanding background write

    session = build_session()
    # Single-worker executor so writes are serialised but non-blocking for fetches
    executor = ThreadPoolExecutor(max_workers=1)

    last_request_time = 0.0

    try:
        while True:
            # ---- rate limiting: smooth per-request throttle ----
            now = time.monotonic()
            wait = MIN_REQUEST_INTERVAL - (now - last_request_time)
            if wait > 0:
                time.sleep(wait)

            last_request_time = time.monotonic()
            data = fetch_page(session, cursor)
            request_count += 1

            trades = data.get('trades', [])
            for trade in trades:
                batch.append({
                    'trade_id':          trade.get('trade_id'),
                    'ticker':            trade.get('ticker'),
                    'count':             trade.get('count'),
                    'yes_price_dollars': trade.get('yes_price_dollars'),
                    'taker_side':        trade.get('taker_side'),
                    'created_time':      trade.get('created_time'),
                })

            total_trades += len(trades)
            if request_count % 100 == 0 or len(trades) == 0:
                print(
                    f"requests: {request_count:,} | total trades: {total_trades:,} | "
                    f"batch: {len(batch):,}"
                )

            # Flush when batch is full — wait for any prior write first, then
            # submit new write in background so next fetch starts immediately
            if len(batch) >= batch_size:
                if pending_write is not None:
                    pending_write.result()   # ensure previous flush finished
                batch_to_write = batch
                batch = []
                pending_write = executor.submit(flush_batch, batch_to_write, write_to, writer_state)

            # Advance or stop
            cursor = data.get('cursor')
            if cursor:
                save_cursor(cursor, cursor_file)
            else:
                break

        # Final flush
        if pending_write is not None:
            pending_write.result()
        if batch:
            flush_batch(batch, write_to, writer_state)

        # If we wrote to a temp file, merge it into the final file
        if write_to == temp_filename:
            if writer_state[0] is not None:
                writer_state[0].close()
                writer_state[0] = None
                print(f"Closed parquet writer for {temp_filename}")
            print(f"Merging {temp_filename} into {filename}...")
            _stream_merge_parquet(filename, temp_filename)
            os.remove(temp_filename)
            print(f"Merge complete — removed {temp_filename}")

        if os.path.exists(cursor_file):
            os.remove(cursor_file)
            print("Completed successfully — removed cursor file")

    except Exception as e:
        if cursor:
            save_cursor(cursor, cursor_file)
            print(f"Error: {e}")
            print(f"Cursor saved to {cursor_file}. Re-run with resume=True to continue.")
        raise

    finally:
        executor.shutdown(wait=True)
        if writer_state[0] is not None:
            writer_state[0].close()
            print(f"Closed parquet writer for {write_to}")
        session.close()

    return total_trades


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pandas as pd

    print("Starting to fetch all Kalshi trades...")
    start = time.time()

    filename = 'kalshi_trades.parquet'

    # Two modes:
    # 1. Fresh start (resume=False) — deletes any existing file, starts from the beginning
    # 2. Resume        (resume=True) — continues from the last saved cursor after a crash

    total_trades = get_all_trades_batched(
        filename=filename,
        batch_size=100_000,
        resume=False,
    )

    elapsed = time.time() - start
    print(f"\nCompleted in {elapsed:.1f}s  ({total_trades / elapsed:,.0f} trades/sec)")
    print(f"Total trades fetched: {total_trades:,}")

    df = pd.read_parquet(filename)
    print(f"Total trades in file: {len(df):,}")
    print("\nFirst 5 trades:")
    print(df.head())
    print("\nDtypes:")
    print(df.dtypes)

# ===  Query 1: Total Volume by Market Type ===
#         week  trade_count  total_contracts  avg_yes_price  contracts_per_trade
# 0 2026-02-02      1392320     2.665806e+08       0.264324           191.465027
# 1 2026-02-16      6598195     9.187163e+08       0.430446           139.237518
# 2 2026-02-09     18509485     2.617990e+09       0.403633           141.440460