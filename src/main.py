#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import logging
import asyncio
import aiohttp
import duckdb
import pytz
import time
import glob
import re # For masking secrets
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP # For median/aggregation
from datetime import datetime, time as dt_time, timedelta, date
from logging.handlers import TimedRotatingFileHandler
import traceback
from typing import Dict, Any, Optional, List, Tuple, Union # Re-added Union

# --- Configuration ---

# Secrets & Environment Variables
BASE_URL_ENV_VAR: str = "BRS_BASE_URL"
API_KEY_ENV_VAR: str = "BRS_API_KEY"
LOG_LEVEL_ENV_VAR: str = "LOG_LEVEL"

# General Settings
LOG_FOLDER: str = "logs"
LOG_FILE_NAME: str = "market_data.log"
LOG_ROTATION_HOURS: int = 48 # Rotation every 48 hours
LOG_BACKUP_COUNT: int = 5 # Keep 5 backup log files
DATA_FOLDER: str = "data"
DB_FILE: str = os.path.join(DATA_FOLDER, "market_data.duckdb")
REQUEST_TIMEOUT_SECONDS: int = 10
PRETTY_PRINT_JSON: bool = False # Keep False for smaller file size
MAX_CONCURRENT_REQUESTS: int = 10
TIMEZONE: str = "Asia/Tehran"
DEFAULT_TIMEZONE = pytz.timezone(TIMEZONE) # Cache timezone object

# User Agent (Keep updated)
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36" # Example
HEADERS: Dict[str, str] = { "User-Agent": USER_AGENT, "Accept": "application/json, text/plain, */*" }

# --- API Endpoint Configuration ---
# Added 'pk_json_path' for unique identification within arrays if symbol is not unique across nested structures
API_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "gold_currency": {
        "relative_url": "/Api/Market/Gold_Currency.php?key={api_key}",
        "output_filename": "gold_currency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["daily", "6h", "12h", "3d", "weekly"],
        # Assumes structure { "gold": [ { "symbol": "X", "price": Y }, ... ], "currency": [ ... ] }
        # We'll aggregate 'gold' and 'currency' separately if needed, or define paths more precisely
        "price_json_path": ["$.price"], # Path relative to each item in the array
        "symbol_json_path": ["$.symbol"], # Path relative to each item
        "array_base_paths": ["gold", "currency"], # NEW: List of base keys containing arrays to process
    },
    "crypto": {
        "relative_url": "/Api/Market/Cryptocurrency.php?key={api_key}",
        "output_filename": "cryptocurrency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["daily", "6h", "12h", "3d", "weekly"],
        # Assumes structure [ { "name": "X", "price_toman": Y }, ... ]
        "price_json_path": ["$.price_toman"],
        "symbol_json_path": ["$.name"],
        "array_base_paths": ["$"], # Represents the root array itself
    },
    "commodity": {
        "relative_url": "/Api/Market/Commodity.php?key={api_key}",
        "output_filename": "commodity.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["daily", "6h", "12h", "3d", "weekly"],
        # Aggregate precious metals only for now
        "price_json_path": ["$.price"],
        "symbol_json_path": ["$.symbol"],
        "array_base_paths": ["metal_precious"], # Only process this array
    },
    "tse_ifb_symbols": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=1",
        "output_filename": "tse_ifb_symbols.json",
        "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True,
        "aggregation_levels": ["daily"], # Only daily aggregation
        # Assumes structure [ { "l18": "X", "pc": Y }, ... ]
        "price_json_path": ["$.pc"],
        "symbol_json_path": ["$.l18"],
        "array_base_paths": ["$"],
    },
    # Other endpoints remain the same, without aggregation config unless added
     "tse_options": { "relative_url": "/Api/Tsetmc/Option.php?key={api_key}", "output_filename": "tse_options.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_nav": { "relative_url": "/Api/Tsetmc/Nav.php?key={api_key}", "output_filename": "tse_nav.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=1", "output_filename": "tse_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "ifb_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=2", "output_filename": "ifb_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "selected_indices": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=3", "output_filename": "selected_indices.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "debt_securities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=4", "output_filename": "debt_securities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "housing_facilities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=5", "output_filename": "housing_facilities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "futures": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=3", "output_filename": "futures.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
}

# Market Hours
TSE_MARKET_OPEN_TIME: dt_time = dt_time(8, 45)
TSE_MARKET_CLOSE_TIME: dt_time = dt_time(12, 45)
TSE_MARKET_DAYS: List[int] = [5, 6, 0, 1, 2] # Sat-Wed

# Aggregation Intervals & Trigger Time
AGGREGATION_INTERVALS: Dict[str, timedelta] = {
    "6h": timedelta(hours=6), "12h": timedelta(hours=12), "daily": timedelta(days=1),
    "3d": timedelta(days=3), "weekly": timedelta(weeks=1),
}
DAILY_AGGREGATION_TIME_LOCAL: dt_time = dt_time(0, 5) # 00:05 Tehran time

# --- Constants ---
COLOR_GREEN: str = "\033[32m"
COLOR_RED: str = "\033[31m"
COLOR_YELLOW: str = "\033[33m"
COLOR_BLUE: str = "\033[34m"
COLOR_RESET: str = "\033[0m"
DECIMAL_PRECISION = 18 # Precision for storing prices
DECIMAL_SCALE = 6    # Scale for storing prices

# --- Global Logger ---
logger = logging.getLogger("MarketDataSync")

# --- Utility ---
def mask_string(s: Optional[str]) -> str:
    """Masks potentially sensitive strings."""
    if s is None: return "None"
    # Mask common key patterns more robustly
    s = re.sub(r"key=[^&?\s]+", "key=********", s, flags=re.IGNORECASE)
    s = re.sub(r"token=[^&?\s]+", "token=********", s, flags=re.IGNORECASE)
    # Add other sensitive patterns if needed
    return s

# --- Logging Setup ---
def setup_logging() -> None:
    log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Set the logger's level - Handlers will inherit or filter further
    logger.setLevel(logging.DEBUG) # Allow all messages to reach handlers initially

    if logger.hasHandlers(): logger.handlers.clear()

    log_dir = LOG_FOLDER
    os.makedirs(log_dir, exist_ok=True)
    cleanup_old_logs(log_dir, LOG_ROTATION_HOURS)
    log_file_path = os.path.join(log_dir, LOG_FILE_NAME)

    # File Handler - Always DEBUG
    file_handler = TimedRotatingFileHandler(
        log_file_path, when="H", interval=LOG_ROTATION_HOURS, backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s|%(levelname)-7s|%(name)s|%(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)

    # Console Handler - Level from ENV
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level) # Filter based on ENV

    class SecureColorFormatter(logging.Formatter):
        # ... (formatter code remains the same as previous version, ensuring masking) ...
        level_colors = {
            logging.DEBUG: COLOR_BLUE, logging.INFO: COLOR_GREEN,
            logging.WARNING: COLOR_YELLOW, logging.ERROR: COLOR_RED, logging.CRITICAL: COLOR_RED,
        }
        def format(self, record):
            color = self.level_colors.get(record.levelno, COLOR_RESET)
            original_message = record.getMessage()
            # Mask secrets HERE before formatting
            masked_message = mask_string(original_message)

            log_fmt = f"{color}•{COLOR_RESET} %(asctime)s|{color}%(levelname)-7s{COLOR_RESET}| {color}{masked_message}{COLOR_RESET}"
            _formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S") # Use simple format
            record.message = record.getMessage() # Keep original if needed
            record.msg = masked_message # Set msg attribute for the formatter
            formatted = _formatter.format(record)
            return formatted

    console_handler.setFormatter(SecureColorFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info(f"Logging setup complete. Console Level: {log_level_str}, File Level: DEBUG")

def cleanup_old_logs(log_directory: str, hours_to_keep: int) -> None:
    cutoff = time.time() - (hours_to_keep * 60 * 60)
    print(f"{COLOR_BLUE}•{COLOR_RESET} Checking logs older than {hours_to_keep} hours in '{log_directory}'...")
    cleaned_count = 0
    try:
        for filename in glob.glob(os.path.join(log_directory, LOG_FILE_NAME + "*")):
            if os.path.isfile(filename):
                try:
                    if os.path.getmtime(filename) < cutoff:
                        os.remove(filename)
                        print(f"{COLOR_YELLOW}✓ Cleaned old log: {os.path.basename(filename)}{COLOR_RESET}")
                        cleaned_count += 1
                except OSError as e:
                    print(f"{COLOR_RED}✗ Error removing {os.path.basename(filename)}: {e}{COLOR_RESET}")
        if cleaned_count == 0: print(f"{COLOR_GREEN}✓ No old logs found.{COLOR_RESET}")
    except Exception as e: print(f"{COLOR_RED}✗ Log cleanup scan error: {e}{COLOR_RESET}")

# --- Timezone & Market Check ---
def is_market_open(tz_str: str, open_time: dt_time, close_time: dt_time, market_days: List[int]) -> bool:
    try:
        now_local = datetime.now(DEFAULT_TIMEZONE) # Use cached timezone
        current_time = now_local.time()
        current_weekday = now_local.weekday()
        logger.debug(f"Market Check: Local={now_local.strftime('%H:%M:%S %Z')}, WDay={current_weekday}, Time={current_time} vs {open_time}-{close_time}, Days={market_days}")
        if current_weekday not in market_days:
            logger.debug("Market Status: CLOSED (Day)")
            return False
        is_open = open_time <= current_time < close_time
        logger.debug(f"Market Status: {'OPEN' if is_open else 'CLOSED'} (Hours)")
        return is_open
    except Exception as e:
        logger.error(f"Market hours check failed: {e}", exc_info=True)
        return False

# --- Database Functions ---
# ... (get_db_connection, get_last_run_time, update_last_run_time, should_run_task remain mostly the same) ...
def get_db_connection(db_path: str) -> Optional[duckdb.DuckDBPyConnection]:
    try:
        logger.debug(f"Connecting to DB: {db_path}")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = duckdb.connect(database=db_path, read_only=False)
        logger.info("DB connection successful.")
        # Ensure tracker table exists on connection
        conn.execute("""
            CREATE TABLE IF NOT EXISTS aggregation_tracker (
                task_name TEXT PRIMARY KEY,
                last_run_timestamp TIMESTAMPTZ
            );
        """)
        logger.debug("Ensured aggregation_tracker table exists.")
        return conn
    except Exception as e:
        logger.critical(f"CRITICAL: DB Connect/Setup failed: {e}", exc_info=True)
        return None

def get_last_run_time(conn: duckdb.DuckDBPyConnection, task_name: str) -> Optional[datetime]:
    try:
        result = conn.execute("SELECT last_run_timestamp FROM aggregation_tracker WHERE task_name = ?", (task_name,)).fetchone()
        if result and result[0]:
            ts = result[0]
            # DuckDB TIMESTAMPTZ should be timezone-aware (UTC)
            if isinstance(ts, datetime) and ts.tzinfo is not None:
                return ts
            elif isinstance(ts, datetime): # If somehow naive, assume UTC
                 logger.warning(f"DB returned naive datetime for {task_name}, assuming UTC.")
                 return pytz.utc.localize(ts)
            else: # Handle date type for daily task tracking if needed
                 logger.warning(f"Unexpected type {type(ts)} for last_run_timestamp for {task_name}")
                 return None
        return None
    except Exception as e:
        logger.error(f"Error getting last run time for '{task_name}': {e}", exc_info=True)
        return None

def update_last_run_time(conn: duckdb.DuckDBPyConnection, task_name: str, timestamp: datetime) -> None:
    try:
        conn.execute("""
            INSERT INTO aggregation_tracker (task_name, last_run_timestamp) VALUES (?, ?)
            ON CONFLICT (task_name) DO UPDATE SET last_run_timestamp = excluded.last_run_timestamp;
            """, (task_name, timestamp))
        logger.debug(f"Updated last run time for '{task_name}' to {timestamp.isoformat()}")
    except Exception as e:
        logger.error(f"Error updating last run time for '{task_name}': {e}", exc_info=True)

def should_run_task(conn: duckdb.DuckDBPyConnection, task_name: str, interval: timedelta, now_utc: datetime) -> bool:
    last_run = get_last_run_time(conn, task_name)
    if last_run is None:
        logger.info(f"Task '{task_name}' due: Never run before.")
        return True

    time_since_last_run = now_utc - last_run
    is_due = time_since_last_run >= interval
    logger.debug(f"Task '{task_name}': Last={last_run.isoformat()}, Since={time_since_last_run}, Interval={interval}, Due={is_due}")
    return is_due


def ensure_table(conn: duckdb.DuckDBPyConnection, create_sql: str, table_name: str) -> bool:
    """Generic function to ensure a table exists."""
    try:
        conn.execute(create_sql)
        logger.debug(f"Ensured table '{table_name}' exists.")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure table '{table_name}': {e}", exc_info=True)
        return False

def ensure_raw_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str) -> bool:
    table_name = f"{endpoint_name}_raw"
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (timestamp TIMESTAMPTZ PRIMARY KEY, data JSON);'
    return ensure_table(conn, sql, table_name)

def ensure_aggregate_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str) -> bool:
    table_name = f"{endpoint_name}_{level}"
    ts_type = "DATE" if level == "daily" else "TIMESTAMPTZ"
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        timestamp {ts_type}, symbol TEXT,
        median_price DECIMAL({DECIMAL_PRECISION}, {DECIMAL_SCALE}), source_count BIGINT,
        PRIMARY KEY (timestamp, symbol)
    );"""
    return ensure_table(conn, sql, table_name)


def insert_raw_data(conn: duckdb.DuckDBPyConnection, endpoint_name: str, timestamp: datetime, data: Any) -> bool:
    table_name = f"{endpoint_name}_raw"
    if not ensure_raw_table(conn, endpoint_name): return False # Added ensure call here
    try:
        json_data_str = json.dumps(data, ensure_ascii=False)
        conn.execute(f'INSERT INTO "{table_name}" (timestamp, data) VALUES (?, ?)', (timestamp, json_data_str))
        logger.debug(f"Inserted raw data into '{table_name}'.")
        return True
    except duckdb.ConstraintException:
         logger.warning(f"Duplicate raw timestamp for '{table_name}'. Skipping.")
         return True
    except Exception as e:
        logger.error(f"Failed insert raw data into '{table_name}': {e}", exc_info=True)
        return False

# --- Aggregation ---
def calculate_and_store_aggregates(
    conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str,
    config: Dict[str, Any], start_time_utc: datetime, end_time_utc: datetime,
    target_timestamp: Union[datetime, date]
) -> bool:
    raw_table = f"{endpoint_name}_raw"
    agg_table = f"{endpoint_name}_{level}"
    task_name = f"aggregate_{level}_{endpoint_name}"

    # --- Check if raw table exists and has data for the period ---
    try:
         count_result = conn.execute(f'SELECT COUNT(*) FROM "{raw_table}" WHERE timestamp >= ? AND timestamp < ?', (start_time_utc, end_time_utc)).fetchone()
         if not count_result or count_result[0] == 0:
              logger.info(f"Skipping aggregation for {agg_table}: No raw data found in period {start_time_utc.date()} - {end_time_utc.date()}.")
              # Update tracker so we don't check again immediately for this period
              update_last_run_time(conn, task_name, datetime.now(pytz.utc))
              return True
    except duckdb.CatalogException:
         logger.warning(f"Skipping aggregation for {agg_table}: Raw table '{raw_table}' does not exist yet.")
         return True # Not an error, just wait for raw data
    except Exception as e:
         logger.error(f"Error checking raw data count for {agg_table}: {e}", exc_info=True)
         return False

    # Ensure aggregate table exists
    if not ensure_aggregate_table(conn, endpoint_name, level): return False

    # Get JSON processing paths
    price_path_in_item = config.get("price_json_path", ["$.price"])[0] # Take first path element
    symbol_path_in_item = config.get("symbol_json_path", ["$.symbol"])[0]
    array_base_paths = config.get("array_base_paths", ["$"]) # List of paths to arrays

    # --- Build Aggregation Query ---
    # We need to handle multiple base paths (like gold and currency)
    all_aggregates = []
    query_success = True

    for base_path in array_base_paths:
        try:
            # Correct function is unnest
            # Need to handle root array ($) vs nested array (gold, metal_precious)
            if base_path == "$": # Root is an array
                unnest_expr = f"unnest(data)"
            else: # Nested array
                unnest_expr = f"unnest(data -> '{base_path}')"

            # Use json_extract_string for symbol, try_cast for price
            query = f"""
            WITH UnnestedItems AS (
                SELECT {unnest_expr} AS item
                FROM "{raw_table}"
                WHERE timestamp >= ? AND timestamp < ?
            ),
            ExtractedData AS (
                SELECT
                    json_extract_string(item, '{symbol_path_in_item}') AS symbol,
                    -- Extract as string first, then try_cast
                    json_extract_string(item, '{price_path_in_item}') AS price_str
                FROM UnnestedItems
            ),
            FilteredData AS (
                SELECT
                    symbol,
                    try_cast(price_str AS DECIMAL({DECIMAL_PRECISION*2}, {DECIMAL_SCALE*2})) AS price_decimal
                    -- Use higher intermediate precision for cast
                FROM ExtractedData
                WHERE symbol IS NOT NULL AND price_str IS NOT NULL AND symbol != ''
            )
            SELECT
                symbol,
                median(price_decimal) AS median_price,
                count(*) AS source_count
            FROM FilteredData
            WHERE price_decimal IS NOT NULL -- Filter out failed casts
            GROUP BY symbol;
            """
            logger.debug(f"Executing agg query for {agg_table} (path: {base_path}) period: {start_time_utc.date()}-{end_time_utc.date()}")
            aggregates = conn.execute(query, (start_time_utc, end_time_utc)).fetchall()
            logger.info(f"Aggregated {len(aggregates)} symbols from path '{base_path}' for {agg_table}")
            all_aggregates.extend(aggregates)

        except duckdb.CatalogException as e:
            # Specific error for non-existent table function - likely typo or version issue
             logger.error(f"DuckDB Catalog Error during aggregation for {agg_table} (path: {base_path}): {e}. Check DuckDB version and JSON functions.", exc_info=True)
             query_success = False
             break # Stop processing other base paths for this endpoint/level if query fails
        except Exception as e:
            logger.error(f"Error during aggregation for {agg_table} (path: {base_path}): {e}", exc_info=True)
            query_success = False
            break # Stop processing

    if not query_success:
        return False # Don't update tracker if query failed

    # --- Insert Aggregates ---
    if not all_aggregates:
        logger.warning(f"No valid aggregate data calculated for {agg_table} in the period.")
    else:
        insert_sql = f"""
        INSERT INTO "{agg_table}" (timestamp, symbol, median_price, source_count) VALUES (?, ?, ?, ?)
        ON CONFLICT (timestamp, symbol) DO UPDATE SET
            median_price = excluded.median_price, source_count = excluded.source_count;
        """
        rows_to_insert = []
        for symbol, median_val, count in all_aggregates:
             if isinstance(median_val, (Decimal, float, int)):
                  # Convert median_val to Decimal with target precision/scale before inserting
                  try:
                      final_median = Decimal(median_val).quantize(Decimal('1e-' + str(DECIMAL_SCALE)), rounding=ROUND_HALF_UP)
                      rows_to_insert.append((target_timestamp, symbol, final_median, count))
                  except (InvalidOperation, TypeError):
                      logger.warning(f"Could not convert median value {median_val} ({type(median_val)}) to Decimal for {symbol} in {agg_table}.")
             else:
                  logger.warning(f"Median value for {symbol} in {agg_table} is not numeric ({type(median_val)}), skipping.")

        if rows_to_insert:
            try:
                conn.executemany(insert_sql, rows_to_insert)
                logger.info(f"Stored {len(rows_to_insert)} aggregates into {agg_table}.")
            except Exception as e:
                logger.error(f"Error inserting aggregates into {agg_table}: {e}", exc_info=True)
                return False # Return failure if insert fails
        else:
            logger.warning(f"No valid rows to insert into {agg_table} after validation.")

    # Update tracker *after* successful processing or if no data was found
    update_last_run_time(conn, task_name, datetime.now(pytz.utc))
    return True


# --- API Fetching ---
# ... (fetch_api_data remains the same as previous, ensuring masking) ...
async def fetch_api_data(
    session: aiohttp.ClientSession, endpoint_name: str, config: Dict[str, Any],
    base_url: str, api_key: str
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    relative_url = config['relative_url'].format(api_key=api_key)
    full_url = f"{base_url.rstrip('/')}/{relative_url.lstrip('/')}"
    masked_url = mask_string(full_url) # Mask upfront
    logger.debug(f"Requesting: {endpoint_name} ({masked_url})") # Log masked URL
    start_time = time.monotonic()
    try:
        async with session.get(full_url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            elapsed_time = time.monotonic() - start_time
            logger.debug(f"Response: {endpoint_name} ({response.status}) in {elapsed_time:.2f}s") # Log only status/time
            if response.status == 200:
                try:
                    data = await response.json(content_type=None)
                    # Log success without data itself unless DEBUG
                    logger.info(f"Success: Fetched '{endpoint_name}'.")
                    # logger.debug(f"Data for {endpoint_name}: {str(data)[:200]}...") # Optional: log snippet at DEBUG
                    return endpoint_name, config, data
                except json.JSONDecodeError as json_err:
                    response_text_snippet = await response.text()
                    response_text_snippet = mask_string(response_text_snippet[:200]) # Mask snippet
                    logger.error(f"JSON Decode Error: {endpoint_name} ({response.status}). Snippet: '{response_text_snippet}'. Err: {json_err}", exc_info=False)
                    return None
                except Exception as e:
                    logger.error(f"Response Proc Error: {endpoint_name} ({response.status}). Err: {e}", exc_info=True)
                    return None
            else:
                response_text_snippet = await response.text()
                response_text_snippet = mask_string(response_text_snippet[:200])
                logger.error(f"API Request Failed: {endpoint_name} ({response.status}). Snippet: '{response_text_snippet}'")
                return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout Error ({REQUEST_TIMEOUT_SECONDS}s): {endpoint_name}", exc_info=False)
        return None
    except aiohttp.ClientError as client_err:
        logger.error(f"Client Error: {endpoint_name}. Err: {client_err}", exc_info=False)
        return None
    except Exception as e:
        logger.error(f"Unexpected Fetch Error: {endpoint_name}. Err: {e}", exc_info=True)
        return None

# --- Main Execution ---
async def main():
    script_start_time = time.monotonic()
    logger.info(f"{COLOR_GREEN}--- Starting Market Data Sync Cycle ---{COLOR_RESET}")

    api_key = os.getenv(API_KEY_ENV_VAR)
    base_url = os.getenv(BASE_URL_ENV_VAR)
    if not api_key or not base_url:
        logger.critical("CRITICAL: BRS_API_KEY or BRS_BASE_URL env var not set.")
        return

    db_conn = get_db_connection(DB_FILE)
    if db_conn is None: return

    now_utc = datetime.now(pytz.utc)
    now_local = now_utc.astimezone(DEFAULT_TIMEZONE)
    logger.debug(f"Cycle time: UTC={now_utc.isoformat()}, Local={now_local.isoformat()}")

    # --- 1. Fetch Raw Data ---
    apis_to_fetch_this_run: List[Tuple[str, Dict[str, Any]]] = []
    logger.info("--- Checking API Fetch Tasks ---")
    for name, config in API_ENDPOINTS.items():
        logger.debug(f"Checking: '{name}'")
        if not config.get('enabled', False):
            logger.debug(" -> Skip: Disabled")
            continue
        if config.get('market_hours_apply', False) and not is_market_open(TIMEZONE, TSE_MARKET_OPEN_TIME, TSE_MARKET_CLOSE_TIME, TSE_MARKET_DAYS):
            logger.debug(" -> Skip: Market Closed")
            continue
        fetch_interval = timedelta(minutes=config.get('fetch_interval_minutes', 10))
        if not should_run_task(db_conn, f"fetch_{name}", fetch_interval, now_utc):
            logger.debug(" -> Skip: Interval not met")
            continue
        logger.info(f"Scheduling fetch: '{name}'")
        apis_to_fetch_this_run.append((name, config))

    # Execute fetches
    fetch_results = []
    if apis_to_fetch_this_run:
        logger.info(f"Fetching data for {len(apis_to_fetch_this_run)} endpoints...")
        # ... (asyncio gather logic remains the same) ...
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async def fetch_with_semaphore(session, name, cfg, key):
             async with semaphore:
                 return await fetch_api_data(session, name, cfg, base_url, key)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
            tasks = [fetch_with_semaphore(session, name, config, api_key) for name, config in apis_to_fetch_this_run]
            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process fetch results
    processed_fetches = 0
    successful_raw_inserts = 0
    fetch_errors = 0
    if fetch_results:
        insert_time_utc = datetime.now(pytz.utc)
        logger.info("--- Processing Fetch Results ---")
        for i, result in enumerate(fetch_results):
            processed_fetches += 1
            endpoint_name, config = apis_to_fetch_this_run[i]
            fetch_task_name = f"fetch_{endpoint_name}"
            if isinstance(result, Exception):
                logger.error(f"Fetch Task Error: '{endpoint_name}'. Details logged previously.", exc_info=False)
                fetch_errors += 1
                update_last_run_time(db_conn, fetch_task_name, now_utc) # Update even on error
            elif result is not None:
                _name, _config, data = result
                if insert_raw_data(db_conn, endpoint_name, insert_time_utc, data):
                    successful_raw_inserts += 1
                else: fetch_errors += 1
                # Save JSON file (optional)
                output_filename = os.path.join(DATA_FOLDER, config['output_filename'])
                try:
                    os.makedirs(DATA_FOLDER, exist_ok=True)
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False,
                                  indent=4 if PRETTY_PRINT_JSON else None,
                                  separators=(',', ':') if not PRETTY_PRINT_JSON else None)
                    logger.debug(f"Saved JSON: {os.path.basename(output_filename)}")
                except IOError as e: logger.error(f"JSON write failed for {os.path.basename(output_filename)}: {e}", exc_info=False)
                update_last_run_time(db_conn, fetch_task_name, now_utc) # Update on success/skip
            else: # Fetch failed (non-200, timeout etc)
                fetch_errors += 1
                update_last_run_time(db_conn, fetch_task_name, now_utc) # Update on failure
        logger.info(f"Fetch Results: Processed={processed_fetches}, Inserts={successful_raw_inserts}, Errors={fetch_errors}")

    # --- 2. Run Aggregations ---
    logger.info("--- Checking Aggregation Tasks ---")
    agg_success = 0
    agg_skipped = 0
    agg_errors = 0
    for endpoint_name, config in API_ENDPOINTS.items():
        if not config.get('enabled', False): continue
        levels = config.get('aggregation_levels', [])
        if not levels: continue

        for level in levels:
            if level not in AGGREGATION_INTERVALS: continue
            interval = AGGREGATION_INTERVALS[level]
            agg_task_name = f"aggregate_{level}_{endpoint_name}"
            run_agg = False
            target_ts = None
            start_ts_utc = None
            end_ts_utc = now_utc

            if level == 'daily':
                if now_local.time() >= DAILY_AGGREGATION_TIME_LOCAL:
                    last_run_daily = get_last_run_time(db_conn, agg_task_name)
                    if last_run_daily is None or last_run_daily.astimezone(DEFAULT_TIMEZONE).date() < now_local.date():
                        run_agg = True
                        target_ts = now_local.date() - timedelta(days=1) # Agg for yesterday
                        start_ts_utc = DEFAULT_TIMEZONE.localize(datetime.combine(target_ts, dt_time.min)).astimezone(pytz.utc)
                        end_ts_utc = DEFAULT_TIMEZONE.localize(datetime.combine(target_ts, dt_time.max)).astimezone(pytz.utc)
                    else: logger.debug(f"Skip Daily '{agg_task_name}': Ran today.")
                else: logger.debug(f"Skip Daily '{agg_task_name}': Time not reached.")
            else: # Other intervals
                if should_run_task(db_conn, agg_task_name, interval, now_utc):
                    run_agg = True
                    target_ts = now_utc # Aggregate up to now
                    start_ts_utc = now_utc - interval

            if run_agg:
                logger.info(f"Running Aggregation: '{agg_task_name}' for period ending {target_ts}")
                if calculate_and_store_aggregates(db_conn, endpoint_name, level, config, start_ts_utc, end_ts_utc, target_ts):
                    agg_success += 1
                else: agg_errors += 1
            else: agg_skipped += 1

    logger.info(f"Aggregation Results: Success={agg_success}, Skipped={agg_skipped}, Errors={agg_errors}")

    # --- Finish ---
    try:
        if db_conn:
            db_conn.close()
            logger.info("DB connection closed.")
    except Exception as e: logger.error(f"Error closing DB: {e}", exc_info=True)

    script_end_time = time.monotonic()
    logger.info(f"{COLOR_GREEN}--- Market Data Sync Cycle Finished in {script_end_time - script_start_time:.2f} seconds ---{COLOR_RESET}")

# --- Entry Point ---
if __name__ == "__main__":
    setup_logging()
    # ... (dotenv loading remains same) ...
    try:
        from dotenv import load_dotenv
        script_dir = os.path.dirname(__file__)
        dotenv_path = os.path.join(script_dir, '..', '.env')
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path=dotenv_path)
            logger.info("Loaded .env from project root.")
    except ImportError: logger.debug(".env library not found.")
    except Exception as e: logger.error(f"Error loading .env: {e}", exc_info=True)

    try:
        asyncio.run(main())
    except KeyboardInterrupt: logger.warning("Script interrupted.")
    except Exception as e: logger.critical(f"Unhandled main exception: {e}", exc_info=True)
