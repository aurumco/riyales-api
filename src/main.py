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
from datetime import datetime, time as dt_time, timedelta, date
from logging.handlers import TimedRotatingFileHandler
import traceback
from typing import Dict, Any, Optional, List, Tuple, Union
import re
from decimal import Decimal, InvalidOperation

# --- Configuration ---

# Secrets & Environment Variables
BASE_URL_ENV_VAR: str = "BRS_BASE_URL"
API_KEY_ENV_VAR: str = "BRS_API_KEY"
LOG_LEVEL_ENV_VAR: str = "LOG_LEVEL" #(DEBUG, INFO, ERROR, WARNING, etc.)

# General Settings
LOG_FOLDER: str = "logs"
LOG_FILE_NAME: str = "market_data.log"
LOG_ROTATION_HOURS: int = 48
DATA_FOLDER: str = "data"
DB_FILE: str = os.path.join(DATA_FOLDER, "market_data.duckdb")
REQUEST_TIMEOUT_SECONDS: int = 10
PRETTY_PRINT_JSON: bool = False
MAX_CONCURRENT_REQUESTS: int = 8
TIMEZONE: str = "Asia/Tehran"

# User Agent (Keep updated)
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
HEADERS: Dict[str, str] = {
    "User-Agent": USER_AGENT, "Accept": "application/json, text/plain, */*",
}

# API Endpoint Configuration (Structure remains similar)
# fetch_interval_minutes is now used by the internal scheduler
API_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    # ... (Keep the same endpoint definitions as your last version, ensure 'relative_url' is used) ...
    "gold_currency": {
        "relative_url": "/Api/Market/Gold_Currency.php?key={api_key}",
        "output_filename": "gold_currency.json",
        "fetch_interval_minutes": 10,
        "market_hours_apply": False,
        "enabled": True,
        "aggregation_levels": ["daily", "6h", "12h", "3d", "weekly"], # Define levels for aggregation
        "price_json_path": ["gold", "$[*].price"], # DuckDB JSON path to extract prices
        "symbol_json_path": ["gold", "$[*].symbol"] # DuckDB JSON path to extract symbols
    },
    "crypto": {
        "relative_url": "/Api/Market/Cryptocurrency.php?key={api_key}",
        "output_filename": "cryptocurrency.json",
        "fetch_interval_minutes": 10,
        "market_hours_apply": False,
        "enabled": True,
        "aggregation_levels": ["daily", "6h", "12h", "3d", "weekly"],
        "price_json_path": ["$[*].price_toman"], # Assuming Toman price aggregation
        "symbol_json_path": ["$[*].name"]
    },
    "commodity": {
        "relative_url": "/Api/Market/Commodity.php?key={api_key}",
        "output_filename": "commodity.json",
        "fetch_interval_minutes": 10,
        "market_hours_apply": False,
        "enabled": True,
        "aggregation_levels": ["daily", "6h", "12h", "3d", "weekly"],
        # Commodity needs multiple paths - Aggregation needs enhancement or simplification
        # For simplicity, let's aggregate only precious metals for now
        "price_json_path": ["metal_precious", "$[*].price"],
        "symbol_json_path": ["metal_precious", "$[*].symbol"]
    },
    "tse_ifb_symbols": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=1",
        "output_filename": "tse_ifb_symbols.json",
        "fetch_interval_minutes": 20, # Fetches less often
        "market_hours_apply": True,
        "enabled": True,
        "aggregation_levels": ["daily"],
        "price_json_path": ["$[*].pc"], # Assuming 'pc' (closing price) for aggregation
        "symbol_json_path": ["$[*].l18"] # Assuming 'l18' is the symbol
    },
    # Add other endpoints similarly, defining aggregation_levels and json paths if needed
    # Endpoints disabled or without aggregation_levels won't be aggregated
     "tse_options": { "relative_url": "/Api/Tsetmc/Option.php?key={api_key}", "output_filename": "tse_options.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_nav": { "relative_url": "/Api/Tsetmc/Nav.php?key={api_key}", "output_filename": "tse_nav.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=1", "output_filename": "tse_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "ifb_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=2", "output_filename": "ifb_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "selected_indices": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=3", "output_filename": "selected_indices.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "debt_securities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=4", "output_filename": "debt_securities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "housing_facilities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=5", "output_filename": "housing_facilities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "futures": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=3", "output_filename": "futures.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
}

# Market Hours (Confirm these are accurate)
TSE_MARKET_OPEN_TIME: dt_time = dt_time(8, 45)
TSE_MARKET_CLOSE_TIME: dt_time = dt_time(12, 45)
TSE_MARKET_DAYS: List[int] = [5, 6, 0, 1, 2] # Sat-Wed

# Aggregation Intervals
AGGREGATION_INTERVALS: Dict[str, timedelta] = {
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "daily": timedelta(days=1),
    "3d": timedelta(days=3),
    "weekly": timedelta(weeks=1),
}
DAILY_AGGREGATION_TIME_LOCAL: dt_time = dt_time(0, 5) # Aggregate daily data at 00:05 Tehran time

# --- ANSI Color Codes ---
COLOR_GREEN: str = "\033[32m"
COLOR_RED: str = "\033[31m"
COLOR_YELLOW: str = "\033[33m"
COLOR_BLUE: str = "\033[34m"
COLOR_RESET: str = "\033[0m"

# --- Global Logger ---
logger = logging.getLogger("MarketDataSync")

# --- Utility Functions ---
def mask_url_secrets(url: str) -> str:
    """Masks sensitive query parameters like 'key' in a URL."""
    # Improved regex to handle potential variations
    masked_url = re.sub(r"([?&]key=)[^&]*", r"\1********", url, flags=re.IGNORECASE)
    # Add more parameters to mask if needed:
    # masked_url = re.sub(r"([?&]token=)[^&]*", r"\1********", masked_url, flags=re.IGNORECASE)
    return masked_url

# --- Logging Setup ---
def setup_logging() -> None:
    """Sets up logging with rotation, configurable level, and secure formatting."""
    log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger.setLevel(logging.DEBUG) # Set base level to DEBUG

    if logger.hasHandlers():
        logger.handlers.clear()

    log_dir = LOG_FOLDER
    os.makedirs(log_dir, exist_ok=True)
    cleanup_old_logs(log_dir, LOG_ROTATION_HOURS) # Use hours now
    log_file_path = os.path.join(log_dir, LOG_FILE_NAME)

    # File Handler - Always log DEBUG and above to file
    file_handler = TimedRotatingFileHandler(
        log_file_path, when="H", interval=LOG_ROTATION_HOURS, backupCount=5, # Rotate based on hours
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s|%(levelname)s|%(name)s|%(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)

    # Console Handler - Level controlled by environment variable
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level) # Set level based on ENV var

    class SecureColorFormatter(logging.Formatter):
        level_colors = {
            logging.DEBUG: COLOR_BLUE, logging.INFO: COLOR_GREEN,
            logging.WARNING: COLOR_YELLOW, logging.ERROR: COLOR_RED, logging.CRITICAL: COLOR_RED,
        }
        def format(self, record):
            color = self.level_colors.get(record.levelno, COLOR_RESET)
            # Mask secrets in the message BEFORE formatting
            original_message = record.getMessage()
            masked_message = mask_url_secrets(original_message) # Mask URLs
            # Potentially add more masking here if needed

            log_fmt = f"{color}•{COLOR_RESET} %(asctime)s|{color}%(levelname)-7s{COLOR_RESET}| {color}{masked_message}{COLOR_RESET}"
            # Use a basic formatter instance internally
            _formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")
            # Need to manually set the message on the record for the formatter to use it
            record.message = record.getMessage() # Keep original for potential use
            record.msg = masked_message # Use masked message for output
            formatted_message = _formatter.format(record)
            # Reset msg to original in case other handlers need it (though we clear handlers)
            # record.msg = original_message
            return formatted_message

    console_handler.setFormatter(SecureColorFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info(f"Logging setup complete. Console level: {log_level_str}, File level: DEBUG")

def cleanup_old_logs(log_directory: str, hours_to_keep: int) -> None:
    """Removes log files older than the specified number of hours."""
    cutoff = time.time() - (hours_to_keep * 60 * 60)
    print(f"{COLOR_BLUE}•{COLOR_RESET} Checking for logs older than {hours_to_keep} hours...")
    cleaned_count = 0
    try:
        # Match log files like market_data.log, market_data.log.YYYY-MM-DD_HH-MM-SS, etc.
        for filename in glob.glob(os.path.join(log_directory, LOG_FILE_NAME + "*")):
            if os.path.isfile(filename):
                try:
                    if os.path.getmtime(filename) < cutoff:
                        os.remove(filename)
                        print(f"{COLOR_YELLOW}✓ Cleaned old log file: {os.path.basename(filename)}{COLOR_RESET}")
                        cleaned_count += 1
                except OSError as e:
                    print(f"{COLOR_RED}✗ Error removing log file {os.path.basename(filename)}: {e}{COLOR_RESET}")
        if cleaned_count == 0:
            print(f"{COLOR_GREEN}✓ No old log files found to clean.{COLOR_RESET}")
    except Exception as e:
        print(f"{COLOR_RED}✗ Error during log cleanup scan: {e}{COLOR_RESET}")


# --- Timezone and Market Hours Check (Enhanced Logging) ---
def is_market_open(tz_str: str, open_time: dt_time, close_time: dt_time, market_days: List[int]) -> bool:
    try:
        timezone = pytz.timezone(tz_str)
        now_local = datetime.now(timezone)
        current_time = now_local.time()
        current_weekday = now_local.weekday()
        logger.debug(f"Market Check: Local Time={now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}, Weekday={current_weekday}, Time={current_time}, Market Days={market_days}, Market Hours={open_time}-{close_time}")
        if current_weekday not in market_days:
            logger.debug("Market Status: CLOSED (Outside Market Days)")
            return False
        is_open = open_time <= current_time < close_time
        logger.debug(f"Market Status: {'OPEN' if is_open else 'CLOSED'} (Within Market Days, Checked Against Hours)")
        return is_open
    except Exception as e:
        logger.error(f"Error checking market hours: {e}", exc_info=True)
        return False # Fail safe

# --- Database Operations ---
# Aggregation Tracker Table Schema:
# CREATE TABLE IF NOT EXISTS aggregation_tracker (
#    task_name TEXT PRIMARY KEY, -- e.g., "fetch_tse_ifb_symbols", "aggregate_daily_gold_currency"
#    last_run_timestamp TIMESTAMPTZ
# );

# Raw Data Table Schema Example (for gold_currency_raw):
# CREATE TABLE IF NOT EXISTS gold_currency_raw (
#     timestamp TIMESTAMPTZ PRIMARY KEY,
#     data JSON -- Stores the raw JSON received from API
# );

# Aggregated Data Table Schema Example (for gold_currency_daily):
# CREATE TABLE IF NOT EXISTS gold_currency_daily (
#     timestamp DATE,         -- The date the aggregation represents
#     symbol TEXT,            -- The currency/gold symbol
#     median_price DECIMAL,   -- Calculated median price for the day
#     source_count BIGINT,    -- How many raw data points contributed
#     PRIMARY KEY (timestamp, symbol)
# ); # Similar structure for _6h, _12h, etc. (timestamp would be TIMESTAMPTZ)


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
    """Gets the last run timestamp for a task from the tracker table."""
    try:
        result = conn.execute("SELECT last_run_timestamp FROM aggregation_tracker WHERE task_name = ?", (task_name,)).fetchone()
        if result and result[0]:
            # Ensure the timestamp is timezone-aware (DuckDB might return naive)
            ts = result[0]
            if isinstance(ts, datetime) and ts.tzinfo is None:
                 return pytz.utc.localize(ts) # Assume stored as UTC
            elif isinstance(ts, datetime):
                 return ts # Already timezone-aware
            else:
                 logger.warning(f"Unexpected type for last_run_timestamp for {task_name}: {type(ts)}")
                 return None # Handle unexpected type gracefully
        return None
    except Exception as e:
        logger.error(f"Error getting last run time for '{task_name}': {e}", exc_info=True)
        return None

def update_last_run_time(conn: duckdb.DuckDBPyConnection, task_name: str, timestamp: datetime) -> None:
    """Updates the last run timestamp for a task."""
    try:
        # Use UPSERT logic
        conn.execute("""
            INSERT INTO aggregation_tracker (task_name, last_run_timestamp) VALUES (?, ?)
            ON CONFLICT (task_name) DO UPDATE SET last_run_timestamp = excluded.last_run_timestamp;
            """, (task_name, timestamp))
        logger.debug(f"Updated last run time for '{task_name}' to {timestamp}")
    except Exception as e:
        logger.error(f"Error updating last run time for '{task_name}': {e}", exc_info=True)

def should_run_task(conn: duckdb.DuckDBPyConnection, task_name: str, interval: timedelta, now_utc: datetime) -> bool:
    """Checks if a task is due to run based on its interval and last run time."""
    last_run = get_last_run_time(conn, task_name)
    if last_run is None:
        logger.info(f"Task '{task_name}' never run before or error fetching time. Running now.")
        return True # Run if never run before

    time_since_last_run = now_utc - last_run
    is_due = time_since_last_run >= interval
    logger.debug(f"Task '{task_name}': Last run={last_run}, Time since={time_since_last_run}, Interval={interval}, Due={is_due}")
    return is_due

def ensure_raw_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str) -> bool:
    table_name = f"{endpoint_name}_raw"
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                timestamp TIMESTAMPTZ PRIMARY KEY,
                data JSON
            );""")
        logger.debug(f"Ensured raw table '{table_name}' exists.")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure raw table '{table_name}': {e}", exc_info=True)
        return False

def insert_raw_data(conn: duckdb.DuckDBPyConnection, endpoint_name: str, timestamp: datetime, data: Any) -> bool:
    table_name = f"{endpoint_name}_raw"
    if not ensure_raw_table(conn, endpoint_name):
        return False
    try:
        json_data_str = json.dumps(data, ensure_ascii=False)
        conn.execute(f'INSERT INTO "{table_name}" (timestamp, data) VALUES (?, ?)', (timestamp, json_data_str))
        logger.debug(f"Inserted raw data into '{table_name}' for {timestamp}.")
        return True
    except duckdb.ConstraintException:
         logger.warning(f"Duplicate raw timestamp {timestamp} for '{table_name}'. Skipping.")
         return True # Not a failure
    except Exception as e:
        logger.error(f"Failed to insert raw data into '{table_name}': {e}", exc_info=True)
        return False

# --- Aggregation Logic ---

def ensure_aggregate_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str) -> bool:
    """Ensures an aggregate table exists (e.g., gold_currency_daily)."""
    table_name = f"{endpoint_name}_{level}"
    # Use TIMESTAMPTZ for hourly/daily/etc., DATE only for true 'daily' if preferred
    # Using TIMESTAMPTZ consistently might be simpler.
    timestamp_col_type = "TIMESTAMPTZ" # Or "DATE" for daily level if strict date needed
    if level == "daily":
       timestamp_col_type = "DATE" # Let's use DATE for daily summary

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        timestamp {timestamp_col_type},
        symbol TEXT,
        median_price DECIMAL(18, 6), -- Adjust precision/scale as needed
        source_count BIGINT,
        PRIMARY KEY (timestamp, symbol)
    );"""
    try:
        conn.execute(create_sql)
        logger.debug(f"Ensured aggregate table '{table_name}' exists.")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure aggregate table '{table_name}': {e}", exc_info=True)
        return False

def calculate_and_store_aggregates(
    conn: duckdb.DuckDBPyConnection,
    endpoint_name: str,
    level: str, # 'daily', '6h', '12h', '3d', 'weekly'
    config: Dict[str, Any],
    start_time_utc: datetime,
    end_time_utc: datetime,
    target_timestamp: Union[datetime, date] # The timestamp for the aggregated record
) -> bool:
    """Calculates median prices for a period and stores them."""
    raw_table = f"{endpoint_name}_raw"
    agg_table = f"{endpoint_name}_{level}"
    task_name = f"aggregate_{level}_{endpoint_name}"

    price_path = config.get("price_json_path")
    symbol_path = config.get("symbol_json_path")

    if not price_path or not symbol_path:
        logger.warning(f"Skipping aggregation for '{endpoint_name}': Price or symbol JSON path not configured.")
        return True # Not an error, just config missing

    if not ensure_aggregate_table(conn, endpoint_name, level):
        return False # Stop if table can't be created

    # Construct the complex JSON extraction query
    # This needs careful adjustment based on the actual JSON structure per endpoint
    # Example for structure like gold_currency: { "gold": [ {"symbol": "X", "price": Y}, ... ] }
    # Example for structure like crypto: [ {"name": "X", "price_toman": Y}, ... ]
    try:
        # Determine path structure (nested array vs direct array)
        if isinstance(price_path, list) and len(price_path) == 2 and isinstance(symbol_path, list) and len(symbol_path) == 2:
             # Nested: e.g., ["gold", "$[*].price"]
             array_path = price_path[0] # e.g., "gold"
             price_expr = f"json_extract(item, ['{price_path[1].split('.')[-1]}'])" # item -> 'price'
             symbol_expr = f"json_extract(item, ['{symbol_path[1].split('.')[-1]}'])" # item -> 'symbol'
             json_table_expr = f"json_array_elements(data -> '{array_path}') AS tbl(item)"
        elif isinstance(price_path, list) and len(price_path) == 1 and isinstance(symbol_path, list) and len(symbol_path) == 1:
             # Direct array: e.g., ["$[*].price_toman"]
             price_expr = f"json_extract(item, ['{price_path[0].split('.')[-1]}'])" # item -> 'price_toman'
             symbol_expr = f"json_extract(item, ['{symbol_path[0].split('.')[-1]}'])" # item -> 'name'
             json_table_expr = f"json_array_elements(data) AS tbl(item)"
        else:
             logger.error(f"Unsupported JSON path structure for aggregation: {endpoint_name}")
             return False

        query = f"""
        WITH ExtractedData AS (
            SELECT
                {symbol_expr}::TEXT AS symbol,
                {price_expr} AS price_any -- Keep original type first
            FROM "{raw_table}", {json_table_expr}
            WHERE timestamp >= ? AND timestamp < ?
        ),
        FilteredData AS (
             SELECT
                 symbol,
                 -- Attempt to cast to DECIMAL, handle potential errors/non-numeric values
                 CASE
                     WHEN try_cast(price_any AS VARCHAR) SIMILAR TO '[+-]?[0-9]+([.][0-9]+)?'
                     THEN try_cast(price_any AS DECIMAL(36, 18)) -- Use high precision for intermediate cast
                     ELSE NULL
                 END AS price_decimal
             FROM ExtractedData
             WHERE symbol IS NOT NULL AND price_any IS NOT NULL
        )
        SELECT
            symbol,
            median(price_decimal) AS median_price,
            count(*) AS source_count
        FROM FilteredData
        WHERE price_decimal IS NOT NULL -- Only aggregate valid numbers
        GROUP BY symbol
        HAVING count(*) > 0; -- Only insert if there's data
        """

        logger.debug(f"Executing aggregation query for {agg_table} between {start_time_utc} and {end_time_utc}")
        aggregates = conn.execute(query, (start_time_utc, end_time_utc)).fetchall()
        logger.info(f"Calculated {len(aggregates)} aggregates for {endpoint_name} level {level}.")

        if not aggregates:
             logger.warning(f"No aggregate data found for {endpoint_name} level {level} in the period.")
             # Still update the tracker time so we don't re-run immediately
             update_last_run_time(conn, task_name, datetime.now(pytz.utc))
             return True

        # Insert aggregates into the target table
        # Use UPSERT to handle potential reruns or overlapping periods gracefully
        insert_sql = f"""
        INSERT INTO "{agg_table}" (timestamp, symbol, median_price, source_count) VALUES (?, ?, ?, ?)
        ON CONFLICT (timestamp, symbol) DO UPDATE SET
            median_price = excluded.median_price,
            source_count = excluded.source_count;
        """
        # DuckDB executemany is often faster for bulk inserts
        # Prepare data for executemany
        rows_to_insert = []
        for symbol, median_val, count in aggregates:
             # Ensure median_val is a Decimal or float before inserting
             if isinstance(median_val, (float, int, Decimal)):
                 rows_to_insert.append((target_timestamp, symbol, median_val, count))
             else:
                  logger.warning(f"Median value for {symbol} in {agg_table} is not numeric ({type(median_val)}), skipping row.")

        if rows_to_insert:
             conn.executemany(insert_sql, rows_to_insert)
             logger.info(f"Stored {len(rows_to_insert)} aggregates into {agg_table}.")
        else:
             logger.warning(f"No valid aggregates calculated for {agg_table} to store.")


        # Update tracker *after* successful processing
        update_last_run_time(conn, task_name, datetime.now(pytz.utc))
        return True

    except Exception as e:
        logger.error(f"Error during aggregation for {agg_table}: {e}", exc_info=True)
        return False


# --- API Fetching ---
async def fetch_api_data(
    session: aiohttp.ClientSession,
    endpoint_name: str,
    config: Dict[str, Any],
    base_url: str,
    api_key: str
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    """Fetches data from a single API endpoint asynchronously."""
    relative_url = config['relative_url'].format(api_key=api_key)
    full_url = f"{base_url.rstrip('/')}/{relative_url.lstrip('/')}"
    # Log the URL *only* at DEBUG level and ensure it's masked
    logger.debug(f"Requesting: {endpoint_name} ({mask_url_secrets(full_url)})")
    start_time = time.monotonic()

    try:
        async with session.get(full_url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            elapsed_time = time.monotonic() - start_time
            logger.debug(f"Response: {endpoint_name} ({response.status}) in {elapsed_time:.2f}s")
            if response.status == 200:
                try:
                    data = await response.json(content_type=None)
                    logger.info(f"Success: Fetched data for {endpoint_name}.")
                    return endpoint_name, config, data
                except json.JSONDecodeError as json_err:
                    # Log error without raw response text if possible, or only snippet
                    response_text_snippet = await response.text()
                    response_text_snippet = response_text_snippet[:200] # Limit snippet size
                    logger.error(f"JSON Decode Error: {endpoint_name} ({response.status}). Snippet: '{response_text_snippet}'. Error: {json_err}", exc_info=False) # Less verbose exc_info
                    return None
                except Exception as e:
                    logger.error(f"Response Processing Error: {endpoint_name} ({response.status}). Error: {e}", exc_info=True)
                    return None
            else:
                # Log error without raw response text if possible, or only snippet
                response_text_snippet = await response.text()
                response_text_snippet = response_text_snippet[:200] # Limit snippet size
                logger.error(f"API Request Failed: {endpoint_name} ({response.status}). Snippet: '{response_text_snippet}'")
                return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout Error ({REQUEST_TIMEOUT_SECONDS}s): {endpoint_name}", exc_info=False)
        return None
    except aiohttp.ClientError as client_err:
        logger.error(f"Client Error: {endpoint_name}. Error: {client_err}", exc_info=False) # Less verbose exc_info
        return None
    except Exception as e:
        logger.error(f"Unexpected Fetch Error: {endpoint_name}. Error: {e}", exc_info=True)
        return None


# --- Main Execution Logic ---
async def main():
    script_start_time = time.monotonic()
    logger.info(f"{COLOR_GREEN}--- Starting Market Data Sync Cycle ---{COLOR_RESET}")

    # --- Get Secrets ---
    api_key = os.getenv(API_KEY_ENV_VAR)
    base_url = os.getenv(BASE_URL_ENV_VAR)
    if not api_key or not base_url:
        logger.critical("CRITICAL: BRS_API_KEY or BRS_BASE_URL environment variable not set.")
        return

    # --- DB Connection ---
    db_conn = get_db_connection(DB_FILE)
    if db_conn is None:
        logger.critical("Halting cycle: Database connection failed.")
        return

    # --- Task Scheduling & Execution ---
    now_utc = datetime.now(pytz.utc)
    logger.debug(f"Current UTC time: {now_utc}")
    tehran_tz = pytz.timezone(TIMEZONE)
    now_local = now_utc.astimezone(tehran_tz)
    logger.debug(f"Current Local time ({TIMEZONE}): {now_local}")

    # 1. Fetch Raw Data
    apis_to_fetch_this_run: List[Tuple[str, Dict[str, Any]]] = []
    for name, config in API_ENDPOINTS.items():
        logger.debug(f"Checking fetch task: '{name}'")
        if not config.get('enabled', False):
            logger.debug(f" -> Skipped: Disabled.")
            continue

        # Check market hours if applicable
        if config.get('market_hours_apply', False):
            if not is_market_open(TIMEZONE, TSE_MARKET_OPEN_TIME, TSE_MARKET_CLOSE_TIME, TSE_MARKET_DAYS):
                logger.debug(f" -> Skipped: Market Closed.")
                continue

        # Check interval using the tracker
        fetch_interval = timedelta(minutes=config.get('fetch_interval_minutes', 10))
        fetch_task_name = f"fetch_{name}"
        if not should_run_task(db_conn, fetch_task_name, fetch_interval, now_utc):
             logger.debug(f" -> Skipped: Interval not met.")
             continue

        logger.info(f"Scheduling fetch for: '{name}'")
        apis_to_fetch_this_run.append((name, config))
        # Tentatively update tracker - prevents re-running immediately if script crashes
        # A more robust approach might update *after* successful fetch/insert.
        # Let's update *after* the fetch attempt below.


    # Execute fetches
    fetch_results = []
    if apis_to_fetch_this_run:
        logger.info(f"Fetching data for {len(apis_to_fetch_this_run)} endpoints...")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async def fetch_with_semaphore(session, name, cfg, key):
             async with semaphore:
                 return await fetch_api_data(session, name, cfg, base_url, key)

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
            tasks = [fetch_with_semaphore(session, name, config, api_key) for name, config in apis_to_fetch_this_run]
            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)
    else:
        logger.info("No APIs due for fetching in this cycle.")


    # Process fetch results and insert raw data
    processed_fetch_count = 0
    successful_raw_inserts = 0
    fetch_errors = 0
    fetched_endpoint_names = [] # Keep track of successful fetches for aggregation later

    if fetch_results:
        insert_time_utc = datetime.now(pytz.utc) # Timestamp for this batch of inserts
        for i, result in enumerate(fetch_results):
            endpoint_name, config = apis_to_fetch_this_run[i] # Get corresponding config
            fetch_task_name = f"fetch_{endpoint_name}"

            if isinstance(result, Exception):
                logger.error(f"Fetch Task Error: '{endpoint_name}'. Error: {result}", exc_info=True)
                fetch_errors += 1
                # Update tracker even on error so it doesn't retry immediately
                update_last_run_time(db_conn, fetch_task_name, now_utc)

            elif result is not None:
                _name, _config, data = result # Unpack result (name/config should match)
                # Insert Raw Data
                if insert_raw_data(db_conn, endpoint_name, insert_time_utc, data):
                    successful_raw_inserts += 1
                    fetched_endpoint_names.append(endpoint_name) # Mark as fetched
                else:
                    fetch_errors += 1 # Count insert failure as an error
                # Save JSON file (optional, could be removed if DB is primary)
                output_filename = os.path.join(DATA_FOLDER, config['output_filename'])
                try:
                    os.makedirs(DATA_FOLDER, exist_ok=True)
                    with open(output_filename, 'w', encoding='utf-8') as f:
                         json.dump(data, f, ensure_ascii=False,
                                   indent=4 if PRETTY_PRINT_JSON else None,
                                   separators=(',', ':') if not PRETTY_PRINT_JSON else None)
                    logger.debug(f"Saved JSON: {output_filename}")
                except IOError as e:
                    logger.error(f"Failed to write JSON file {output_filename}: {e}", exc_info=True)
                    # Don't count this as a fetch error necessarily, but log it

                # Update tracker after successful attempt (fetch+insert or fetch+skip duplicate)
                update_last_run_time(db_conn, fetch_task_name, now_utc)

            else:
                # Fetch failed (e.g., non-200 status, timeout), already logged
                fetch_errors += 1
                update_last_run_time(db_conn, fetch_task_name, now_utc) # Update tracker

            processed_fetch_count +=1

        logger.info(f"Fetch processing done. Processed: {processed_fetch_count}, Raw Inserts: {successful_raw_inserts}, Errors: {fetch_errors}")


    # 2. Run Aggregations
    logger.info("--- Starting Aggregation Checks ---")
    aggregation_success = 0
    aggregation_skipped = 0
    aggregation_errors = 0

    # Iterate through ALL endpoints to check if *any* aggregation is due
    for endpoint_name, config in API_ENDPOINTS.items():
        if not config.get('enabled', False):
            continue # Skip disabled endpoints

        levels_to_aggregate = config.get('aggregation_levels', [])
        if not levels_to_aggregate:
             logger.debug(f"No aggregation levels configured for '{endpoint_name}'.")
             continue

        for level in levels_to_aggregate:
            if level not in AGGREGATION_INTERVALS:
                 logger.warning(f"Unknown aggregation level '{level}' for endpoint '{endpoint_name}'. Skipping.")
                 continue

            interval = AGGREGATION_INTERVALS[level]
            agg_task_name = f"aggregate_{level}_{endpoint_name}"

            # Special handling for daily aggregation trigger time
            run_aggregation = False
            target_timestamp = None # The timestamp the aggregate represents

            if level == 'daily':
                 # Should run if it's past midnight (aggregation time) and hasn't run for today yet
                 last_run_daily = get_last_run_time(db_conn, agg_task_name)
                 # Check if current local time is past the trigger time
                 if now_local.time() >= DAILY_AGGREGATION_TIME_LOCAL:
                      # Check if it has already run today (based on last run time)
                      if last_run_daily is None or last_run_daily.astimezone(tehran_tz).date() < now_local.date():
                           run_aggregation = True
                           # Aggregate data for the *previous* day
                           aggregation_date = now_local.date() - timedelta(days=1)
                           start_time_utc = tehran_tz.localize(datetime.combine(aggregation_date, dt_time.min)).astimezone(pytz.utc)
                           end_time_utc = tehran_tz.localize(datetime.combine(aggregation_date, dt_time.max)).astimezone(pytz.utc)
                           target_timestamp = aggregation_date # Store as DATE
                      else:
                           logger.debug(f"Task '{agg_task_name}': Already run today ({last_run_daily.astimezone(tehran_tz).date()}).")
                 else:
                      logger.debug(f"Task '{agg_task_name}': Daily aggregation time ({DAILY_AGGREGATION_TIME_LOCAL}) not yet reached.")

            else: # For 6h, 12h, 3d, weekly intervals
                 if should_run_task(db_conn, agg_task_name, interval, now_utc):
                      run_aggregation = True
                      # Aggregate data for the interval ending *now*
                      end_time_utc = now_utc
                      start_time_utc = now_utc - interval
                      target_timestamp = now_utc # Store aggregation timestamp as TIMESTAMPTZ

            # Execute aggregation if needed
            if run_aggregation:
                 logger.info(f"Running aggregation task: '{agg_task_name}' for period ending {target_timestamp}")
                 if calculate_and_store_aggregates(db_conn, endpoint_name, level, config, start_time_utc, end_time_utc, target_timestamp):
                      aggregation_success += 1
                 else:
                      aggregation_errors += 1
                      # Don't update tracker on error to allow retry next cycle
            else:
                 aggregation_skipped += 1
                 logger.debug(f"Aggregation task '{agg_task_name}' skipped (not due or already run).")

    logger.info(f"Aggregation checks done. Success: {aggregation_success}, Skipped: {aggregation_skipped}, Errors: {aggregation_errors}")


    # --- Finish ---
    try:
        if db_conn:
            # Optional: Run VACUUM or OPTIMIZE if DB grows large (infrequently)
            # db_conn.execute("VACUUM;")
            db_conn.close()
            logger.info("Database connection closed.")
    except Exception as e:
        logger.error(f"Error closing DB connection: {e}", exc_info=True)

    script_end_time = time.monotonic()
    logger.info(f"{COLOR_GREEN}--- Market Data Sync Cycle Finished in {script_end_time - script_start_time:.2f} seconds ---{COLOR_RESET}")

# --- Script Entry Point ---
if __name__ == "__main__":
    setup_logging() # Setup logging first

    # Load .env (for local dev)
    try:
        from dotenv import load_dotenv
        script_dir = os.path.dirname(__file__)
        dotenv_path = os.path.join(script_dir, '..', '.env') # Assumes .env is in root
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path=dotenv_path)
            logger.info("Loaded environment variables from project root .env file.")
    except ImportError:
        logger.debug(".env library not installed, skipping load.") # Debug level
    except Exception as e:
        logger.error(f"Error loading .env file: {e}", exc_info=True)

    # Run main async loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Script execution interrupted by user.")
    except Exception as e:
        logger.critical(f"Unhandled exception in main execution scope: {e}", exc_info=True)
        # Consider exiting with error code for Actions
        # import sys
        # sys.exit(1)
