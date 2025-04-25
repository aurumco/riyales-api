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
# Removed TimedRotatingFileHandler, using basic FileHandler per run
import traceback
from typing import Dict, Any, Optional, List, Tuple, Union

# --- Configuration ---

# Secrets & Environment Variables
BASE_URL_ENV_VAR: str = "BRS_BASE_URL"    # Base URL for the API provider (e.g., "https://brsapi.ir")
API_KEY_ENV_VAR: str = "BRS_API_KEY"      # API Key for authentication

# --- LOG LEVEL Environment Variable ---
# Controls the verbosity of logs sent to the CONSOLE.
# Set this environment variable to one of the following values (case-insensitive):
# - DEBUG: Show detailed diagnostic information (API calls, checks, query details, etc.). Useful for troubleshooting.
# - INFO: Show informational messages (cycle start/end, successful operations, scheduled tasks). Recommended for normal monitoring.
# - WARNING: Show warnings about potential issues (e.g., skipped data, non-critical errors, data validation issues).
# - ERROR: Show only error messages when an operation fails.
# - CRITICAL: Show only critical errors that might prevent the script from continuing (e.g., DB connection failure, missing essential config).
# If this variable is not set or has an invalid value, it defaults to 'INFO'.
# Note: The log FILE specified by LOG_FOLDER/LOG_FILE_NAME always captures DEBUG level and above.
LOG_LEVEL_ENV_VAR: str = "LOG_LEVEL"

# General Settings
LOG_FOLDER: str = "logs"                  # Directory for log files (relative to script execution)
DATA_FOLDER: str = "data"                 # Directory for output JSON and DB (relative to script execution)
AGGREGATE_JSON_FOLDER: str = os.path.join(DATA_FOLDER, "aggregates") # Subfolder for aggregate JSONs for charting
DB_FILE: str = os.path.join(DATA_FOLDER, "market_data.duckdb") # Path to the DuckDB database file
REQUEST_TIMEOUT_SECONDS: int = 15         # Timeout for individual API requests (in seconds)
PRETTY_PRINT_JSON: bool = False           # Format latest JSON output with indentation (True) or compact (False)
MAX_CONCURRENT_REQUESTS: int = 10         # Max simultaneous API requests via aiohttp
TIMEZONE: str = "Asia/Tehran"             # Default timezone for local operations (e.g., market hours check)
DEFAULT_TIMEZONE = pytz.timezone(TIMEZONE)# Cached timezone object

# User Agent (Mimic a recent browser, update periodically)
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
HEADERS: Dict[str, str] = { "User-Agent": USER_AGENT, "Accept": "application/json, text/plain, */*" }

# --- API Endpoint Configuration ---
# Structure defining how to fetch and process data for each API endpoint.
# Keys:
#   'relative_url': URL path relative to BASE_URL, {api_key} is replaced.
#   'output_filename': Filename for the latest raw data JSON in DATA_FOLDER.
#   'fetch_interval_minutes': Minimum time between fetch attempts.
#   'market_hours_apply': If True, fetch only during TSE_MARKET hours.
#   'enabled': Set to False to temporarily disable fetching and aggregation for this endpoint.
#   'aggregation_levels': List of aggregation periods ('6h', '12h', 'daily', '3d', 'weekly').
#   'price_json_path': JSON path (e.g., '$.price') within each array item to find the price value.
#   'symbol_json_path': JSON path (e.g., '$.symbol') within each array item for the identifier.
#   'array_base_paths': List of JSON paths (e.g., '$.data.items', '$') pointing to arrays containing the price/symbol data.
API_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "gold_currency": {
        "relative_url": "/Api/Market/Gold_Currency.php?key={api_key}", "output_filename": "gold_currency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["6h", "12h", "daily", "3d", "weekly"],
        "price_json_path": "$.price", "symbol_json_path": "$.symbol",
        "array_base_paths": ["$.gold", "$.currency"], # Handles nested arrays
    },
    "crypto": {
        "relative_url": "/Api/Market/Cryptocurrency.php?key={api_key}", "output_filename": "cryptocurrency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["6h", "12h", "daily", "3d", "weekly"],
        "price_json_path": "$.price_toman", "symbol_json_path": "$.name",
        "array_base_paths": ["$"], # '$' indicates the root JSON is the array
    },
    "commodity": {
        "relative_url": "/Api/Market/Commodity.php?key={api_key}", "output_filename": "commodity.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["6h", "12h", "daily", "3d", "weekly"],
        "price_json_path": "$.price", "symbol_json_path": "$.symbol",
        "array_base_paths": ["$.metal_precious"], # Only process precious metals
    },
    "tse_ifb_symbols": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=1", "output_filename": "tse_ifb_symbols.json",
        "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True,
        "aggregation_levels": ["daily"], # Typically only daily aggregation makes sense here
        "price_json_path": "$.pc", # Closing price
        "symbol_json_path": "$.l18", # Symbol name
        "array_base_paths": ["$"],
    },
     # Disabled endpoints (can be enabled by setting 'enabled': True)
     "tse_options": { "relative_url": "/Api/Tsetmc/Option.php?key={api_key}", "output_filename": "tse_options.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_nav": { "relative_url": "/Api/Tsetmc/Nav.php?key={api_key}", "output_filename": "tse_nav.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=1", "output_filename": "tse_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "ifb_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=2", "output_filename": "ifb_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "selected_indices": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=3", "output_filename": "selected_indices.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     # Enabled endpoints without aggregation configured (can be added)
     "debt_securities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=4", "output_filename": "debt_securities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "housing_facilities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=5", "output_filename": "housing_facilities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "futures": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=3", "output_filename": "futures.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
}

# Market Hours (Tehran Stock Exchange - Adjust if official hours change)
TSE_MARKET_OPEN_TIME: dt_time = dt_time(8, 45)   # Market opening time (local Tehran time)
TSE_MARKET_CLOSE_TIME: dt_time = dt_time(12, 45) # Market closing time (local Tehran time)
TSE_MARKET_DAYS: List[int] = [5, 6, 0, 1, 2]     # Active weekdays (0=Monday, 5=Saturday, 6=Sunday)

# Aggregation Intervals & Daily Trigger Time
AGGREGATION_INTERVALS: Dict[str, timedelta] = {
    "6h": timedelta(hours=6), "12h": timedelta(hours=12), "daily": timedelta(days=1),
    "3d": timedelta(days=3), "weekly": timedelta(weeks=1),
}
# Time (local) after midnight when daily aggregation for the *previous* day runs.
DAILY_AGGREGATION_TIME_LOCAL: dt_time = dt_time(0, 5) # e.g., 5 minutes past midnight Tehran time

# --- Constants ---
COLOR_GREEN: str = "\033[32m"
COLOR_RED: str = "\033[31m"
COLOR_YELLOW: str = "\033[33m"
COLOR_BLUE: str = "\033[34m"
COLOR_RESET: str = "\033[0m"
# DuckDB DECIMAL precision and scale for storing prices
DECIMAL_PRECISION = 18 # Total digits
DECIMAL_SCALE = 6    # Digits after decimal point

# --- Global Logger ---
logger = logging.getLogger("MarketDataSync") # Logger instance configured in setup_logging()

# --- Utility Function ---
def mask_string(s: Optional[Any]) -> str:
    """Masks potentially sensitive parts (keys, tokens) in strings for safe logging."""
    if s is None: return "None"
    s = str(s) # Ensure input is string
    s = re.sub(r"key=([^&?\s]+)", "key=********", s, flags=re.IGNORECASE)
    s = re.sub(r"token=([^&?\s]+)", "token=********", s, flags=re.IGNORECASE)
    s = re.sub(r"(Authorization\s*:\s*)(\w+\s+)\S+", r"\1\2********", s, flags=re.IGNORECASE)
    return s

# --- Logging Setup ---
def setup_logging() -> None:
    """Configures logging: Creates a new log file per run, sets up console handler based on ENV."""
    # Determine console log level from environment variable
    log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, 'INFO').upper()
    try:
        log_level = getattr(logging, log_level_str) # Convert string to logging level object
    except AttributeError:
        print(f"{COLOR_YELLOW}Warning: Invalid LOG_LEVEL '{log_level_str}'. Defaulting to INFO.{COLOR_RESET}")
        log_level = logging.INFO
        log_level_str = 'INFO'

    # Set logger base level to DEBUG to allow all messages to reach handlers
    logger.setLevel(logging.DEBUG)
    # Clear any previous handlers (important if script is re-run in same process)
    if logger.hasHandlers(): logger.handlers.clear()

    os.makedirs(LOG_FOLDER, exist_ok=True) # Ensure log directory exists

    # --- File Handler (DEBUG level, new file per run, no colors) ---
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(LOG_FOLDER, f"market_data_{run_timestamp}.log")
    try:
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG) # Always log DEBUG+ to file
        file_formatter = logging.Formatter(
            "%(asctime)s|%(levelname)-7s|%(name)s|%(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"{COLOR_RED}Error setting up file logger ({log_file_path}): {e}{COLOR_RESET}") # Use print as logger might fail

    # --- Console Handler (Level from ENV, with colors and masking) ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level) # Set handler level from ENV variable

    class SecureColorFormatter(logging.Formatter):
        """Custom formatter to add colors and mask sensitive data for console output."""
        level_colors = {
            logging.DEBUG: COLOR_BLUE, logging.INFO: COLOR_GREEN,
            logging.WARNING: COLOR_YELLOW, logging.ERROR: COLOR_RED, logging.CRITICAL: COLOR_RED,
        }
        def format(self, record):
            color = self.level_colors.get(record.levelno, COLOR_RESET)
            # Mask the original message before formatting
            masked_message = mask_string(record.getMessage())
            # Define the format string for console output
            log_fmt = f"{color}•{COLOR_RESET} %(asctime)s|{color}%(levelname)-7s{COLOR_RESET}| {color}{masked_message}{COLOR_RESET}"
            _formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")
            # Create a temporary record to apply the masked message without modifying the original
            temp_record = logging.makeLogRecord(record.__dict__)
            temp_record.msg = masked_message
            temp_record.message = temp_record.getMessage() # Needed for formatter
            return _formatter.format(temp_record)

    console_handler.setFormatter(SecureColorFormatter())
    logger.addHandler(console_handler)

    # Cleanup old logs after setting up handlers for the current run
    cleanup_old_logs(LOG_FOLDER, 12) # Keep logs only for the last 12 hours

    # Log initial status messages
    logger.info(f"Logging setup complete. Target Console Level: {log_level_str}, File Level: DEBUG")
    logger.info(f"Current log file: {log_file_path}")
    # Add a debug message explaining how to control the console level
    logger.debug(f"Console log level is controlled by the '{LOG_LEVEL_ENV_VAR}' environment variable.")

def cleanup_old_logs(log_directory: str, hours_to_keep: int) -> None:
    """Removes log files matching 'market_data_*.log' pattern older than specified hours."""
    cutoff_time = datetime.now() - timedelta(hours=hours_to_keep)
    logger.debug(f"Checking for logs older than {hours_to_keep} hours ({cutoff_time.isoformat()}) in '{log_directory}'...")
    cleaned_count = 0
    try:
        log_pattern = os.path.join(log_directory, "market_data_*.log")
        for filename in glob.glob(log_pattern):
            if os.path.isfile(filename):
                try:
                    # Try parsing timestamp from filename
                    match = re.search(r"market_data_(\d{8}_\d{6})\.log", os.path.basename(filename))
                    file_is_old = False
                    if match:
                        file_dt = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                        if file_dt < cutoff_time: file_is_old = True
                    else:
                        # Fallback: Use modification time if name doesn't match
                        if datetime.fromtimestamp(os.path.getmtime(filename)) < cutoff_time:
                            file_is_old = True
                            logger.warning(f"Log file '{os.path.basename(filename)}' name mismatch, checking mtime.")

                    if file_is_old:
                        os.remove(filename)
                        logger.info(f"Cleaned old log file: {os.path.basename(filename)}")
                        cleaned_count += 1
                except Exception as e:
                    logger.error(f"Error processing log file {os.path.basename(filename)} during cleanup: {e}", exc_info=False)
        logger.debug(f"Log cleanup finished. Removed {cleaned_count} old files.")
    except Exception as e:
        logger.error(f"Log cleanup scan failed: {e}", exc_info=True) # Error during glob listing

# --- Timezone & Market Check ---
def is_market_open(tz: pytz.BaseTzInfo, open_time: dt_time, close_time: dt_time, market_days: List[int]) -> bool:
    """Checks if the current time is within specified market hours and days."""
    try:
        now_local = datetime.now(tz)
        current_time = now_local.time()
        current_weekday = now_local.weekday()
        logger.debug(f"Market Check: LocalTime={now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}, WDay={current_weekday}, Time={current_time} vs {open_time}-{close_time}, Days={market_days}")
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
def get_db_connection(db_path: str) -> Optional[duckdb.DuckDBPyConnection]:
    """Establishes DuckDB connection, ensures data directory and tracker table exist."""
    try:
        logger.debug(f"Connecting to DB: {db_path}")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = duckdb.connect(database=db_path, read_only=False)
        conn.execute("SELECT 42;") # Test connection
        logger.info("DB connection successful.")
        # Ensure task tracker table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS aggregation_tracker (
                task_name TEXT PRIMARY KEY,
                last_run_timestamp TIMESTAMPTZ NOT NULL
            );
        """)
        logger.debug("Ensured aggregation_tracker table exists.")
        return conn
    except Exception as e:
        logger.critical(f"CRITICAL: DB Connect/Setup failed for '{db_path}': {e}", exc_info=True)
        return None

def get_last_run_time(conn: duckdb.DuckDBPyConnection, task_name: str) -> Optional[datetime]:
    """Retrieves the last successful run timestamp (UTC) for a given task."""
    try:
        result = conn.execute("SELECT last_run_timestamp FROM aggregation_tracker WHERE task_name = ?", (task_name,)).fetchone()
        if result and result[0]:
            ts = result[0]
            if isinstance(ts, datetime):
                # Ensure timezone-aware UTC
                if ts.tzinfo is None: return pytz.utc.localize(ts)
                return ts.astimezone(pytz.utc)
            logger.warning(f"Unexpected type {type(ts)} for last_run_timestamp for '{task_name}'.")
        return None
    except Exception as e:
        logger.error(f"Error getting last run time for '{task_name}': {e}", exc_info=True)
        return None

def update_last_run_time(conn: duckdb.DuckDBPyConnection, task_name: str, timestamp: datetime) -> None:
    """Updates or inserts the last run timestamp (converts to UTC) for a task."""
    try:
        # Ensure timestamp is UTC before storing
        if timestamp.tzinfo is None: timestamp = DEFAULT_TIMEZONE.localize(timestamp).astimezone(pytz.utc)
        elif timestamp.tzinfo != pytz.utc: timestamp = timestamp.astimezone(pytz.utc)

        conn.execute("""
            INSERT INTO aggregation_tracker (task_name, last_run_timestamp) VALUES (?, ?)
            ON CONFLICT (task_name) DO UPDATE SET last_run_timestamp = excluded.last_run_timestamp;
            """, (task_name, timestamp))
        logger.debug(f"Updated last run time for '{task_name}' to {timestamp.isoformat()}")
    except Exception as e:
        logger.error(f"Error updating last run time for '{task_name}': {e}", exc_info=True)

def should_run_task(conn: duckdb.DuckDBPyConnection, task_name: str, interval: timedelta, now_utc: datetime) -> bool:
    """Determines if a task is due based on its last run time (UTC) and interval."""
    last_run_utc = get_last_run_time(conn, task_name)
    if last_run_utc is None:
        logger.info(f"Task '{task_name}' is due: Never run before.")
        return True
    time_since_last_run = now_utc - last_run_utc
    is_due = time_since_last_run >= interval
    logger.debug(f"Task '{task_name}': LastRunUTC={last_run_utc.isoformat()}, TimeSince={time_since_last_run}, Interval={interval}, IsDue={is_due}")
    return is_due

def ensure_table(conn: duckdb.DuckDBPyConnection, create_sql: str, table_name: str) -> bool:
    """Executes a CREATE TABLE IF NOT EXISTS statement safely."""
    try:
        conn.execute(create_sql)
        logger.debug(f"Ensured table '{table_name}' exists.")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure table '{table_name}': {e}", exc_info=True)
        return False

def ensure_raw_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str) -> bool:
    """Ensures the _raw table for storing raw JSON data exists."""
    table_name = f"{endpoint_name}_raw"
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (timestamp TIMESTAMPTZ PRIMARY KEY, data JSON NOT NULL);'
    return ensure_table(conn, sql, table_name)

def ensure_aggregate_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str) -> bool:
    """Ensures the _level table for storing aggregated data exists."""
    table_name = f"{endpoint_name}_{level}"
    ts_type = "DATE" if level == "daily" else "TIMESTAMPTZ"
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        timestamp {ts_type} NOT NULL, symbol TEXT NOT NULL,
        median_price DECIMAL({DECIMAL_PRECISION}, {DECIMAL_SCALE}), source_count BIGINT,
        PRIMARY KEY (timestamp, symbol)
    );"""
    return ensure_table(conn, sql, table_name)

def insert_raw_data(conn: duckdb.DuckDBPyConnection, endpoint_name: str, timestamp: datetime, data: Any) -> bool:
    """Inserts the raw fetched JSON data (as string) into the corresponding _raw table."""
    table_name = f"{endpoint_name}_raw"
    if not ensure_raw_table(conn, endpoint_name): return False
    # Ensure timestamp is UTC
    if timestamp.tzinfo is None: timestamp = pytz.utc.localize(timestamp)
    else: timestamp = timestamp.astimezone(pytz.utc)
    try:
        json_data_str = json.dumps(data, ensure_ascii=False)
        conn.execute(f'INSERT INTO "{table_name}" (timestamp, data) VALUES (?, ?)', (timestamp, json_data_str))
        logger.debug(f"Inserted raw data into '{table_name}' for {timestamp.isoformat()}.")
        return True
    except duckdb.ConstraintException:
         logger.warning(f"Duplicate raw timestamp {timestamp.isoformat()} for '{table_name}'. Skipping insert.")
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
    """Calculates median price for symbols over a period and stores it."""
    raw_table = f"{endpoint_name}_raw"
    agg_table = f"{endpoint_name}_{level}"
    agg_task_name = f"aggregate_{level}_{endpoint_name}"

    logger.info(f"Starting aggregation for '{agg_table}' (Period: {start_time_utc.isoformat()} to {end_time_utc.isoformat()})")

    # Check raw table existence and if it has data for the period
    try:
         table_exists_check = conn.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{raw_table}' LIMIT 1").fetchone()
         if not table_exists_check:
              logger.warning(f"Skip Aggregation for {agg_table}: Raw table '{raw_table}' does not exist."); return True
         count_result = conn.execute(f'SELECT COUNT(*) FROM "{raw_table}" WHERE timestamp >= ? AND timestamp < ?', (start_time_utc, end_time_utc)).fetchone()
         if not count_result or count_result[0] == 0:
              logger.info(f"Skip Aggregation for {agg_table}: No raw data in period."); update_last_run_time(conn, agg_task_name, datetime.now(pytz.utc)); return True
         logger.debug(f"Found {count_result[0]} raw points for '{raw_table}' in period.")
    except Exception as e:
         logger.error(f"Error checking raw data for {agg_table}: {e}", exc_info=True); return False

    if not ensure_aggregate_table(conn, endpoint_name, level): return False

    price_path = config.get("price_json_path", "$.price")
    symbol_path = config.get("symbol_json_path", "$.symbol")
    array_base_paths = config.get("array_base_paths", ["$"])

    all_aggregates = []
    query_success = True

    for base_path in array_base_paths:
        logger.debug(f"Aggregating from base path: '{base_path}' for table '{agg_table}'")
        try:
            # *** Corrected Query using FROM ..., unnest() directly on json_extract result ***
            # This relies on DuckDB correctly interpreting the extracted JSON array for unnest.
            query = f"""
            WITH ExtractedArrays AS (
                -- 1. Extract the potential array using the base path
                SELECT json_extract(data, '{base_path}') as item_array_json
                FROM "{raw_table}"
                WHERE timestamp >= ? AND timestamp < ?
            ),
            FilteredArrays AS (
                -- 2. Filter: Only keep rows where the extracted value IS a JSON array
                SELECT item_array_json
                FROM ExtractedArrays
                WHERE json_type(item_array_json) = 'ARRAY'
            ),
            UnnestedItems AS (
                -- 3. Unnest the JSON array directly using FROM clause expansion
                SELECT item
                FROM FilteredArrays, unnest(item_array_json) AS t(item) -- Should work if item_array_json is a valid JSON array
            ),
            ExtractedData AS (
                -- 4. Extract data from each unnested JSON 'item'
                SELECT
                    json_extract_string(item, '{symbol_path}') AS symbol,
                    json_extract_string(item, '{price_path}') AS price_str
                FROM UnnestedItems
                WHERE json_type(item) = 'OBJECT' -- Process only if item is an object
            ),
            FilteredData AS (
                -- 5. Filter valid data and attempt to cast price to DECIMAL
                SELECT
                    symbol,
                    try_cast(price_str AS DECIMAL({DECIMAL_PRECISION*2}, {DECIMAL_SCALE*2})) AS price_decimal
                FROM ExtractedData
                WHERE symbol IS NOT NULL AND price_str IS NOT NULL AND symbol != '' AND price_str != ''
            )
            -- 6. Final aggregation (median and count)
            SELECT
                symbol,
                median(price_decimal) AS median_price,
                count(*) AS source_count
            FROM FilteredData
            WHERE price_decimal IS NOT NULL -- Exclude rows where price couldn't be cast
            GROUP BY symbol;
            """
            logger.debug(f"Executing agg query for {agg_table} (path: {base_path}) period: {start_time_utc.isoformat()} - {end_time_utc.isoformat()}")
            # print(f"DEBUG QUERY for {agg_table} path {base_path}:\n{query}\nParams: {start_time_utc}, {end_time_utc}") # Uncomment for query debugging
            aggregates = conn.execute(query, (start_time_utc, end_time_utc)).fetchall()
            logger.info(f"Aggregated {len(aggregates)} symbols from path '{base_path}' for {agg_table}")
            all_aggregates.extend(aggregates)

        except (duckdb.BinderException, duckdb.CatalogException, duckdb.ParserException, duckdb.InvalidInputException) as db_query_err:
             # Log specific DuckDB errors clearly
             logger.error(f"DuckDB Query Error during aggregation for {agg_table} (path: {base_path}): {type(db_query_err).__name__} - {db_query_err}", exc_info=True)
             logger.error(f"Failed Query Context: Table='{raw_table}', BasePath='{base_path}', PricePath='{price_path}', SymbolPath='{symbol_path}'")
             query_success = False
             break # Stop processing this endpoint/level
        except Exception as e:
            logger.error(f"General Error during aggregation query for {agg_table} (path: {base_path}): {e}", exc_info=True)
            query_success = False
            break

    if not query_success:
        logger.error(f"Aggregation failed for {endpoint_name} level {level} due to query error(s).")
        return False # Don't update tracker

    # --- Insert Aggregates ---
    # (Insert logic remains the same - batch insert/update validated data)
    if not all_aggregates:
        logger.warning(f"No valid aggregate data calculated for {agg_table} in period.")
    else:
        insert_sql = f"""
        INSERT INTO "{agg_table}" (timestamp, symbol, median_price, source_count) VALUES (?, ?, ?, ?)
        ON CONFLICT (timestamp, symbol) DO UPDATE SET
            median_price = excluded.median_price, source_count = excluded.source_count;
        """
        rows_to_insert = []
        invalid_median_count = 0
        for symbol, median_val, count in all_aggregates:
             if median_val is None: logger.debug(f"Median NULL for {symbol} in {agg_table}, skipping."); invalid_median_count += 1; continue
             try:
                  final_median = Decimal(str(median_val)).quantize(Decimal('1e-' + str(DECIMAL_SCALE)), rounding=ROUND_HALF_UP)
                  rows_to_insert.append((target_timestamp, symbol, final_median, count))
             except (InvalidOperation, TypeError, ValueError) as q_err:
                  logger.warning(f"Could not convert/quantize median {median_val} ({type(median_val)}) for '{symbol}' in {agg_table}: {q_err}"); invalid_median_count += 1

        if rows_to_insert:
            try:
                with conn.transaction(): conn.executemany(insert_sql, rows_to_insert)
                logger.info(f"Stored/Updated {len(rows_to_insert)} aggregates into {agg_table}. ({invalid_median_count} invalid medians skipped)")
            except Exception as e: logger.error(f"Error inserting/updating aggregates into {agg_table}: {e}", exc_info=True); return False
        else: logger.warning(f"No valid rows to insert into {agg_table} after validation ({invalid_median_count} invalid medians skipped).")

    # Update tracker since process completed (or no data found/valid)
    update_last_run_time(conn, agg_task_name, datetime.now(pytz.utc))
    # Export JSON after successful storage/update attempt
    export_aggregates_to_json(conn, endpoint_name, level)
    return True

# --- Aggregate JSON Export ---
def export_aggregates_to_json(conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str) -> bool:
    """Exports aggregated data for a specific level to a JSON file suitable for charting."""
    # (Export logic remains the same)
    agg_table = f"{endpoint_name}_{level}"
    output_dir = AGGREGATE_JSON_FOLDER
    output_filename = os.path.join(output_dir, f"agg_{endpoint_name}_{level}.json")
    logger.info(f"Attempting export: '{agg_table}' -> {output_filename}")
    try:
        query = f'SELECT timestamp, symbol, median_price FROM "{agg_table}" WHERE median_price IS NOT NULL ORDER BY timestamp ASC'
        all_data = conn.execute(query).fetchall()
        os.makedirs(output_dir, exist_ok=True)
        if not all_data:
            logger.warning(f"No aggregate data in '{agg_table}' to export. Writing empty JSON.");
            with open(output_filename, 'w', encoding='utf-8') as f: json.dump({}, f)
            return True

        chart_data: Dict[str, List[List[Union[int, float]]]] = {}
        conversion_errors, processed_points = 0, 0
        for ts, symbol, price in all_data:
            processed_points +=1
            if symbol not in chart_data: chart_data[symbol] = []
            try:
                timestamp_ms_utc: Optional[int] = None
                if isinstance(ts, datetime): ts_utc = ts.astimezone(pytz.utc) if ts.tzinfo else pytz.utc.localize(ts); timestamp_ms_utc = int(ts_utc.timestamp() * 1000)
                elif isinstance(ts, date): dt_utc = pytz.utc.localize(datetime.combine(ts, dt_time.min)); timestamp_ms_utc = int(dt_utc.timestamp() * 1000)
                else: raise TypeError(f"Unsupported timestamp type: {type(ts)}")
                price_float = float(price)
                chart_data[symbol].append([timestamp_ms_utc, price_float])
            except (TypeError, ValueError) as conv_err: conversion_errors += 1; logger.error(f"Error converting data for chart export (Symbol: {symbol}, Time: {ts}, Price: {price}): {conv_err}", exc_info=False)

        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(chart_data, f, ensure_ascii=False, indent=4 if PRETTY_PRINT_JSON else None, separators=(',', ':') if not PRETTY_PRINT_JSON else None)
        log_msg = f"Exported {processed_points - conversion_errors}/{processed_points} points to {output_filename}"
        if conversion_errors > 0: logger.warning(log_msg + f" ({conversion_errors} errors).")
        else: logger.info(log_msg + ".")
        return True
    except duckdb.CatalogException:
         logger.warning(f"Cannot export JSON for '{agg_table}': Table does not exist. Writing empty JSON.");
         os.makedirs(output_dir, exist_ok=True)
         with open(output_filename, 'w', encoding='utf-8') as f: json.dump({}, f)
         return True
    except Exception as e: logger.error(f"Failed to export aggregates from '{agg_table}' to JSON: {e}", exc_info=True); return False

# --- API Fetching ---
async def fetch_api_data(
    session: aiohttp.ClientSession, endpoint_name: str, config: Dict[str, Any],
    base_url: str, api_key: str
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    """Fetches data from a single API endpoint asynchronously, handling errors and masking."""
    # (Fetch logic remains the same - robust error handling)
    relative_url = config.get('relative_url', 'MISSING_URL_IN_CONFIG')
    try: full_url = f"{base_url.rstrip('/')}/{relative_url.lstrip('/').format(api_key=api_key)}"
    except KeyError: logger.error(f"Missing 'api_key' placeholder in relative_url for {endpoint_name}: '{relative_url}'"); return None
    except Exception as url_e: logger.error(f"Error formatting URL for {endpoint_name}: {url_e}"); return None

    masked_url = mask_string(full_url)
    logger.debug(f"Requesting: {endpoint_name} ({masked_url})")
    start_time = time.monotonic()
    try:
        async with session.get(full_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            elapsed_time = time.monotonic() - start_time
            logger.debug(f"Response: {endpoint_name} Status={response.status} in {elapsed_time:.2f}s")
            body_bytes = await response.read() # Read body once
            try:
                if response.status == 200:
                    data = json.loads(body_bytes.decode(response.charset or 'utf-8'))
                    logger.info(f"Success: Fetched '{endpoint_name}'.")
                    return endpoint_name, config, data
                else:
                    snippet = mask_string(body_bytes[:500].decode(response.charset or 'utf-8', errors='replace'))
                    logger.error(f"API Request Failed: {endpoint_name} ({response.status}). Snippet: '{snippet}'")
                    return None
            except json.JSONDecodeError as json_err:
                snippet = mask_string(body_bytes[:500].decode(response.charset or 'utf-8', errors='replace'))
                logger.error(f"JSON Decode Error: {endpoint_name} (Status: {response.status}). Snippet: '{snippet}'. Err: {json_err}", exc_info=False)
                return None
            except Exception as e:
                snippet = mask_string(body_bytes[:500].decode(response.charset or 'utf-8', errors='replace'))
                logger.error(f"Response Processing Error: {endpoint_name} (Status: {response.status}). Snippet: '{snippet}'. Err: {e}", exc_info=True)
                return None
    except asyncio.TimeoutError: logger.error(f"Timeout Error ({REQUEST_TIMEOUT_SECONDS}s): {endpoint_name} ({masked_url})", exc_info=False); return None
    except aiohttp.ClientError as client_err: logger.error(f"Client Error: {endpoint_name} ({masked_url}). Err: {client_err}", exc_info=False); return None
    except Exception as e: logger.error(f"Unexpected Fetch Error: {endpoint_name} ({masked_url}). Err: {e}", exc_info=True); return None

# --- Main Execution ---
async def main():
    """Main asynchronous function orchestrating the fetch and aggregation process."""
    setup_logging() # Setup logging first
    script_start_time = time.monotonic()
    logger.info(f"{COLOR_GREEN}--- Starting Market Data Sync Cycle ---{COLOR_RESET}")

    # Load essential config
    api_key = os.getenv(API_KEY_ENV_VAR)
    base_url = os.getenv(BASE_URL_ENV_VAR)
    if not api_key or not base_url: logger.critical(f"CRITICAL: Env vars '{API_KEY_ENV_VAR}' or '{BASE_URL_ENV_VAR}' not set. Exiting."); return

    # Connect to DB
    db_conn = get_db_connection(DB_FILE)
    if db_conn is None: logger.critical("CRITICAL: DB connection failed. Exiting."); return

    now_utc = datetime.now(pytz.utc)
    now_local = now_utc.astimezone(DEFAULT_TIMEZONE)
    logger.debug(f"Cycle time: UTC={now_utc.isoformat()}, Local={now_local.isoformat()}")

    # --- 1. Fetch Raw Data ---
    # (Scheduling and execution logic remains unchanged)
    apis_to_fetch_this_run: List[Tuple[str, Dict[str, Any]]] = []
    logger.info("--- Checking API Fetch Tasks ---")
    # ... (loop through API_ENDPOINTS, check enabled, market hours, interval using should_run_task) ...
    for name, config in API_ENDPOINTS.items():
        logger.debug(f"Checking fetch task: '{name}'")
        if not config.get('enabled', False): logger.debug(f" -> Skip '{name}': Disabled."); continue
        if config.get('market_hours_apply', False) and not is_market_open(DEFAULT_TIMEZONE, TSE_MARKET_OPEN_TIME, TSE_MARKET_CLOSE_TIME, TSE_MARKET_DAYS): logger.debug(f" -> Skip '{name}': Market Closed."); continue
        fetch_interval = timedelta(minutes=config.get('fetch_interval_minutes', 10))
        if not should_run_task(db_conn, f"fetch_{name}", fetch_interval, now_utc): logger.debug(f" -> Skip '{name}': Interval not met."); continue
        logger.info(f"Scheduling fetch for: '{name}'")
        apis_to_fetch_this_run.append((name, config))

    fetch_results = []
    if apis_to_fetch_this_run:
        logger.info(f"Attempting to fetch data for {len(apis_to_fetch_this_run)} endpoints...")
        # ... (asyncio semaphore and gather logic) ...
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async def fetch_with_semaphore(session, name, cfg, key):
             async with semaphore: return await fetch_api_data(session, name, cfg, base_url, key)
        client_timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS + 5)
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector, timeout=client_timeout) as session:
            tasks = [fetch_with_semaphore(session, name, config, api_key) for name, config in apis_to_fetch_this_run]
            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)
    else: logger.info("No API endpoints scheduled for fetching.")

    # --- Process Fetch Results ---
    # (Processing logic remains unchanged - insert raw, save json, update tracker)
    processed_fetches, successful_raw_inserts, fetch_errors = 0, 0, 0
    if fetch_results:
        insert_time_utc = datetime.now(pytz.utc)
        logger.info("--- Processing Fetch Results ---")
        # ... (loop through results, handle success/failure, insert_raw_data, save json, update_last_run_time) ...
        for i, result in enumerate(fetch_results):
            processed_fetches += 1
            endpoint_name, config = apis_to_fetch_this_run[i]
            fetch_task_name = f"fetch_{endpoint_name}"
            fetch_successful = False
            if isinstance(result, Exception): logger.error(f"Fetch Task Error: '{endpoint_name}'. Exception: {result}", exc_info=False); fetch_errors += 1
            elif result is not None:
                _name, _config, data = result
                if insert_raw_data(db_conn, endpoint_name, insert_time_utc, data): successful_raw_inserts += 1; fetch_successful = True
                else: fetch_errors += 1
                output_filename = os.path.join(DATA_FOLDER, config['output_filename'])
                try:
                    os.makedirs(DATA_FOLDER, exist_ok=True)
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=4 if PRETTY_PRINT_JSON else None, separators=(',', ':') if not PRETTY_PRINT_JSON else None)
                    logger.debug(f"Saved latest raw JSON: {os.path.basename(output_filename)}")
                except IOError as e: logger.error(f"Failed to write latest JSON file {os.path.basename(output_filename)}: {e}", exc_info=False)
            else: fetch_errors += 1
            update_last_run_time(db_conn, fetch_task_name, now_utc)
        logger.info(f"Fetch Results Summary: Processed={processed_fetches}, DB Inserts={successful_raw_inserts}, Errors={fetch_errors}")


    # --- 2. Run Aggregations ---
    logger.info("--- Checking Aggregation Tasks ---")
    agg_success, agg_skipped, agg_errors = 0, 0, 0
    # (Aggregation scheduling logic remains unchanged)
    # ... (loop through endpoints, check levels, determine if run needed based on daily/interval and should_run_task) ...
    for endpoint_name, config in API_ENDPOINTS.items():
        if not config.get('enabled', False): continue
        levels = config.get('aggregation_levels', [])
        if not levels: continue
        logger.debug(f"Checking aggregations for '{endpoint_name}': Levels={levels}")
        for level in levels:
            if level not in AGGREGATION_INTERVALS: logger.warning(f"Invalid level '{level}' for '{endpoint_name}'. Skipping."); continue
            interval = AGGREGATION_INTERVALS[level]
            agg_task_name = f"aggregate_{level}_{endpoint_name}"
            run_agg, target_ts, start_ts_utc, end_ts_utc = False, None, None, now_utc

            if level == 'daily':
                if now_local.time() >= DAILY_AGGREGATION_TIME_LOCAL:
                    last_run_daily_utc = get_last_run_time(db_conn, agg_task_name)
                    should_run_daily = False
                    if last_run_daily_utc is None: should_run_daily = True; logger.info(f"Daily task '{agg_task_name}' due: Never run.")
                    else:
                        last_run_local_date = last_run_daily_utc.astimezone(DEFAULT_TIMEZONE).date()
                        yesterday_local_date = now_local.date() - timedelta(days=1)
                        if last_run_local_date < yesterday_local_date: should_run_daily = True; logger.info(f"Daily task '{agg_task_name}' due: Last run ({last_run_local_date}) < yesterday ({yesterday_local_date}).")
                        else: logger.debug(f"Skip Daily '{agg_task_name}': Already processed up to {last_run_local_date}.")
                    if should_run_daily:
                        run_agg = True
                        target_ts = now_local.date() - timedelta(days=1)
                        start_dt_local = datetime.combine(target_ts, dt_time.min)
                        end_dt_local = datetime.combine(target_ts + timedelta(days=1), dt_time.min)
                        start_ts_utc = DEFAULT_TIMEZONE.localize(start_dt_local).astimezone(pytz.utc)
                        end_ts_utc = DEFAULT_TIMEZONE.localize(end_dt_local).astimezone(pytz.utc)
                else: logger.debug(f"Skip Daily '{agg_task_name}': Trigger time {DAILY_AGGREGATION_TIME_LOCAL} not reached.")
            else: # Interval-based
                if should_run_task(db_conn, agg_task_name, interval, now_utc):
                    run_agg = True
                    target_ts = now_utc
                    start_ts_utc = now_utc - interval
                    end_ts_utc = now_utc

            if run_agg and target_ts is not None and start_ts_utc is not None and end_ts_utc is not None:
                logger.info(f"Running Aggregation: '{agg_task_name}' target={target_ts.isoformat()} period=[{start_ts_utc.isoformat()} to {end_ts_utc.isoformat()})")
                try:
                    # Call aggregation function
                    if calculate_and_store_aggregates(db_conn, endpoint_name, level, config, start_ts_utc, end_ts_utc, target_ts): agg_success += 1
                    else: agg_errors += 1
                except Exception as agg_e: logger.error(f"Unexpected error calling calculate_and_store_aggregates for {agg_task_name}: {agg_e}", exc_info=True); agg_errors += 1
            elif run_agg: logger.warning(f"Aggregation scheduled for {agg_task_name} but timestamps invalid. Skipping."); agg_skipped += 1
            else: agg_skipped += 1

    logger.info(f"Aggregation Results Summary: Success={agg_success}, Skipped={agg_skipped}, Errors={agg_errors}")

    # --- Finish ---
    try:
        if db_conn: db_conn.close(); logger.info("DB connection closed.")
    except Exception as e: logger.error(f"Error closing DB: {e}", exc_info=True)

    script_end_time = time.monotonic()
    total_duration = script_end_time - script_start_time
    logger.info(f"{COLOR_GREEN}--- Market Data Sync Cycle Finished in {total_duration:.2f} seconds ---{COLOR_RESET}")

    # --- Post-Execution Suggestions ---
    logger.info("Improvement Suggestion 1: Implement retry logic with exponential backoff for API fetch failures.")
    logger.info("Improvement Suggestion 2: Add data validation checks (e.g., using Pydantic models) for API responses.")

# --- Script Entry Point ---
if __name__ == "__main__":
    # Optional: Load .env for local development
    try:
        import importlib.util
        dotenv_spec = importlib.util.find_spec("dotenv")
        if dotenv_spec:
            from dotenv import load_dotenv
            script_dir = os.path.dirname(__file__)
            dotenv_path = os.path.join(script_dir, '..', '.env')
            if os.path.exists(dotenv_path): load_dotenv(dotenv_path=dotenv_path, verbose=True)
            else: print(f"{COLOR_BLUE}•{COLOR_RESET} No .env file found at '{dotenv_path}'.")
        else: print(f"{COLOR_YELLOW}⚠️ {COLOR_RESET} python-dotenv not installed.")
    except Exception as e: print(f"{COLOR_RED}✗ Error checking/loading .env: {e}{COLOR_RESET}")

    # Run main async function
    try:
        asyncio.run(main())
    except KeyboardInterrupt: print(f"\n{COLOR_YELLOW}⚠️ Script interrupted.{COLOR_RESET}")
    except Exception as e:
        # Log critical errors if logger is available, otherwise print stack trace
        if logger.handlers: logger.critical(f"Unhandled critical exception: {e}", exc_info=True)
        else: print(f"{COLOR_RED}--- CRITICAL EXCEPTION ---"); traceback.print_exc(); print(f"{COLOR_RED}--- END EXCEPTION ---{COLOR_RESET}")
