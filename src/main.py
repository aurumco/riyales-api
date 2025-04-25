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
LOG_LEVEL_ENV_VAR: str = "LOG_LEVEL"      # Controls console log verbosity (INFO, DEBUG, WARNING, ERROR) - Set via environment variable

# General Settings
LOG_FOLDER: str = "logs"                  # Directory for log files
DATA_FOLDER: str = "data"                 # Directory for output JSON and DB
AGGREGATE_JSON_FOLDER: str = os.path.join(DATA_FOLDER, "aggregates") # Subfolder for aggregate JSONs for charting
DB_FILE: str = os.path.join(DATA_FOLDER, "market_data.duckdb") # DuckDB database file path
REQUEST_TIMEOUT_SECONDS: int = 15         # Timeout for individual API requests
PRETTY_PRINT_JSON: bool = False           # Save JSON compact (False) or pretty-printed (True) for latest data files
MAX_CONCURRENT_REQUESTS: int = 10         # Max simultaneous API requests
TIMEZONE: str = "Asia/Tehran"             # Default timezone for operations like market hours
DEFAULT_TIMEZONE = pytz.timezone(TIMEZONE)# Cached timezone object for performance

# User Agent (Update periodically to mimic real browsers)
# Found via searching "latest chrome user agent string" - Update as needed
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
HEADERS: Dict[str, str] = { "User-Agent": USER_AGENT, "Accept": "application/json, text/plain, */*" }

# --- API Endpoint Configuration ---
# Defines the endpoints to fetch and how to process them.
# Key explanation in the main.py docstring (or README).
API_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "gold_currency": {
        "relative_url": "/Api/Market/Gold_Currency.php?key={api_key}", "output_filename": "gold_currency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["6h", "12h", "daily", "3d", "weekly"],
        "price_json_path": "$.price", "symbol_json_path": "$.symbol",
        "array_base_paths": ["$.gold", "$.currency"],
    },
    "crypto": {
        "relative_url": "/Api/Market/Cryptocurrency.php?key={api_key}", "output_filename": "cryptocurrency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["6h", "12h", "daily", "3d", "weekly"],
        "price_json_path": "$.price_toman", "symbol_json_path": "$.name",
        "array_base_paths": ["$"],
    },
    "commodity": {
        "relative_url": "/Api/Market/Commodity.php?key={api_key}", "output_filename": "commodity.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["6h", "12h", "daily", "3d", "weekly"],
        "price_json_path": "$.price", "symbol_json_path": "$.symbol",
        "array_base_paths": ["$.metal_precious"],
    },
    "tse_ifb_symbols": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=1", "output_filename": "tse_ifb_symbols.json",
        "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True,
        "aggregation_levels": ["daily"],
        "price_json_path": "$.pc", "symbol_json_path": "$.l18",
        "array_base_paths": ["$"],
    },
     # Other endpoints remain the same, aggregation can be added later if needed
     "tse_options": { "relative_url": "/Api/Tsetmc/Option.php?key={api_key}", "output_filename": "tse_options.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_nav": { "relative_url": "/Api/Tsetmc/Nav.php?key={api_key}", "output_filename": "tse_nav.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "tse_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=1", "output_filename": "tse_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "ifb_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=2", "output_filename": "ifb_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "selected_indices": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=3", "output_filename": "selected_indices.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False },
     "debt_securities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=4", "output_filename": "debt_securities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "housing_facilities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=5", "output_filename": "housing_facilities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
     "futures": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=3", "output_filename": "futures.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True },
}

# Market Hours (Tehran Stock Exchange - Adjust if necessary)
TSE_MARKET_OPEN_TIME: dt_time = dt_time(8, 45)   # Market opening time (local Tehran time)
TSE_MARKET_CLOSE_TIME: dt_time = dt_time(12, 45) # Market closing time (local Tehran time)
TSE_MARKET_DAYS: List[int] = [5, 6, 0, 1, 2]     # Weekdays for TSE (0=Monday, 5=Saturday, 6=Sunday)

# Aggregation Intervals & Trigger Time
AGGREGATION_INTERVALS: Dict[str, timedelta] = {
    "6h": timedelta(hours=6), "12h": timedelta(hours=12), "daily": timedelta(days=1),
    "3d": timedelta(days=3), "weekly": timedelta(weeks=1),
}
DAILY_AGGREGATION_TIME_LOCAL: dt_time = dt_time(0, 5) # Time (local) to run daily aggregation for the previous day

# --- Constants ---
COLOR_GREEN: str = "\033[32m"
COLOR_RED: str = "\033[31m"
COLOR_YELLOW: str = "\033[33m"
COLOR_BLUE: str = "\033[34m"
COLOR_RESET: str = "\033[0m"
# Precision and Scale for storing prices in DuckDB DECIMAL type
DECIMAL_PRECISION = 18
DECIMAL_SCALE = 6

# --- Global Logger ---
logger = logging.getLogger("MarketDataSync")

# --- Utility ---
def mask_string(s: Optional[str]) -> str:
    """Masks potentially sensitive strings like API keys in logs."""
    if s is None: return "None"
    s = str(s) # Ensure it's a string
    # Mask common key/token patterns using regex (case-insensitive)
    s = re.sub(r"key=([^&?\s]+)", "key=********", s, flags=re.IGNORECASE)
    s = re.sub(r"token=([^&?\s]+)", "token=********", s, flags=re.IGNORECASE)
    # Mask potential Authorization headers carefully (show type, hide value)
    s = re.sub(r"(Authorization\s*:\s*)(\w+\s+)\S+", r"\1\2********", s, flags=re.IGNORECASE)
    return s

# --- Logging Setup ---
def setup_logging() -> None:
    """Configures logging: one file per run, console output with colors, log cleanup."""
    # Determine console log level from environment variable (default: INFO)
    log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, 'INFO').upper()
    try:
        log_level = getattr(logging, log_level_str)
    except AttributeError:
        print(f"{COLOR_YELLOW}Warning: Invalid LOG_LEVEL '{log_level_str}'. Defaulting to INFO.{COLOR_RESET}")
        log_level = logging.INFO
        log_level_str = 'INFO' # Update string for logging message

    # *** IMPORTANT: Logger Base Level vs Handler Level ***
    # Set logger base level to DEBUG: Allows capturing all messages.
    # Handlers will then filter based on their own levels.
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers to prevent duplicate logs if setup is called again
    if logger.hasHandlers(): logger.handlers.clear()

    os.makedirs(LOG_FOLDER, exist_ok=True)

    # --- File Handler (New file per run, logs DEBUG and above, no colors) ---
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(LOG_FOLDER, f"market_data_{run_timestamp}.log")
    try:
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG) # Log everything to the file
        # Simple formatter without colors for the file
        file_formatter = logging.Formatter(
            "%(asctime)s|%(levelname)-7s|%(name)s|%(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        # Fallback to console if file logging fails
        print(f"{COLOR_RED}Error setting up file logger ({log_file_path}): {e}{COLOR_RESET}")

    # --- Console Handler (Level from ENV, with colors and masking) ---
    console_handler = logging.StreamHandler()
    # *** Set console handler level based on the environment variable ***
    console_handler.setLevel(log_level)

    class SecureColorFormatter(logging.Formatter):
        # (Formatter code remains same - handles colors and masking)
        level_colors = {
            logging.DEBUG: COLOR_BLUE, logging.INFO: COLOR_GREEN,
            logging.WARNING: COLOR_YELLOW, logging.ERROR: COLOR_RED, logging.CRITICAL: COLOR_RED,
        }
        def format(self, record):
            color = self.level_colors.get(record.levelno, COLOR_RESET)
            # IMPORTANT: Mask secrets *before* formatting
            masked_message = mask_string(record.getMessage())

            log_fmt = f"{color}•{COLOR_RESET} %(asctime)s|{color}%(levelname)-7s{COLOR_RESET}| {color}{masked_message}{COLOR_RESET}"
            _formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S")

            # Create a temporary record to avoid modifying the original used by other handlers
            temp_record = logging.makeLogRecord(record.__dict__)
            # Override the message attribute *for this formatter*
            temp_record.msg = masked_message
            # Ensure getMessage uses the modified msg if needed (though we set it directly)
            temp_record.message = temp_record.getMessage()

            return _formatter.format(temp_record)

    console_handler.setFormatter(SecureColorFormatter())
    logger.addHandler(console_handler)

    # Perform log cleanup *after* setting up handlers for the current run
    cleanup_old_logs(LOG_FOLDER, 12) # Keep logs for 12 hours

    # Log the final effective log levels
    logger.info(f"Logging setup complete. Target Console Level: {log_level_str}, File Level: DEBUG")
    logger.info(f"Current log file: {log_file_path}")
    # Explain how to change console level
    logger.debug(f"To change console log level, set the '{LOG_LEVEL_ENV_VAR}' environment variable (e.g., 'DEBUG', 'INFO', 'WARNING').")

def cleanup_old_logs(log_directory: str, hours_to_keep: int) -> None:
    """Removes log files matching the pattern older than the specified hours."""
    cutoff_time = datetime.now() - timedelta(hours=hours_to_keep)
    logger.debug(f"Checking for logs older than {hours_to_keep} hours ({cutoff_time.isoformat()}) in '{log_directory}'...")
    cleaned_count = 0
    try:
        # Use glob to find potential log files based on the naming convention
        log_pattern = os.path.join(log_directory, "market_data_*.log")
        for filename in glob.glob(log_pattern):
            if os.path.isfile(filename):
                try:
                    # Attempt to parse timestamp from filename first
                    match = re.search(r"market_data_(\d{8}_\d{6})\.log", os.path.basename(filename))
                    file_is_old = False
                    if match:
                        file_ts_str = match.group(1)
                        file_dt = datetime.strptime(file_ts_str, "%Y%m%d_%H%M%S")
                        if file_dt < cutoff_time:
                            file_is_old = True
                    else:
                        # Fallback: Use file modification time if filename doesn't match pattern
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(filename))
                        if file_mtime < cutoff_time:
                            file_is_old = True
                            logger.warning(f"Log file '{os.path.basename(filename)}' has unexpected name, checking mtime.")

                    if file_is_old:
                        os.remove(filename)
                        logger.info(f"Cleaned old log file: {os.path.basename(filename)}")
                        cleaned_count += 1
                except OSError as e:
                    logger.error(f"Error removing log file {os.path.basename(filename)}: {e}")
                except ValueError:
                    logger.warning(f"Could not parse timestamp from log filename: {os.path.basename(filename)}")

        logger.debug(f"Log cleanup finished. Removed {cleaned_count} old files.")
    except Exception as e:
        logger.error(f"Log cleanup scan failed: {e}", exc_info=True)

# --- Timezone & Market Check ---
def is_market_open(tz: pytz.BaseTzInfo, open_time: dt_time, close_time: dt_time, market_days: List[int]) -> bool:
    """Checks if the current time is within specified market hours and days in the given timezone."""
    try:
        now_local = datetime.now(tz)
        current_time = now_local.time()
        current_weekday = now_local.weekday() # Monday is 0, Sunday is 6

        logger.debug(f"Market Check: LocalTime={now_local.strftime('%H:%M:%S %Z')}, Weekday={current_weekday}, CurrentTime={current_time}, MarketHours={open_time}-{close_time}, MarketDays={market_days}")

        if current_weekday not in market_days:
            logger.debug("Market Status: CLOSED (Outside Market Days)")
            return False

        is_open = open_time <= current_time < close_time
        logger.debug(f"Market Status: {'OPEN' if is_open else 'CLOSED'} (Within Market Hours Check)")
        return is_open
    except Exception as e:
        logger.error(f"Market hours check failed: {e}", exc_info=True)
        return False # Fail safe: assume market is closed if check fails

# --- Database Functions ---
def get_db_connection(db_path: str) -> Optional[duckdb.DuckDBPyConnection]:
    """Establishes and returns a DuckDB connection, ensuring tracker table exists."""
    try:
        logger.debug(f"Attempting to connect to DB: {db_path}")
        # Ensure data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = duckdb.connect(database=db_path, read_only=False)
        # Basic check after connection
        conn.execute("SELECT 42;")
        logger.info("DB connection successful.")
        # Ensure task tracker table exists on every connection
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
            # DuckDB TIMESTAMPTZ should return timezone-aware datetime (UTC)
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                     # Should not happen with TIMESTAMPTZ, but handle defensively
                     logger.warning(f"DB returned naive datetime for '{task_name}', assuming UTC.")
                     return pytz.utc.localize(ts)
                # Ensure it's UTC
                return ts.astimezone(pytz.utc)
            else:
                 logger.warning(f"Unexpected type {type(ts)} for last_run_timestamp for '{task_name}'.")
                 return None
        return None # Task never run or timestamp is NULL
    except Exception as e:
        logger.error(f"Error getting last run time for '{task_name}': {e}", exc_info=True)
        return None

def update_last_run_time(conn: duckdb.DuckDBPyConnection, task_name: str, timestamp: datetime) -> None:
    """Updates or inserts the last run timestamp (converts to UTC) for a task."""
    # Ensure the timestamp is timezone-aware UTC before inserting
    if timestamp.tzinfo is None:
        # Assume the naive timestamp is in the script's default timezone
        timestamp = DEFAULT_TIMEZONE.localize(timestamp).astimezone(pytz.utc)
        logger.debug(f"Converted naive timestamp to UTC for {task_name}: {timestamp.isoformat()}")
    elif timestamp.tzinfo != pytz.utc:
        timestamp = timestamp.astimezone(pytz.utc)
        logger.debug(f"Converted timestamp to UTC for {task_name}: {timestamp.isoformat()}")

    try:
        # Use UPSERT logic
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
    """Generic function to execute a CREATE TABLE IF NOT EXISTS statement safely."""
    try:
        conn.execute(create_sql)
        logger.debug(f"Ensured table '{table_name}' exists or was created.")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure table '{table_name}': {e}", exc_info=True)
        return False

def ensure_raw_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str) -> bool:
    """Ensures the table for storing raw JSON data for an endpoint exists."""
    table_name = f"{endpoint_name}_raw"
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (timestamp TIMESTAMPTZ PRIMARY KEY, data JSON NOT NULL);'
    return ensure_table(conn, sql, table_name)

def ensure_aggregate_table(conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str) -> bool:
    """Ensures the table for storing aggregated data for a specific level exists."""
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
        # Convert Python object to JSON string for storage
        json_data_str = json.dumps(data, ensure_ascii=False)
        conn.execute(f'INSERT INTO "{table_name}" (timestamp, data) VALUES (?, ?)', (timestamp, json_data_str))
        logger.debug(f"Inserted raw data into '{table_name}' for {timestamp.isoformat()}.")
        return True
    except duckdb.ConstraintException:
         logger.warning(f"Duplicate raw timestamp {timestamp.isoformat()} for '{table_name}'. Skipping insert.")
         return True # Treat as success, data already exists
    except Exception as e:
        logger.error(f"Failed insert raw data into '{table_name}': {e}", exc_info=True)
        return False

# --- Aggregation ---
def calculate_and_store_aggregates(
    conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str,
    config: Dict[str, Any], start_time_utc: datetime, end_time_utc: datetime,
    target_timestamp: Union[datetime, date] # Timestamp or Date for the aggregated record
) -> bool:
    """Calculates median price for symbols over a period and stores it."""
    raw_table = f"{endpoint_name}_raw"
    agg_table = f"{endpoint_name}_{level}"
    agg_task_name = f"aggregate_{level}_{endpoint_name}" # For tracker updates

    logger.info(f"Starting aggregation for '{agg_table}' (Period: {start_time_utc.isoformat()} to {end_time_utc.isoformat()})")

    # --- Check if raw table exists ---
    try:
         table_exists_check = conn.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{raw_table}' LIMIT 1").fetchone()
         if not table_exists_check:
              logger.warning(f"Skip Aggregation for {agg_table}: Raw table '{raw_table}' does not exist.")
              # No need to update tracker, wait for raw data to be inserted first
              return True
    except Exception as e:
         logger.error(f"Error checking existence of raw table '{raw_table}': {e}", exc_info=True)
         return False

    # --- Check if raw table has data for the period ---
    try:
         count_result = conn.execute(f'SELECT COUNT(*) FROM "{raw_table}" WHERE timestamp >= ? AND timestamp < ?', (start_time_utc, end_time_utc)).fetchone()
         if not count_result or count_result[0] == 0:
              logger.info(f"Skip Aggregation for {agg_table}: No raw data found in period {start_time_utc.isoformat()} to {end_time_utc.isoformat()}.")
              # Update tracker: We've checked this empty period.
              update_last_run_time(conn, agg_task_name, datetime.now(pytz.utc))
              return True # Not an error, just nothing to aggregate
         else:
             logger.debug(f"Found {count_result[0]} raw data points for '{raw_table}' in the aggregation period.")
    except Exception as e:
         logger.error(f"Error checking raw data count for {agg_table}: {e}", exc_info=True)
         return False # Real error during check

    # Ensure aggregate table exists
    if not ensure_aggregate_table(conn, endpoint_name, level): return False

    # Get JSON processing paths from config
    price_path = config.get("price_json_path", "$.price")
    symbol_path = config.get("symbol_json_path", "$.symbol")
    array_base_paths = config.get("array_base_paths", ["$"]) # List of paths to arrays

    # --- Build and Execute Aggregation Query ---
    all_aggregates = []
    query_success = True

    for base_path in array_base_paths:
        logger.debug(f"Aggregating from base path: '{base_path}' for table '{agg_table}'")
        try:
            # --- Modified query with simpler approach for older DuckDB versions ---
            query = f"""
            WITH RawData AS (
                SELECT 
                    timestamp,
                    data
                FROM "{raw_table}"
                WHERE timestamp >= ? AND timestamp < ?
            ),
            JsonExtract AS (
                SELECT 
                    json_extract(data, '{base_path}') as items_array
                FROM RawData
                WHERE json_type(json_extract(data, '{base_path}')) = 'ARRAY'
            ),
            Expanded AS (
                SELECT 
                    item,
                    json_extract_string(item, '{symbol_path}') as symbol,
                    json_extract_string(item, '{price_path}') as price_str
                FROM (
                    SELECT unnest(items_array) as item 
                    FROM JsonExtract
                )
                WHERE json_type(item) = 'OBJECT'
            ),
            Filtered AS (
                SELECT
                    symbol,
                    try_cast(price_str AS DECIMAL({DECIMAL_PRECISION*2}, {DECIMAL_SCALE*2})) AS price_decimal
                FROM Expanded
                WHERE symbol IS NOT NULL AND price_str IS NOT NULL 
                  AND symbol != '' AND price_str != ''
            )
            SELECT
                symbol,
                median(price_decimal) AS median_price,
                count(*) AS source_count
            FROM Filtered
            WHERE price_decimal IS NOT NULL
            GROUP BY symbol;
            """
            logger.debug(f"Executing agg query for {agg_table} (path: {base_path}) period: {start_time_utc.isoformat()} - {end_time_utc.isoformat()}")
            # print(f"DEBUG QUERY for {agg_table} path {base_path}:\n{query}\nParams: {start_time_utc}, {end_time_utc}") # Uncomment for deep debug
            aggregates = conn.execute(query, (start_time_utc, end_time_utc)).fetchall()
            logger.info(f"Aggregated {len(aggregates)} symbols from path '{base_path}' for {agg_table}")
            all_aggregates.extend(aggregates)

        # Catch specific DuckDB errors and general errors
        except (duckdb.BinderException, duckdb.CatalogException, duckdb.ParserException, duckdb.InvalidInputException) as db_query_err:
             logger.error(f"DuckDB Query Error during aggregation for {agg_table} (path: {base_path}): {db_query_err}", exc_info=True)
             # Provide context for debugging
             logger.error(f"Failed Query Context: Table='{raw_table}', BasePath='{base_path}', PricePath='{price_path}', SymbolPath='{symbol_path}'")
             query_success = False
             break # Stop processing other base paths for this endpoint/level
        except Exception as e:
            logger.error(f"General Error during aggregation query for {agg_table} (path: {base_path}): {e}", exc_info=True)
            query_success = False
            break

    if not query_success:
        logger.error(f"Aggregation failed for {endpoint_name} level {level} due to query error(s).")
        return False # Don't update tracker if query failed

    # --- Insert Aggregates ---
    if not all_aggregates:
        logger.warning(f"No valid aggregate data calculated for {agg_table} in period {start_time_utc.isoformat()} - {end_time_utc.isoformat()}.")
    else:
        insert_sql = f"""
        INSERT INTO "{agg_table}" (timestamp, symbol, median_price, source_count) VALUES (?, ?, ?, ?)
        ON CONFLICT (timestamp, symbol) DO UPDATE SET
            median_price = excluded.median_price, source_count = excluded.source_count;
        """
        rows_to_insert = []
        invalid_median_count = 0
        for symbol, median_val, count in all_aggregates:
             # Validate and convert median value before adding to batch
             if median_val is None:
                 logger.debug(f"Median value for {symbol} in {agg_table} is NULL, skipping.")
                 invalid_median_count += 1
                 continue
             try:
                  # Convert median_val (could be float from median()) to Decimal and quantize
                  final_median = Decimal(str(median_val)).quantize(Decimal('1e-' + str(DECIMAL_SCALE)), rounding=ROUND_HALF_UP)
                  # target_timestamp is already Date or Datetime (UTC)
                  rows_to_insert.append((target_timestamp, symbol, final_median, count))
             except (InvalidOperation, TypeError, ValueError) as q_err:
                  logger.warning(f"Could not convert/quantize median value {median_val} ({type(median_val)}) to Decimal for symbol '{symbol}' in {agg_table}: {q_err}")
                  invalid_median_count += 1

        if rows_to_insert:
            try:
                # Use transaction for atomic bulk insert/update
                with conn.transaction():
                    conn.executemany(insert_sql, rows_to_insert)
                logger.info(f"Stored/Updated {len(rows_to_insert)} aggregates into {agg_table}. ({invalid_median_count} invalid medians skipped)")
            except Exception as e:
                logger.error(f"Error inserting/updating aggregates into {agg_table}: {e}", exc_info=True)
                return False # Return failure if insert fails
        else:
            logger.warning(f"No valid rows to insert into {agg_table} after validation ({invalid_median_count} invalid medians skipped).")

    # Update tracker only if the process completed (even if no data was inserted/updated)
    update_last_run_time(conn, agg_task_name, datetime.now(pytz.utc))
    # After successful storage/update attempt, export this level to JSON
    export_aggregates_to_json(conn, endpoint_name, level)
    return True

# --- Aggregate JSON Export ---
def export_aggregates_to_json(conn: duckdb.DuckDBPyConnection, endpoint_name: str, level: str) -> bool:
    """Exports aggregated data for a specific level to a JSON file suitable for charting."""
    agg_table = f"{endpoint_name}_{level}"
    output_dir = AGGREGATE_JSON_FOLDER
    output_filename = os.path.join(output_dir, f"agg_{endpoint_name}_{level}.json")

    logger.info(f"Attempting to export aggregates from '{agg_table}' to JSON: {output_filename}")
    try:
        # Fetch all data for the level, ordered by time ascending
        query = f'SELECT timestamp, symbol, median_price FROM "{agg_table}" WHERE median_price IS NOT NULL ORDER BY timestamp ASC'
        all_data = conn.execute(query).fetchall()

        if not all_data:
            logger.warning(f"No aggregate data found in '{agg_table}' to export.")
            # Create an empty JSON object file for consistency
            os.makedirs(output_dir, exist_ok=True)
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump({}, f)
            logger.info(f"Created empty aggregate JSON file: {output_filename}")
            return True

        # Structure data for charting: { "SYMBOL": [[timestamp_ms_utc, price_float], ...], ... }
        chart_data: Dict[str, List[List[Union[int, float]]]] = {}
        conversion_errors = 0
        for ts, symbol, price in all_data:
            if symbol not in chart_data:
                chart_data[symbol] = []

            timestamp_ms_utc: Optional[int] = None
            try:
                # Convert timestamp (Date or Datetime) to milliseconds epoch UTC
                if isinstance(ts, datetime):
                    # Ensure it's UTC
                    if ts.tzinfo is None: ts_utc = pytz.utc.localize(ts)
                    else: ts_utc = ts.astimezone(pytz.utc)
                    timestamp_ms_utc = int(ts_utc.timestamp() * 1000)
                elif isinstance(ts, date):
                    # Represent daily data as midnight UTC of that day
                    dt_utc = pytz.utc.localize(datetime.combine(ts, dt_time.min))
                    timestamp_ms_utc = int(dt_utc.timestamp() * 1000)
                else:
                    raise TypeError(f"Unsupported timestamp type: {type(ts)}")

                # Convert Decimal price to float for JSON (handle potential None although query filters)
                price_float = float(price) if price is not None else None

                if timestamp_ms_utc is not None and price_float is not None:
                     chart_data[symbol].append([timestamp_ms_utc, price_float])
                else:
                     # This case should ideally not happen due to query filter and previous checks
                     conversion_errors += 1
                     logger.warning(f"Skipping record for {symbol} at {ts} due to null price/timestamp after conversion.")

            except (TypeError, ValueError) as conv_err:
                 conversion_errors += 1
                 logger.error(f"Error converting data for chart export (Symbol: {symbol}, Time: {ts}, Price: {price}): {conv_err}", exc_info=False)


        # Save the structured data to JSON
        os.makedirs(output_dir, exist_ok=True)
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(chart_data, f, ensure_ascii=False,
                      indent=4 if PRETTY_PRINT_JSON else None,
                      separators=(',', ':') if not PRETTY_PRINT_JSON else None)

        if conversion_errors > 0:
            logger.warning(f"Exported aggregates to {output_filename} with {conversion_errors} data conversion errors (check logs).")
        else:
            logger.info(f"Successfully exported {len(all_data)} aggregate points to {output_filename}")
        return True

    except duckdb.CatalogException:
         logger.warning(f"Cannot export JSON for '{agg_table}': Table does not exist.")
         # If table doesn't exist, treat as success (nothing to export)
         # Ensure directory exists and write empty file maybe?
         os.makedirs(output_dir, exist_ok=True)
         with open(output_filename, 'w', encoding='utf-8') as f:
             json.dump({}, f)
         return True
    except Exception as e:
        logger.error(f"Failed to export aggregates from '{agg_table}' to JSON: {e}", exc_info=True)
        return False

# --- API Fetching ---
async def fetch_api_data(
    session: aiohttp.ClientSession, endpoint_name: str, config: Dict[str, Any],
    base_url: str, api_key: str
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    """Fetches data from a single API endpoint asynchronously, handling errors and masking."""
    relative_url = config.get('relative_url', 'MISSING_URL') # Safer access
    full_url = f"{base_url.rstrip('/')}/{relative_url.lstrip('/').format(api_key=api_key)}"
    masked_url = mask_string(full_url)
    logger.debug(f"Requesting: {endpoint_name} ({masked_url})")
    start_time = time.monotonic()
    try:
        async with session.get(full_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            elapsed_time = time.monotonic() - start_time
            logger.debug(f"Response: {endpoint_name} Status={response.status} in {elapsed_time:.2f}s")

            response_text_snippet = None # Define outside try block
            try:
                # Check status code *before* attempting to read body
                if response.status == 200:
                    # Attempt to decode JSON
                    data = await response.json(content_type=None)
                    logger.info(f"Success: Fetched '{endpoint_name}'.")
                    return endpoint_name, config, data
                else:
                    # Read response text for non-200 status codes for logging
                    response_text_snippet = await response.text()
                    response_text_snippet = mask_string(response_text_snippet[:500]) # Limit and mask
                    logger.error(f"API Request Failed: {endpoint_name} ({response.status}). Snippet: '{response_text_snippet}'")
                    return None
            except json.JSONDecodeError as json_err:
                # Handle JSON decoding error specifically (can happen even on 200 OK)
                response_text_snippet = await response.text() # Read text again if json failed
                response_text_snippet = mask_string(response_text_snippet[:500])
                logger.error(f"JSON Decode Error: {endpoint_name} (Status: {response.status}). Snippet: '{response_text_snippet}'. Err: {json_err}", exc_info=False)
                return None
            except Exception as e:
                # Catch other errors during response processing (e.g., reading body)
                # Try reading text if possible for context
                if response_text_snippet is None:
                     try: response_text_snippet = await response.text()
                     except: response_text_snippet = "[Could not read response text]"
                response_text_snippet = mask_string(response_text_snippet[:500])
                logger.error(f"Response Processing Error: {endpoint_name} (Status: {response.status}). Snippet: '{response_text_snippet}'. Err: {e}", exc_info=True)
                return None

    except asyncio.TimeoutError:
        logger.error(f"Timeout Error ({REQUEST_TIMEOUT_SECONDS}s): {endpoint_name} ({masked_url})", exc_info=False)
        return None
    except aiohttp.ClientError as client_err:
        # Handles connection errors, proxy errors, SSL errors etc.
        logger.error(f"Client Error: {endpoint_name} ({masked_url}). Err: {client_err}", exc_info=False) # Often details are in message
        return None
    except Exception as e:
        # Catch any other unexpected exceptions during the request attempt
        logger.error(f"Unexpected Fetch Error: {endpoint_name} ({masked_url}). Err: {e}", exc_info=True)
        return None

# --- Main Execution ---
async def main():
    """Main asynchronous function orchestrating the fetch and aggregation process."""
    # Setup logging first - subsequent logs will go to the correct file/console
    setup_logging()

    script_start_time = time.monotonic()
    logger.info(f"{COLOR_GREEN}--- Starting Market Data Sync Cycle ---{COLOR_RESET}")

    # Load configuration from environment variables
    api_key = os.getenv(API_KEY_ENV_VAR)
    base_url = os.getenv(BASE_URL_ENV_VAR)
    if not api_key or not base_url:
        logger.critical(f"CRITICAL: Required environment variables '{API_KEY_ENV_VAR}' or '{BASE_URL_ENV_VAR}' are not set. Exiting.")
        return

    # Establish database connection - exit if failed
    db_conn = get_db_connection(DB_FILE)
    if db_conn is None:
        logger.critical("CRITICAL: Failed to establish database connection. Exiting.")
        return

    # Get current time in UTC and local timezone
    now_utc = datetime.now(pytz.utc)
    now_local = now_utc.astimezone(DEFAULT_TIMEZONE)
    logger.debug(f"Cycle time: UTC={now_utc.isoformat()}, Local={now_local.isoformat()}")

    # --- 1. Determine and Fetch Raw Data ---
    apis_to_fetch_this_run: List[Tuple[str, Dict[str, Any]]] = []
    logger.info("--- Checking API Fetch Tasks ---")
    for name, config in API_ENDPOINTS.items():
        logger.debug(f"Checking fetch task: '{name}'")
        # Check if globally enabled
        if not config.get('enabled', False):
            logger.debug(f" -> Skip '{name}': Disabled in config.")
            continue
        # Check market hours if applicable
        if config.get('market_hours_apply', False) and not is_market_open(DEFAULT_TIMEZONE, TSE_MARKET_OPEN_TIME, TSE_MARKET_CLOSE_TIME, TSE_MARKET_DAYS):
            logger.debug(f" -> Skip '{name}': Market is closed.")
            continue
        # Check fetch interval based on last run time
        fetch_interval = timedelta(minutes=config.get('fetch_interval_minutes', 10))
        if not should_run_task(db_conn, f"fetch_{name}", fetch_interval, now_utc):
            logger.debug(f" -> Skip '{name}': Interval not met.")
            continue

        # If all checks pass, schedule for fetching
        logger.info(f"Scheduling fetch for: '{name}'")
        apis_to_fetch_this_run.append((name, config))

    # Execute API fetches concurrently if any are scheduled
    fetch_results = []
    if apis_to_fetch_this_run:
        logger.info(f"Attempting to fetch data for {len(apis_to_fetch_this_run)} endpoints...")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async def fetch_with_semaphore(session, name, cfg, key):
             async with semaphore:
                 return await fetch_api_data(session, name, cfg, base_url, key)

        # Create a single client session for connection pooling
        client_timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS + 5) # Slightly longer total timeout
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS) # Control concurrent connections
        async with aiohttp.ClientSession(connector=connector, timeout=client_timeout) as session:
            tasks = [fetch_with_semaphore(session, name, config, api_key) for name, config in apis_to_fetch_this_run]
            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)
    else:
        logger.info("No API endpoints scheduled for fetching in this cycle.")

    # --- Process Fetch Results ---
    processed_fetches = 0
    successful_raw_inserts = 0
    fetch_errors = 0
    if fetch_results:
        insert_time_utc = datetime.now(pytz.utc) # Use a consistent timestamp for all inserts in this batch
        logger.info("--- Processing Fetch Results ---")
        for i, result in enumerate(fetch_results):
            processed_fetches += 1
            # Get corresponding config using index, assuming order is preserved by gather
            endpoint_name, config = apis_to_fetch_this_run[i]
            fetch_task_name = f"fetch_{endpoint_name}"

            fetch_successful = False # Flag to track if this specific fetch led to data processing
            if isinstance(result, Exception):
                logger.error(f"Fetch Task Error (Caught by Gather): '{endpoint_name}'. Exception: {result}", exc_info=False)
                fetch_errors += 1
            elif result is not None:
                # Successful fetch and decode
                _name, _config, data = result
                # Attempt to insert raw data into DB
                if insert_raw_data(db_conn, endpoint_name, insert_time_utc, data):
                    successful_raw_inserts += 1
                    fetch_successful = True # Mark as successful data processed
                else:
                    fetch_errors += 1 # Count DB insert failure as error

                # Save the latest fetched data to a JSON file (always attempt even if DB fails)
                output_filename = os.path.join(DATA_FOLDER, config['output_filename'])
                try:
                    os.makedirs(DATA_FOLDER, exist_ok=True)
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False,
                                  indent=4 if PRETTY_PRINT_JSON else None,
                                  separators=(',', ':') if not PRETTY_PRINT_JSON else None)
                    logger.debug(f"Saved latest raw JSON: {os.path.basename(output_filename)}")
                except IOError as e:
                    logger.error(f"Failed to write latest JSON file {os.path.basename(output_filename)}: {e}", exc_info=False)
            else:
                # Fetch failed (non-200, timeout, decode error, etc.) - Error logged in fetch_api_data
                fetch_errors += 1

            # Update last run time for the fetch task regardless of success/failure
            # This prevents immediate retries on failing endpoints
            update_last_run_time(db_conn, fetch_task_name, now_utc)

        logger.info(f"Fetch Results Summary: Processed={processed_fetches}, DB Inserts={successful_raw_inserts}, Errors={fetch_errors}")

    # --- 2. Run Aggregations ---
    logger.info("--- Checking Aggregation Tasks ---")
    agg_success = 0
    agg_skipped = 0
    agg_errors = 0
    for endpoint_name, config in API_ENDPOINTS.items():
        if not config.get('enabled', False): continue # Skip disabled endpoints

        levels = config.get('aggregation_levels', [])
        if not levels: continue # Skip if no aggregation levels defined

        logger.debug(f"Checking aggregations for '{endpoint_name}': Levels={levels}")
        for level in levels:
            if level not in AGGREGATION_INTERVALS:
                logger.warning(f"Invalid aggregation level '{level}' configured for '{endpoint_name}'. Skipping.")
                continue

            interval = AGGREGATION_INTERVALS[level]
            agg_task_name = f"aggregate_{level}_{endpoint_name}"
            run_agg = False
            target_ts: Union[datetime, date, None] = None
            start_ts_utc: Optional[datetime] = None
            end_ts_utc: Optional[datetime] = now_utc # Default end time is now

            # Determine if aggregation should run based on type (daily vs interval) and timing
            if level == 'daily':
                # Daily runs after midnight for the *previous* complete day
                if now_local.time() >= DAILY_AGGREGATION_TIME_LOCAL:
                    last_run_daily_utc = get_last_run_time(db_conn, agg_task_name)
                    # Run if never run OR last run was for a day before yesterday (local time comparison)
                    should_run_daily = False
                    if last_run_daily_utc is None:
                        should_run_daily = True
                        logger.info(f"Daily task '{agg_task_name}' due: Never run before.")
                    else:
                        last_run_local_date = last_run_daily_utc.astimezone(DEFAULT_TIMEZONE).date()
                        yesterday_local_date = now_local.date() - timedelta(days=1)
                        # Run if last run was before yesterday (meaning yesterday hasn't been processed)
                        if last_run_local_date < yesterday_local_date:
                            should_run_daily = True
                            logger.info(f"Daily task '{agg_task_name}' due: Last run ({last_run_local_date}) was before yesterday ({yesterday_local_date}).")
                        else:
                            logger.debug(f"Skip Daily '{agg_task_name}': Already processed up to or including yesterday ({last_run_local_date}).")

                    if should_run_daily:
                        run_agg = True
                        # Target is the date of the day being aggregated (yesterday)
                        target_ts = now_local.date() - timedelta(days=1)
                        # Period is midnight to midnight of the target date, converted to UTC
                        start_dt_local = datetime.combine(target_ts, dt_time.min)
                        end_dt_local = datetime.combine(target_ts + timedelta(days=1), dt_time.min) # End is exclusive start of next day
                        start_ts_utc = DEFAULT_TIMEZONE.localize(start_dt_local).astimezone(pytz.utc)
                        end_ts_utc = DEFAULT_TIMEZONE.localize(end_dt_local).astimezone(pytz.utc)
                else:
                    logger.debug(f"Skip Daily '{agg_task_name}': Trigger time {DAILY_AGGREGATION_TIME_LOCAL} not reached yet ({now_local.time()}).")
            else: # Interval-based aggregations (6h, 12h, etc.)
                if should_run_task(db_conn, agg_task_name, interval, now_utc):
                    run_agg = True
                    # Target timestamp is the end of the interval (now_utc)
                    target_ts = now_utc
                    # Period starts 'interval' duration before now
                    start_ts_utc = now_utc - interval
                    end_ts_utc = now_utc # End boundary is current time

            # Execute aggregation if scheduled and times are valid
            if run_agg and target_ts is not None and start_ts_utc is not None and end_ts_utc is not None:
                logger.info(f"Running Aggregation: '{agg_task_name}' target={target_ts.isoformat()} period=[{start_ts_utc.isoformat()} to {end_ts_utc.isoformat()})")
                try:
                    # Call the main aggregation function
                    if calculate_and_store_aggregates(db_conn, endpoint_name, level, config, start_ts_utc, end_ts_utc, target_ts):
                        agg_success += 1
                    else:
                        # Failure already logged inside calculate_and_store_aggregates
                        agg_errors += 1
                except Exception as agg_e:
                     # Catch unexpected errors during the call itself
                     logger.error(f"Unexpected error calling calculate_and_store_aggregates for {agg_task_name}: {agg_e}", exc_info=True)
                     agg_errors += 1
            elif run_agg:
                 # Should not happen if logic is correct, but catch potential None timestamps
                 logger.warning(f"Aggregation scheduled for {agg_task_name} but start/end/target timestamps were invalid. Skipping.")
                 agg_skipped += 1
            else:
                # Skipped due to timing or market hours - already logged why
                agg_skipped += 1

    logger.info(f"Aggregation Results Summary: Success={agg_success}, Skipped={agg_skipped}, Errors={agg_errors}")

    # --- Finish ---
    try:
        if db_conn:
            # Optional: Add checkpoint to force WAL to main db file if needed frequently
            # logger.debug("Checkpointing DB...")
            # db_conn.execute("CHECKPOINT;")
            db_conn.close()
            logger.info("DB connection closed.")
    except Exception as e:
        logger.error(f"Error during DB close/cleanup: {e}", exc_info=True)

    script_end_time = time.monotonic()
    total_duration = script_end_time - script_start_time
    logger.info(f"{COLOR_GREEN}--- Market Data Sync Cycle Finished in {total_duration:.2f} seconds ---{COLOR_RESET}")

    # --- Post-Execution Suggestions ---
    # These are printed to the console log for the user
    logger.info("Improvement Suggestion 1: Implement more specific error handling for common API issues (e.g., rate limits, maintenance status codes).")
    logger.info("Improvement Suggestion 2: Consider adding data quality checks (e.g., price outliers, missing symbols) after fetching or before aggregation.")

# --- Script Entry Point ---
if __name__ == "__main__":
    # Load .env file for local development (optional)
    try:
        from dotenv import load_dotenv
        script_dir = os.path.dirname(__file__)
        # Look for .env in the parent directory relative to src/ (common structure)
        dotenv_path = os.path.join(script_dir, '..', '.env')
        if os.path.exists(dotenv_path):
             if load_dotenv(dotenv_path=dotenv_path, verbose=True): # verbose=True logs which file is loaded
                 # Logger might not be fully setup here, use print
                 print(f"{COLOR_BLUE}•{COLOR_RESET} Loaded environment variables from: {dotenv_path}")
             else:
                 print(f"{COLOR_YELLOW}⚠️{COLOR_RESET} .env file found but loading failed or empty: {dotenv_path}")
        else:
            print(f"{COLOR_BLUE}•{COLOR_RESET} No .env file found at: {dotenv_path}. Relying on system environment variables.")
    except ImportError:
        print(f"{COLOR_YELLOW}⚠️ {COLOR_RESET} python-dotenv library not installed, cannot load .env file.")
    except Exception as e:
        print(f"{COLOR_RED}✗ Error loading .env file: {e}{COLOR_RESET}")

    # Run the main asynchronous function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Use print as logger might be shutdown
        print(f"\n{COLOR_YELLOW}⚠️ Script interrupted by user.{COLOR_RESET}")
    except Exception as e:
        # Catch any unhandled exceptions right at the top level
        # Use logger if available, otherwise print critical error message
        if logger.handlers:
            logger.critical(f"Unhandled critical exception in main execution: {e}", exc_info=True)
        else:
            print(f"{COLOR_RED}--- CRITICAL UNHANDLED EXCEPTION ---{COLOR_RESET}")
            print(f"{COLOR_RED}✗ Error: {e}{COLOR_RESET}")
            traceback.print_exc()
            print(f"{COLOR_RED}--- END CRITICAL EXCEPTION ---{COLOR_RESET}")
