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
from datetime import datetime, time as dt_time, timedelta, timezone as dt_timezone
from logging.handlers import TimedRotatingFileHandler
import traceback
from typing import Dict, Any, Optional, List, Tuple, Sequence
import math # For checking float NaN

# --- Configuration ---

# General Settings
BASE_URL_ENV_VAR: str = "BRS_BASE_URL"
API_KEY_ENV_VAR: str = "BRS_API_KEY"
LOG_LEVEL_ENV_VAR: str = "LOG_LEVEL" #(DEBUG, INFO, WARNING, ERROR)
DEFAULT_LOG_LEVEL: str = "INFO"       # Default log level if env var is not set

LOG_FOLDER: str = "logs"
LOG_FILE_NAME: str = "market_data_sync.log"
LOG_ROTATION_HOURS: int = 48
DATA_FOLDER: str = "data"
DB_FILE: str = os.path.join(DATA_FOLDER, "market_data.duckdb")
STATE_FILE: str = os.path.join(DATA_FOLDER, ".sync_state.json")

REQUEST_TIMEOUT_SECONDS: int = 15
PRETTY_PRINT_JSON: bool = False
MAX_CONCURRENT_REQUESTS: int = 10
TIMEZONE: str = "Asia/Tehran"

# User Agent (Update periodically)
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
HEADERS: Dict[str, str] = {
    "User-Agent": USER_AGENT, "Accept": "application/json, text/plain, */*",
}

# API Endpoints Configuration
# NEW: 'price_json_path': JSON path (using '.' for nesting) to extract the numeric price for aggregation. Use None if not applicable.
# 'aggregation_enabled': Enable calculation of aggregated data for this endpoint?
API_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "gold_currency": {
        "relative_url": "/Api/Market/Gold_Currency.php?key={api_key}",
        "output_filename": "gold_currency.json",
        "raw_table": "gold_currency_history", # Changed key name for clarity
        "fetch_interval_minutes": 10,
        "market_hours_apply": False,
        "enabled": True,
        "aggregation_enabled": True,
        "price_json_path": "price", # Assuming 'price' key holds the numeric value
    },
    "crypto": {
        "relative_url": "/Api/Market/Cryptocurrency.php?key={api_key}",
        "output_filename": "cryptocurrency.json",
        "raw_table": "crypto_history",
        "fetch_interval_minutes": 10,
        "market_hours_apply": False,
        "enabled": True,
        "aggregation_enabled": True,
        # Assuming price_toman is the reference for aggregation
        "price_json_path": "price_toman",
    },
    "commodity": {
        "relative_url": "/Api/Market/Commodity.php?key={api_key}",
        "output_filename": "commodity.json",
        "raw_table": "commodity_history",
        "fetch_interval_minutes": 10,
        "market_hours_apply": False, # Global commodity markets have varied hours
        "enabled": True,
        "aggregation_enabled": True,
        "price_json_path": "price", # Assuming 'price' key
    },
    "tse_options": {
        "relative_url": "/Api/Tsetmc/Option.php?key={api_key}",
        "output_filename": "tse_options.json",
        "raw_table": "tse_options_history",
        "fetch_interval_minutes": 20, # Target interval
        "market_hours_apply": True,
        "enabled": False, # Disabled by default
        "aggregation_enabled": False, # Aggregation might not make sense for options?
        "price_json_path": None, # TBD if needed
    },
    "tse_nav": {
        "relative_url": "/Api/Tsetmc/Nav.php?key={api_key}",
        "output_filename": "tse_nav.json",
        "raw_table": "tse_nav_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": False, # Disabled by default
        "aggregation_enabled": True,
        "price_json_path": "pl", # Assuming 'pl' (Last Price) is the value
    },
    "tse_index": {
        "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=1",
        "output_filename": "tse_index.json",
        "raw_table": "tse_index_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": False, # Disabled by default
        "aggregation_enabled": True,
        "price_json_path": "index",
    },
    "ifb_index": {
        "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=2",
        "output_filename": "ifb_index.json",
        "raw_table": "ifb_index_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": False, # Disabled by default
        "aggregation_enabled": True,
        "price_json_path": "index",
    },
    "selected_indices": {
        "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=3",
        "output_filename": "selected_indices.json",
        "raw_table": "selected_indices_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": False, # Disabled by default
        "aggregation_enabled": True,
        "price_json_path": "index",
    },
    "tse_ifb_symbols": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=1",
        "output_filename": "tse_ifb_symbols.json",
        "raw_table": "tse_ifb_symbols_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": True,
        "aggregation_enabled": True,
        "price_json_path": "pl", # Assuming last price 'pl'
    },
    "debt_securities": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=4",
        "output_filename": "debt_securities.json",
        "raw_table": "debt_securities_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": True,
        "aggregation_enabled": True,
        "price_json_path": "pl", # Assuming last price 'pl'
    },
    "housing_facilities": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=5",
        "output_filename": "housing_facilities.json",
        "raw_table": "housing_facilities_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": True,
        "aggregation_enabled": True,
        "price_json_path": "pl", # Assuming last price 'pl'
    },
    "futures": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=3",
        "output_filename": "futures.json",
        "raw_table": "futures_history",
        "fetch_interval_minutes": 20,
        "market_hours_apply": True,
        "enabled": True,
        "aggregation_enabled": False, # Aggregation less common for futures?
        "price_json_path": None, # TBD if needed
    },
}

# Market Hours (Adjust if necessary)
TSE_MARKET_OPEN_TIME: dt_time = dt_time(8, 45)
TSE_MARKET_CLOSE_TIME: dt_time = dt_time(12, 45)
TSE_MARKET_DAYS: List[int] = [5, 6, 0, 1, 2] # Sat-Wed

# Aggregation Timeframes
AGGREGATION_PERIODS: Dict[str, timedelta] = {
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "daily": timedelta(days=1),
    "3d": timedelta(days=3),
    "weekly": timedelta(weeks=1),
}

# --- ANSI Color Codes ---
COLOR_GREEN = "\033[32m"
COLOR_RED = "\033[31m"
COLOR_YELLOW = "\033[33m"
COLOR_BLUE = "\033[34m"
COLOR_RESET = "\033[0m"

# --- Global Logger ---
logger = logging.getLogger("MarketDataSync") # Changed logger name

# --- Utility Functions ---
def mask_sensitive_data(url: str) -> str:
    """Replaces sensitive query parameters like 'key' with MASKED."""
    try:
        from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        # Mask 'key' or other sensitive params
        for key in list(query_params.keys()):
             if key.lower() == 'key': # Case-insensitive check
                 query_params[key] = ['***MASKED***']
        # Rebuild URL
        new_query = urlencode(query_params, doseq=True)
        # Use urlunparse components: scheme, netloc, path, params, query, fragment
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        # Fallback if URL parsing fails
        return url.split("?")[0] + "?***PARAMS_MASKED***"

def get_nested_value(data: Any, path: str, default: Any = None) -> Any:
    """Safely retrieves a nested value from dict/list using dot notation path."""
    if not path: return data
    keys = path.split('.')
    value = data
    try:
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list):
                try:
                    value = value[int(key)]
                except (IndexError, ValueError):
                    return default
            else:
                return default
            if value is None:
                return default
        return value
    except Exception:
        return default

# --- Logging Setup ---
def setup_logging() -> None:
    """Configures logging based on LOG_LEVEL env var."""
    log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, DEFAULT_LOG_LEVEL).upper()
    log_level = getattr(logging, log_level_str, logging.INFO) # Default to INFO if invalid level

    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    os.makedirs(LOG_FOLDER, exist_ok=True)
    cleanup_old_logs(LOG_FOLDER, LOG_ROTATION_HOURS)
    log_file_path = os.path.join(LOG_FOLDER, LOG_FILE_NAME)

    # File handler - logs everything from the set level downwards
    file_handler = TimedRotatingFileHandler(
        log_file_path, when="H", interval=1, # Rotate hourly for potentially large logs
        backupCount=LOG_ROTATION_HOURS, encoding='utf-8'
    )
    file_handler.setLevel(log_level) # File logs at the configured level

    # Console handler - typically logs INFO and above, regardless of DEBUG setting for file
    console_log_level = max(log_level, logging.INFO) # Show at least INFO on console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)

    class ColorFormatter(logging.Formatter):
        # (Same as before)
        level_colors = {
            logging.DEBUG: COLOR_BLUE, logging.INFO: COLOR_GREEN,
            logging.WARNING: COLOR_YELLOW, logging.ERROR: COLOR_RED, logging.CRITICAL: COLOR_RED,
        }
        def format(self, record):
            color = self.level_colors.get(record.levelno, COLOR_RESET)
            # Format levelname and message for color output
            record.levelname = f"{color}{record.levelname}{COLOR_RESET}"
            # Apply color only to the message part for console
            message = record.getMessage()
            record.msg = f"{color}{message}{COLOR_RESET}"

            # Use a simpler format for console, applying colors correctly
            log_fmt = f"{color}•{COLOR_RESET} %(asctime)s [%(levelname)s] %(message)s"
            formatter = logging.Formatter(log_fmt, datefmt="%H:%M:%S") # Use simpler time for console
            # Handle potential formatting issues if record.msg was already formatted
            try:
                return formatter.format(record)
            except Exception:
                 # Fallback if custom formatting fails
                 record.msg = message # Reset message to original
                 std_formatter = logging.Formatter(f"• %(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
                 return std_formatter.format(record)


    console_handler.setFormatter(ColorFormatter())

    # File formatter - no colors, more detail
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s:%(lineno)d - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info(f"Logging initialized. Level set to: {log_level_str} ({log_level}). Console logs >= INFO.")
    logger.debug("Debug logging enabled.") # This will only show if level is DEBUG

def cleanup_old_logs(log_directory: str, hours_to_keep: int) -> None:
    """Removes log files older than specified hours."""
    cutoff = time.time() - (hours_to_keep * 60 * 60)
    logger.debug(f"Running log cleanup. Keeping logs newer than {hours_to_keep} hours.")
    cleaned_count = 0
    try:
        # Match base name + rotation pattern (e.g., .log.2024-04-25_10)
        for filename in glob.glob(os.path.join(log_directory, LOG_FILE_NAME + "*")):
            if os.path.isfile(filename) and filename != os.path.join(log_directory, LOG_FILE_NAME): # Don't delete current log
                if os.path.getmtime(filename) < cutoff:
                    try:
                        os.remove(filename)
                        logger.info(f"Cleaned up old log file: {os.path.basename(filename)}")
                        cleaned_count += 1
                    except OSError as e:
                        logger.warning(f"Could not remove old log file {os.path.basename(filename)}: {e}")
        logger.debug(f"Log cleanup finished. Removed {cleaned_count} old files.")
    except Exception as e:
        logger.error(f"Error during log cleanup process: {e}", exc_info=True)


# --- State Management ---
def load_state(filepath: str) -> Dict[str, Any]:
    """Loads the sync state from a JSON file."""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                state = json.load(f)
                logger.info(f"Loaded sync state from {filepath}")
                return state
    except (IOError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load state file {filepath}: {e}. Starting with default state.")
    return {"last_tse_run_pattern": None} # Default state

def save_state(filepath: str, state: Dict[str, Any]) -> None:
    """Saves the sync state to a JSON file."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Saved sync state to {filepath}")
    except IOError as e:
        logger.error(f"Could not save state file {filepath}: {e}")


# --- Timezone and Market Hours Check ---
def is_market_open(tz_str: str, open_time: dt_time, close_time: dt_time, market_days: List[int]) -> bool:
    """Checks if the current time is within market hours for the given timezone."""
    try:
        timezone = pytz.timezone(tz_str)
        now_local = datetime.now(timezone)
        current_time = now_local.time()
        current_weekday = now_local.weekday()
        logger.debug(f"Market check: Local time {now_local.strftime('%H:%M:%S %Z')} ({current_weekday=}, {current_time=})")

        if current_weekday not in market_days:
            logger.debug(f"Market check: Day ({current_weekday}) outside market days {market_days}.")
            return False

        is_open = open_time <= current_time < close_time
        logger.debug(f"Market check: Hours ({open_time}-{close_time}). Open: {is_open}")
        return is_open
    except Exception as e:
        logger.error(f"Error checking market hours: {e}", exc_info=True)
        return False

# --- Database Operations ---
def get_db_connection(db_path: str) -> Optional[duckdb.DuckDBPyConnection]:
    """Establishes and returns a DuckDB connection. Returns None on failure."""
    try:
        logger.debug(f"Connecting to DB: {db_path}")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = duckdb.connect(database=db_path, read_only=False)
        # Set timezone for the session, important for TIMESTAMPTZ comparisons
        conn.execute("SET TimeZone='UTC';")
        logger.info(f"DB connection successful: {db_path}")
        return conn
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to connect to DB '{db_path}': {e}", exc_info=True)
        return None

def close_db_connection(conn: Optional[duckdb.DuckDBPyConnection], db_path: str) -> None:
    """Safely closes the DB connection."""
    if conn:
        try:
            conn.close()
            logger.info(f"DB connection closed: {db_path}")
        except Exception as e:
            logger.error(f"Error closing DB connection '{db_path}': {e}", exc_info=True)

def ensure_table(conn: duckdb.DuckDBPyConnection, table_name: str, schema: str) -> bool:
    """Creates the specified table with the given schema if it doesn't exist."""
    # Use double quotes for table name safety
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({schema});'
    try:
        conn.execute(create_sql)
        logger.debug(f"Ensured table '{table_name}' exists.")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure table '{table_name}': {e}\nSQL: {create_sql}", exc_info=True)
        return False

def define_schemas() -> Dict[str, str]:
    """Defines SQL schemas for different table types."""
    schemas = {
        "raw_history": "timestamp TIMESTAMPTZ PRIMARY KEY, source_endpoint TEXT, data JSON",
        "aggregation_tracker": "aggregation_type TEXT PRIMARY KEY, last_run_utc_ts TIMESTAMPTZ",
        # Schema for aggregated tables (median)
        "aggregated_median": """
            period_start_utc_ts TIMESTAMPTZ,
            period_end_utc_ts TIMESTAMPTZ,
            symbol TEXT,                 -- Identifier (e.g., 'USD', 'BTC', 'GOLD_18K', stock symbol)
            median_price DECIMAL(18, 6), -- Assuming price needs precision
            source_count INTEGER,        -- How many raw data points were used
            PRIMARY KEY (period_start_utc_ts, symbol) -- Unique combo
        """
    }
    return schemas

def initialize_database_schema(conn: duckdb.DuckDBPyConnection, schemas: Dict[str, str], endpoints: Dict[str, Dict[str, Any]]) -> bool:
    """Ensures all necessary tables exist based on config."""
    all_tables_ok = True
    # Ensure tracker table
    if not ensure_table(conn, "aggregation_tracker", schemas["aggregation_tracker"]):
        all_tables_ok = False

    # Ensure tables for each endpoint
    for name, config in endpoints.items():
        if not config.get('enabled', False): continue

        # Ensure raw history table
        raw_table = config.get("raw_table")
        if raw_table:
            if not ensure_table(conn, raw_table, schemas["raw_history"]):
                all_tables_ok = False
        else:
            logger.warning(f"Endpoint '{name}' is enabled but has no 'raw_table' defined.")

        # Ensure aggregation tables if enabled
        if config.get("aggregation_enabled", False) and raw_table and config.get("price_json_path"):
            for period_key in AGGREGATION_PERIODS.keys():
                agg_table_name = f"{raw_table}_{period_key}"
                if not ensure_table(conn, agg_table_name, schemas["aggregated_median"]):
                    all_tables_ok = False
    return all_tables_ok


def insert_raw_data(conn: duckdb.DuckDBPyConnection, table_name: str, endpoint_name: str, timestamp: datetime, data: Any) -> bool:
    """Inserts raw fetched data into the history table."""
    try:
        json_data_str = json.dumps(data, ensure_ascii=False)
        conn.execute(
            f'INSERT INTO "{table_name}" (timestamp, source_endpoint, data) VALUES (?, ?, ?)',
            (timestamp, endpoint_name, json_data_str)
        )
        logger.debug(f"Raw data inserted into '{table_name}' for {endpoint_name} @ {timestamp}.")
        return True
    except duckdb.ConstraintException:
         logger.warning(f"Duplicate timestamp {timestamp} for raw table '{table_name}'. Skipping.")
         return True # Not an error in this context
    except Exception as e:
        logger.error(f"Failed to insert raw data into '{table_name}': {e}", exc_info=True)
        return False


def get_last_aggregation_time(conn: duckdb.DuckDBPyConnection, agg_type: str) -> Optional[datetime]:
    """Gets the last run time for a specific aggregation type."""
    try:
        result = conn.execute("SELECT last_run_utc_ts FROM aggregation_tracker WHERE aggregation_type = ?", (agg_type,)).fetchone()
        if result and result[0]:
             # Ensure the result is timezone-aware (UTC)
             ts = result[0]
             if isinstance(ts, datetime):
                 return ts.replace(tzinfo=dt_timezone.utc) if ts.tzinfo is None else ts.astimezone(dt_timezone.utc)
             else: # Handle potential non-datetime types if DB schema changes
                 logger.warning(f"Unexpected type for last_run_utc_ts in tracker: {type(ts)}")
                 # Attempt conversion if possible, otherwise return None
                 try:
                     # Example: Assuming it might be a string
                     parsed_ts = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
                     return parsed_ts.replace(tzinfo=dt_timezone.utc) if parsed_ts.tzinfo is None else parsed_ts.astimezone(dt_timezone.utc)
                 except ValueError:
                      logger.error(f"Could not parse last run time for {agg_type}: {ts}")
                      return None

        return None # No previous run found
    except Exception as e:
        logger.error(f"Error fetching last aggregation time for '{agg_type}': {e}", exc_info=True)
        return None


def update_last_aggregation_time(conn: duckdb.DuckDBPyConnection, agg_type: str, run_time: datetime) -> None:
    """Updates the last run time for an aggregation type."""
    try:
        # Use REPLACE INTO (UPSERT semantic)
        conn.execute(
            "INSERT OR REPLACE INTO aggregation_tracker (aggregation_type, last_run_utc_ts) VALUES (?, ?)",
            (agg_type, run_time)
        )
        logger.debug(f"Updated last run time for aggregation '{agg_type}' to {run_time}.")
    except Exception as e:
        logger.error(f"Error updating last aggregation time for '{agg_type}': {e}", exc_info=True)


def run_aggregation(
    conn: duckdb.DuckDBPyConnection,
    source_table: str,
    target_table: str,
    price_json_path: str,
    period_start_utc: datetime,
    period_end_utc: datetime,
    aggregation_type: str # For logging
    ) -> bool:
    """Calculates median price for symbols in a period and saves to target table."""

    logger.info(f"Running aggregation '{aggregation_type}' for table '{source_table}'...")
    logger.info(f"Period: {period_start_utc} -> {period_end_utc}")
    logger.debug(f"Target table: '{target_table}', Price path: '$.{price_json_path}'")

    # Need to handle different JSON structures within the 'data' column.
    # DuckDB's json_extract_string is useful.
    # We assume the 'data' column contains either a dict or a list of dicts.
    # We need a symbol identifier and the price.

    # Heuristic: Guess symbol key (common names) - this is brittle!
    # A better approach would be to add 'symbol_json_path' to API_ENDPOINTS config.
    # For now, let's try common names.
    symbol_paths = ["symbol", "name", "l18", "name_en"]
    symbol_path_sql = ""
    for i, p in enumerate(symbol_paths):
        symbol_path_sql += f"json_extract_string(data, '$.{p}')"
        if i < len(symbol_paths) - 1:
            symbol_path_sql += ", "
    symbol_sql = f"COALESCE({symbol_path_sql}, 'UNKNOWN')" # Fallback symbol


    # Handle list vs dict in JSON data column
    # This query attempts to handle both cases:
    # 1. If data is a list, unnest it first.
    # 2. If data is a dict, treat it as a single item.
    # 3. Extract symbol and price.
    # 4. Calculate median.
    aggregation_sql = f"""
    INSERT INTO "{target_table}" (period_start_utc_ts, period_end_utc_ts, symbol, median_price, source_count)
    WITH RawData AS (
        SELECT
            timestamp,
            json_extract(data, '$') as json_obj -- Ensure we work with JSON objects
        FROM "{source_table}"
        WHERE timestamp >= ? AND timestamp < ? -- Filter by period
    ),
    UnnestedData AS (
        -- If json_obj is a list, unnest; otherwise, keep as is in a list structure
        SELECT
            timestamp,
            CASE
                WHEN json_type(json_obj) = 'ARRAY' THEN unnest(json_transform(json_obj, '[{{symbol: x ->> "{symbol_paths[0]}", price: x ->> "{price_json_path}" }}]')) -- Example paths
                ELSE json_transform(json_obj, '{{symbol: x ->> "{symbol_paths[0]}", price: x ->> "{price_json_path}" }}') -- Example paths
            END as item
        FROM RawData
        WHERE json_obj IS NOT NULL
    ),
    ExtractedData AS (
         SELECT
             json_extract_string(item, '$.symbol') as symbol,
             -- Try casting price to DECIMAL, handle errors gracefully
             TRY_CAST(json_extract_string(item, '$.price') AS DECIMAL(18, 6)) as price
         FROM UnnestedData
         WHERE item IS NOT NULL AND json_type(item) = 'OBJECT' -- Ensure item is an object
    )
    SELECT
        ? AS period_start_utc_ts,       -- Parameter for start time
        ? AS period_end_utc_ts,         -- Parameter for end time
        COALESCE(symbol, 'UNKNOWN') AS symbol, -- Use fallback if symbol extraction failed
        median(price) AS median_price, -- Calculate median for numeric prices
        count(price) AS source_count   -- Count non-null prices used
    FROM ExtractedData
    WHERE price IS NOT NULL AND NOT isnan(price) -- Exclude non-numeric or NaN prices
    GROUP BY symbol -- Group by the extracted symbol
    HAVING count(price) > 0 -- Only insert if there are valid data points
    ON CONFLICT (period_start_utc_ts, symbol) DO UPDATE SET
        median_price = excluded.median_price,
        source_count = excluded.source_count,
        period_end_utc_ts = excluded.period_end_utc_ts; -- Update existing record for the period/symbol
    """
    # Note: The JSON path extraction (e.g., '$.price') might need adjustment per API.
    # Using 'symbol_paths[0]' and 'price_json_path' is a simplification here.
    # A robust solution would involve more complex SQL or preprocessing.

    try:
        conn.execute(aggregation_sql, (period_start_utc, period_end_utc, period_start_utc, period_end_utc))
        logger.info(f"Successfully completed aggregation '{aggregation_type}' for '{source_table}'.")
        return True
    except Exception as e:
        logger.error(f"Failed aggregation '{aggregation_type}' for '{source_table}': {e}", exc_info=True)
        logger.error(f"Aggregation SQL (simplified):\n{aggregation_sql[:1000]}...") # Log part of the SQL
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
    masked_url = mask_sensitive_data(full_url) # Mask URL for logging
    start_time = time.monotonic()

    try:
        logger.debug(f"Requesting: {endpoint_name} ({masked_url})") # Log masked URL
        async with session.get(full_url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            elapsed_time = time.monotonic() - start_time
            logger.debug(f"Response: {endpoint_name} ({response.status}) in {elapsed_time:.2f}s")

            if response.status == 200:
                try:
                    data = await response.json(content_type=None)
                    logger.info(f"Success: Fetched '{endpoint_name}'.")
                    return endpoint_name, config, data
                except json.JSONDecodeError as json_err:
                    # Log response text only if DEBUG level is enabled
                    response_text = await response.text()
                    logger.error(f"JSON Decode Error: {endpoint_name} ({masked_url}) Status: {response.status}.", exc_info=True)
                    logger.debug(f"Response Text (first 500 chars): {response_text[:500]}") # DEBUG level for full text
                    return None
                except Exception as e:
                    logger.error(f"Response Processing Error: {endpoint_name} ({masked_url}) Status: {response.status}.", exc_info=True)
                    return None
            else:
                response_text = await response.text()
                logger.error(f"API Request Failed: {endpoint_name} ({masked_url}). Status: {response.status}")
                logger.debug(f"Failed Response Text (first 500 chars): {response_text[:500]}") # DEBUG level for full text
                return None

    except asyncio.TimeoutError:
        elapsed_time = time.monotonic() - start_time
        logger.error(f"Timeout Error ({REQUEST_TIMEOUT_SECONDS}s): {endpoint_name} ({masked_url}). Elapsed: {elapsed_time:.2f}s")
        return None
    except aiohttp.ClientError as client_err:
        elapsed_time = time.monotonic() - start_time
        logger.error(f"Client Error: {endpoint_name} ({masked_url}). Error: {client_err}", exc_info=True)
        return None
    except Exception as e:
        elapsed_time = time.monotonic() - start_time
        logger.error(f"Unexpected Fetch Error: {endpoint_name} ({masked_url}). Error: {e}", exc_info=True)
        return None

# --- Main Execution Logic ---
async def run_fetch_cycle(state: Dict[str, Any]):
    """Runs one cycle of fetching and saving raw data."""
    logger.info(f"{COLOR_BLUE}--- Starting Data Fetch Cycle ---{COLOR_RESET}")
    cycle_start_time = time.monotonic()

    # --- Get Secrets ---
    api_key = os.getenv(API_KEY_ENV_VAR)
    base_url = os.getenv(BASE_URL_ENV_VAR)
    if not api_key or not base_url:
        logger.critical("API Key or Base URL not configured in environment variables. Aborting fetch cycle.")
        return False, state # Indicate failure

    # --- Determine APIs to Fetch ---
    apis_to_fetch_this_run: List[Tuple[str, Dict[str, Any]]] = []
    now_utc = datetime.now(dt_timezone.utc)
    now_local = now_utc.astimezone(pytz.timezone(TIMEZONE))
    current_minute = now_local.minute
    # Pattern for TSE runs: 0 for even 10-min slots (0-9, 20-29, 40-49), 1 for odd (10-19, 30-39, 50-59)
    current_tse_pattern = (current_minute // 10) % 2

    for name, config in API_ENDPOINTS.items():
        logger.debug(f"Checking: '{name}'")
        if not config.get('enabled', False):
            logger.debug(f" -> Skip: Disabled.")
            continue

        # Check market hours
        if config.get('market_hours_apply', False):
            market_is_open = is_market_open(TIMEZONE, TSE_MARKET_OPEN_TIME, TSE_MARKET_CLOSE_TIME, TSE_MARKET_DAYS)
            if not market_is_open:
                logger.debug(f" -> Skip: Market closed.")
                continue
            else: # Market is open, now check 20-min interval logic
                 # Run if interval is 20 AND it's an "even" 10-minute slot
                interval = config.get('fetch_interval_minutes', 10)
                if interval >= 20 and current_tse_pattern != 0:
                     logger.debug(f" -> Skip: {interval}min interval, but it's an odd pattern ({current_minute=}).")
                     continue

        logger.debug(f" -> Adding '{name}' to fetch list.")
        apis_to_fetch_this_run.append((name, config))

    if not apis_to_fetch_this_run:
        logger.info("No APIs to fetch in this cycle (check enabled/market hours/intervals).")
        state['last_tse_run_pattern'] = current_tse_pattern # Still update state
        return True, state # Successful cycle, just no fetches

    # --- Fetch Data Concurrently ---
    # (Fetch logic remains largely the same as previous main(), using fetch_api_data)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async def fetch_with_semaphore(session, name, cfg, key):
         async with semaphore:
             return await fetch_api_data(session, name, cfg, base_url, key)

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        tasks = [
            fetch_with_semaphore(session, name, config, api_key)
            for name, config in apis_to_fetch_this_run
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # --- Process Results ---
    db_conn: Optional[duckdb.DuckDBPyConnection] = None
    raw_inserted_count = 0
    json_saved_count = 0
    error_count = 0
    data_fetched = False # Flag to check if any data was actually saved

    try:
        db_conn = get_db_connection(DB_FILE)
        if not db_conn:
            logger.critical("DB connection failed. Cannot save fetched data to DB.")
            # Decide how to handle - maybe save JSON only? For now, log critical and continue.
            # Set error count high to indicate major issue?
            error_count = len(results) # Treat all as errors if DB fails
        else:
            # Ensure raw tables exist (only needs to run once effectively, but safe to run each time)
             schemas = define_schemas()
             if not initialize_database_schema(db_conn, schemas, API_ENDPOINTS):
                  logger.critical("Failed to initialize database schema. Aborting save.")
                  error_count = len(results)


        if error_count == 0: # Proceed only if DB connection and schema seem OK
            timestamp_utc = datetime.now(dt_timezone.utc) # Consistent timestamp for this batch

            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Fetch task failed: {result}", exc_info=True)
                    error_count += 1
                    continue
                if result is None: # Fetch failed, already logged
                    error_count += 1
                    continue

                # Successful fetch
                endpoint_name, config, data = result
                output_filename = os.path.join(DATA_FOLDER, config['output_filename'])
                raw_table = config.get('raw_table')

                # Save JSON
                try:
                    os.makedirs(DATA_FOLDER, exist_ok=True)
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False,
                                  indent=4 if PRETTY_PRINT_JSON else None,
                                  separators=(',', ':') if not PRETTY_PRINT_JSON else None)
                    logger.info(f"Saved JSON: {os.path.basename(output_filename)}")
                    json_saved_count += 1
                    data_fetched = True # Mark that we got some data
                except IOError as e:
                    logger.error(f"Failed to write JSON: {os.path.basename(output_filename)}: {e}", exc_info=True)
                    error_count += 1
                    continue # Don't try DB insert if JSON failed

                # Save to Raw DB Table (if configured and DB connection exists)
                if raw_table and db_conn:
                    if insert_raw_data(db_conn, raw_table, endpoint_name, timestamp_utc, data):
                        raw_inserted_count += 1
                    else:
                        error_count += 1

    except Exception as e:
         logger.critical(f"Critical error during result processing: {e}", exc_info=True)
         error_count += 1 # Increment general error count
    finally:
        close_db_connection(db_conn, DB_FILE) # Ensure connection is closed

    state['last_tse_run_pattern'] = current_tse_pattern # Update state regardless of success
    cycle_duration = time.monotonic() - cycle_start_time
    logger.info(f"Fetch cycle summary: JSON={json_saved_count}, DB Raw Inserts={raw_inserted_count}, Errors={error_count}")
    logger.info(f"{COLOR_BLUE}--- Data Fetch Cycle Finished in {cycle_duration:.2f} seconds ---{COLOR_RESET}")
    return data_fetched, state # Return if data was fetched and updated state


async def run_aggregation_tasks(now_utc: datetime):
    """Checks and runs necessary aggregation tasks."""
    logger.info(f"{COLOR_YELLOW}--- Starting Aggregation Tasks Check ---{COLOR_RESET}")
    aggregation_start_time = time.monotonic()
    db_conn: Optional[duckdb.DuckDBPyConnection] = None
    tasks_run_count = 0

    try:
        db_conn = get_db_connection(DB_FILE)
        if not db_conn:
            logger.error("Cannot run aggregations: DB connection failed.")
            return

        # Ensure aggregation tables exist (important!)
        schemas = define_schemas()
        if not initialize_database_schema(db_conn, schemas, API_ENDPOINTS):
            logger.error("Cannot run aggregations: Failed to ensure DB schema.")
            return

        for agg_type, period_delta in AGGREGATION_PERIODS.items():
            logger.debug(f"Checking aggregation type: '{agg_type}'")
            last_run_ts = get_last_aggregation_time(db_conn, agg_type)
            logger.debug(f" -> Last run: {last_run_ts}")

            # Determine the start time for the *next* aggregation period
            if last_run_ts:
                next_period_start = last_run_ts + period_delta
            else:
                # If never run, find the earliest timestamp in ANY raw table as a starting point?
                # Or just start from X days/weeks ago? Let's start from period_delta ago for simplicity.
                next_period_start = now_utc - period_delta
                logger.info(f"No previous run found for '{agg_type}', starting aggregation from {next_period_start}")

            # Check if the end of the next potential period has passed
            if next_period_start < now_utc:
                 # We need to run aggregation for the period ending *now* or *before now*
                 # For simplicity, let's aggregate up to the *start* of the current hour/day etc.
                 # to have consistent periods.
                 # Example: If period is 'daily', aggregate for the full day ending at the *last* midnight UTC.
                 # Example: If period is '6h', aggregate for the 6h block ending at the last 00:00, 06:00, 12:00, 18:00 UTC.

                 # Calculate the end of the *last completed* period before now_utc
                 # This is tricky. Let's try a simpler approach first:
                 # Run if now_utc is past the end of the *next* period based on last run.

                 period_end_candidate = next_period_start # The end of the period we want to aggregate
                 if now_utc >= period_end_candidate:
                    logger.info(f"Time to run aggregation '{agg_type}'. Period ending around: {period_end_candidate}")

                    # Define the actual period boundaries based on the candidate end time
                    period_end_utc = period_end_candidate
                    period_start_utc = period_end_utc - period_delta

                    success_for_all_endpoints = True
                    for endpoint_name, config in API_ENDPOINTS.items():
                        if config.get("aggregation_enabled") and config.get("raw_table") and config.get("price_json_path"):
                            raw_table = config["raw_table"]
                            target_table = f"{raw_table}_{agg_type}"
                            price_path = config["price_json_path"]

                            # Run the aggregation SQL
                            # NOTE: The SQL needs refinement to correctly extract symbols/prices from varied JSON
                            agg_success = run_aggregation(
                                conn=db_conn,
                                source_table=raw_table,
                                target_table=target_table,
                                price_json_path=price_path, # Pass the path
                                period_start_utc=period_start_utc,
                                period_end_utc=period_end_utc,
                                aggregation_type=agg_type
                            )
                            if not agg_success:
                                success_for_all_endpoints = False
                                # Log error, but continue trying other endpoints for this period

                    # If aggregation was attempted (even if partially failed), update the last run time
                    if success_for_all_endpoints:
                         update_last_aggregation_time(db_conn, agg_type, period_end_utc) # Use period end as the marker
                         tasks_run_count += 1
                    else:
                         logger.warning(f"Aggregation '{agg_type}' completed with errors for some endpoints. Last run time NOT updated.")


            else:
                logger.debug(f" -> Not time yet for '{agg_type}'. Next run after ~{next_period_start}")

    except Exception as e:
        logger.critical(f"Critical error during aggregation tasks: {e}", exc_info=True)
    finally:
        close_db_connection(db_conn, DB_FILE)

    aggregation_duration = time.monotonic() - aggregation_start_time
    logger.info(f"Aggregation check summary: Tasks Run = {tasks_run_count}")
    logger.info(f"{COLOR_YELLOW}--- Aggregation Tasks Check Finished in {aggregation_duration:.2f} seconds ---{COLOR_RESET}")


# --- Script Entry Point ---
if __name__ == "__main__":
    # Setup logging first
    setup_logging()
    logger.info(f"Script execution started at {datetime.now(pytz.timezone(TIMEZONE)).strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # Load .env file (for local development)
    try:
        from dotenv import load_dotenv
        script_dir = os.path.dirname(__file__)
        dotenv_path_parent = os.path.join(script_dir, '..', '.env') # Look in parent dir relative to src/
        if os.path.exists(dotenv_path_parent):
            load_dotenv(dotenv_path=dotenv_path_parent)
            logger.info(f"Loaded environment variables from: {dotenv_path_parent}")
    except ImportError:
        logger.debug("dotenv library not installed, skipping .env file load.")
    except Exception as e:
        logger.error(f"Error loading .env file: {e}")

    # Load previous state
    sync_state = load_state(STATE_FILE)

    # --- Run Main Async Logic ---
    exit_code = 0
    try:
        # Run the fetch cycle
        data_was_fetched, updated_state = asyncio.run(run_fetch_cycle(sync_state))

        # Run aggregation tasks (synchronously after fetching)
        # Pass the current time to aggregation tasks
        asyncio.run(run_aggregation_tasks(datetime.now(dt_timezone.utc)))

        # Save the updated state
        save_state(STATE_FILE, updated_state)

    except KeyboardInterrupt:
        logger.warning("Script interrupted by user.")
        exit_code = 1
    except Exception as e:
        logger.critical(f"Unhandled exception in main execution flow: {e}", exc_info=True)
        exit_code = 1
    finally:
        logger.info(f"Script execution finished at {datetime.now(pytz.timezone(TIMEZONE)).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logging.shutdown() # Ensure all logs are flushed

    # Exit with appropriate code (optional, useful for Actions)
    # sys.exit(exit_code)
