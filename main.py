#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import logging
import asyncio
import aiohttp
import pytz
import time
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP # For median/aggregation
from datetime import datetime, time as dt_time, timedelta, date
import traceback
from typing import Dict, Any, Optional, List, Tuple, Union
from urllib.parse import urlparse, urlunparse
import random

# --- Configuration ---
# Secrets & Environment Variables
BASE_URL_ENV_VAR: str = "BRS_BASE_URL"    # Base URL for the API provider
API_KEY_ENV_VAR: str = "BRS_API_KEY"      # API Key for authentication
LOG_LEVEL_ENV_VAR: str = "LOG_LEVEL"      # Controls console log verbosity (INFO, DEBUG, WARNING, ERROR) - Set via environment variable

# General Settings
LOG_FOLDER: str = "logs"                  # Directory for log files
DATA_FOLDER: str = "api/v1/market"        # Standardized directory for output JSON and DB
AGGREGATE_JSON_FOLDER: str = os.path.join(DATA_FOLDER, "aggregates") # Subfolder for aggregate JSONs
ALL_MARKET_DATA_FILENAME: str = os.path.join(DATA_FOLDER, "all_market_data.json") # Filename for the consolidated JSON output in the new data folder
DICTIONARY_FOLDER: str = "dictionaries"   # Directory for mapping files
CRYPTO_NAME_MAPPING_FILE: str = os.path.join(DICTIONARY_FOLDER, "crypto_names_fa.json")
REQUEST_TIMEOUT_SECONDS: int = 15         # Timeout for individual API requests
PRETTY_PRINT_JSON: bool = False           # Save JSON compact (False) or pretty-printed (True) for latest data files
MAX_CONCURRENT_REQUESTS: int = 10         # Max simultaneous API requests
TIMEZONE: str = "Asia/Tehran"             # Default timezone for operations like market hours
DEFAULT_TIMEZONE = pytz.timezone(TIMEZONE)# Cached timezone object for performance
GENERAL_LOG_FILENAME: str = "app.log"     # Base filename for general logs
ERROR_LOG_FILENAME: str = "error.log"     # Base filename for error logs
RANDOMIZE_USER_AGENT: bool = False        # Toggle random User-Agent per request

# User-Agent rotator: list of modern browser signatures
USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.74 Mobile Safari/537.36"
]

BASE_HEADERS: Dict[str, str] = { "Accept": "application/json, text/plain, */*" }

# --- API Endpoint Configuration ---
# Defines the endpoints to fetch and how to process them.
API_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "gold": {
        "relative_url": "/Api/Market/Gold_Currency.php?key={api_key}", "output_filename": "gold.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["4h", "12h", "24h", "3d", "7d"],
        "price_json_path": "$.price", "symbol_json_path": "$.symbol",
        "array_base_paths": ["$.gold"],
        "transform_function": lambda data, mapping: {"gold": data.get("gold", [])} if data else {"gold": []}
    },
    "currency": {
        "relative_url": "/Api/Market/Gold_Currency.php?key={api_key}", "output_filename": "currency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["4h", "12h", "24h", "3d", "7d"],
        "price_json_path": "$.price", "symbol_json_path": "$.symbol",
        "array_base_paths": ["$.currency"],
        "transform_function": lambda data, mapping: {"currency": data.get("currency", [])} if data else {"currency": []}
    },
    "crypto": {
        "relative_url": "/Api/Market/Cryptocurrency.php?key={api_key}", "output_filename": "cryptocurrency.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["4h", "12h", "24h", "3d", "7d"],
        "price_json_path": "$.price_toman", "symbol_json_path": "$.name",
        "array_base_paths": ["$"],
        "transform_function": lambda data, mapping: add_persian_names_to_crypto(data, mapping)
    },
    "commodity": {
        "relative_url": "/Api/Market/Commodity.php?key={api_key}", "output_filename": "commodity.json",
        "fetch_interval_minutes": 10, "market_hours_apply": False, "enabled": True,
        "aggregation_levels": ["4h", "12h", "24h", "3d", "7d"],
        "price_json_path": "$.price", "symbol_json_path": "$.symbol",
        "array_base_paths": ["$.metal_precious"],
        "transform_function": lambda data, mapping: data # No transformation needed
    },
    "tse_ifb_symbols": {
        "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=1", "output_filename": "tse_ifb_symbols.json",
        "fetch_interval_minutes": 10, "market_hours_apply": True, "enabled": True,
        "aggregation_levels": ["24h", "7d"],
        "price_json_path": "$.pc", "symbol_json_path": "$.l18",
        "array_base_paths": ["$"],
        "transform_function": lambda data, mapping: data # No transformation needed
    },
     # Other endpoints remain the same, transformation can be added later if needed
     "tse_options": { "relative_url": "/Api/Tsetmc/Option.php?key={api_key}", "output_filename": "tse_options.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False, "transform_function": lambda data, mapping: data },
     "tse_nav": { "relative_url": "/Api/Tsetmc/Nav.php?key={api_key}", "output_filename": "tse_nav.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False, "transform_function": lambda data, mapping: data },
     "tse_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=1", "output_filename": "tse_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False, "transform_function": lambda data, mapping: data },
     "ifb_index": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=2", "output_filename": "ifb_index.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False, "transform_function": lambda data, mapping: data },
     "selected_indices": { "relative_url": "/Api/Tsetmc/Index.php?key={api_key}&type=3", "output_filename": "selected_indices.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": False, "transform_function": lambda data, mapping: data },
     "debt_securities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=4", "output_filename": "debt_securities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True, "aggregation_levels": ["4h","12h","24h","3d","7d"], "transform_function": lambda data, mapping: data },
     "housing_facilities": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=5", "output_filename": "housing_facilities.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True, "aggregation_levels": ["4h","12h","24h","3d","7d"], "transform_function": lambda data, mapping: data },
     "futures": { "relative_url": "/Api/Tsetmc/AllSymbols.php?key={api_key}&type=3", "output_filename": "futures.json", "fetch_interval_minutes": 20, "market_hours_apply": True, "enabled": True, "aggregation_levels": ["4h","12h","24h","3d","7d"], "transform_function": lambda data, mapping: data },
}

# Market Hours (Tehran Stock Exchange - Adjust if necessary)
TSE_MARKET_OPEN_TIME: dt_time = dt_time(8, 30)   # Market opening time (local Tehran time)
TSE_MARKET_CLOSE_TIME: dt_time = dt_time(12, 45) # Market closing time (local Tehran time)
TSE_MARKET_DAYS: List[int] = [5, 6, 0, 1, 2]     # Weekdays for TSE (0=Monday, 5=Saturday, 6=Sunday)

# Aggregation Intervals & Trigger Time
AGGREGATION_INTERVALS: Dict[str, timedelta] = {
    "4h": timedelta(hours=4), "12h": timedelta(hours=12), "24h": timedelta(days=1),
    "3d": timedelta(days=3), "7d": timedelta(weeks=1),
}
DAILY_AGGREGATION_TIME_LOCAL: dt_time = dt_time(0, 5) # Time (local) to run daily aggregation for the previous day

# --- Constants ---
COLOR_GREEN: str = "\033[32m"
COLOR_RED: str = "\033[31m"
COLOR_YELLOW: str = "\033[33m"
COLOR_BLUE: str = "\033[34m"
COLOR_GRAY: str = "\033[90m"
COLOR_RESET: str = "\033[0m"

# --- Global Logger ---
logger = logging.getLogger("MarketDataSync")

# --- Global Crypto Name Mapping ---
CRYPTO_NAME_MAP: Dict[str, str] = {}

# Directory for stock-related market data JSON files
STOCK_FOLDER_NAME: str = "stock"
STOCK_DATA_FOLDER: str = os.path.join(DATA_FOLDER, STOCK_FOLDER_NAME)

# Blacklist configuration
BLACKLIST_FILE: str = os.path.join(DICTIONARY_FOLDER, "blacklist.json")

def load_crypto_name_map(filepath: str) -> Dict[str, str]:
    """Loads the crypto name mapping from a JSON file."""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
                if isinstance(mapping, dict):
                    logger.debug(f"• Successfully loaded {len(mapping)} crypto name mappings from {filepath}")
                    return mapping
                else:
                    logger.debug(f"• Error loading crypto map: {filepath} content is not a JSON object.")
                    return {}
        else:
            logger.debug(f"• Crypto name mapping file not found: {filepath}. Persian names will not be added.")
            return {}
    except (json.JSONDecodeError, IOError) as e:
        logger.debug(f"• Error loading crypto name map from {filepath}: {e}", exc_info=True)
        return {}

def add_persian_names_to_crypto(data: Any, mapping: Dict[str, str]) -> Any:
    """Adds 'nameFa' field to crypto data based on the mapping."""
    if not isinstance(data, list) or not mapping:
        logger.debug("Skipping Persian name mapping: Invalid data format or empty map.")
        return data # Return original data if not a list or mapping is empty

    missing_names_count = 0
    for item in data:
        if isinstance(item, dict):
            english_name = item.get('name')
            if english_name:
                persian_name = mapping.get(english_name)
                if persian_name:
                    item['nameFa'] = persian_name
                else:
                    missing_names_count += 1
                    logger.debug(f"• No Persian name found for: {english_name}")
            else:
                 logger.debug("Skipping item: Missing 'name' key.")
        else:
             logger.debug(f"• Skipping item: Expected dict, got {type(item)}.")

    if missing_names_count > 0:
        logger.debug(f"• Persian name mapping: {missing_names_count} crypto names were not found in the map.")

    return data

def load_blacklist(filepath: str) -> List[str]:
    """Loads blacklist entries (names or symbols) from a JSON file."""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    logger.debug(f"• Loaded {len(data)} blacklist entries from {filepath}")
                    return [str(item) for item in data]
                else:
                    logger.debug(f"• Blacklist file {filepath} content is not a list.")
        else:
            logger.debug(f"• Blacklist file not found: {filepath}. No entries will be filtered.")
    except Exception as e:
        logger.debug(f"• Error loading blacklist from {filepath}: {e}", exc_info=True)
    return []

def filter_blacklist(data: Any, blacklist: List[str]) -> Any:
    """Recursively filters out items whose name/symbol/l18/l30 matches any blacklist entry."""
    if not blacklist:
        return data
    def is_allowed(item: Any) -> bool:
        if not isinstance(item, dict):
            return True
        for key in ('name', 'symbol', 'l18', 'l30', 'cs', 'nameFa', 'symbolFa', 'nameEn', 'symbolEn', 'name_en'):
            val = item.get(key)
            if isinstance(val, str) and val in blacklist:
                return False
        return True
    if isinstance(data, list):
        return [itm for itm in data if is_allowed(itm)]
    if isinstance(data, dict):
        filtered = {}
        for k, v in data.items():
            if isinstance(v, list):
                filtered[k] = [itm for itm in v if is_allowed(itm)]
            else:
                filtered[k] = v
        return filtered
    return data

# --- Consolidated JSON Output ---
def create_consolidated_json() -> bool:
    """Combines individual JSON files from DATA_FOLDER into a single file."""
    logger.debug(f"• {COLOR_BLUE}• Creating Consolidated JSON Output{COLOR_RESET}")
    consolidated_data: Dict[str, Any] = {}
    target_filepath = ALL_MARKET_DATA_FILENAME
    files_processed = 0
    errors_encountered = 0

    try:
        # Ensure data folder exists (though it should by now)
        os.makedirs(DATA_FOLDER, exist_ok=True)

        # Find JSON files directly in DATA_FOLDER, excluding the consolidated file itself
        # and files from the aggregates subfolder
        for filename in os.listdir(DATA_FOLDER):
            if filename.endswith('.json') and filename != os.path.basename(ALL_MARKET_DATA_FILENAME) and os.path.isfile(os.path.join(DATA_FOLDER, filename)):
                filepath = os.path.join(DATA_FOLDER, filename)
                # Extract endpoint name from filename (e.g., 'gold.json' -> 'gold')
                endpoint_name = filename.rsplit('.', 1)[0]
                logger.debug(f"• Reading {filename} for consolidation...")
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        consolidated_data[endpoint_name] = data
                        files_processed += 1
                except (json.JSONDecodeError, IOError) as e:
                    logger.debug(f"• Error reading or parsing {filename}: {e}")
                    errors_encountered += 1
                except Exception as e:
                    logger.debug(f"• Unexpected error processing {filename}: {e}", exc_info=True)
                    errors_encountered += 1

        if not consolidated_data:
            logger.warning("No valid individual JSON files found to consolidate.")
            # Optionally write an empty file if desired
            # with open(target_filepath, 'w', encoding='utf-8') as f:
            #     json.dump({}, f)
            return True # Not an error if no source files

        # Write the consolidated data
        with open(target_filepath, 'w', encoding='utf-8') as f:
            json.dump(consolidated_data, f, ensure_ascii=False,
                      indent=4 if PRETTY_PRINT_JSON else None,
                      separators=(',', ':') if not PRETTY_PRINT_JSON else None)

        if errors_encountered > 0:
            logger.debug(f"• Consolidated JSON created at {target_filepath}, but encountered {errors_encountered} errors reading source files.")
        else:
            logger.debug(f"• {COLOR_GREEN}✓ Consolidated JSON successfully created at: {target_filepath} ({files_processed} files combined){COLOR_RESET}")
        return True

    except Exception as e:
        logger.debug(f"• Failed to create consolidated JSON file: {e}", exc_info=True)
        return False

# --- Utility ---
def mask_string(s: Optional[str]) -> str:
    """Masks potentially sensitive strings like API keys and base URLs in logs."""
    if s is None: return "None"
    s = str(s)

    # Mask common key/token patterns using regex (case-insensitive)
    s = re.sub(r"key=([^&?\s]+)", "key=********", s, flags=re.IGNORECASE)
    s = re.sub(r"token=([^&?\s]+)", "token=********", s, flags=re.IGNORECASE)

    # Mask potential Authorization headers carefully (show type, hide value)
    s = re.sub(r"(Authorization\s*:\s*)(\w+\s+)\S+", r"\1\2********", s, flags=re.IGNORECASE)

    # Mask BRS_BASE_URL if present
    base_url_to_mask = os.getenv(BASE_URL_ENV_VAR)
    if base_url_to_mask:
        # Escape potential regex characters in the URL
        escaped_base_url = re.escape(base_url_to_mask)
        # Use regex to replace the base URL, handling http/https variations
        s = re.sub(rf"https?://{escaped_base_url.split('://')[-1]}", "https://********", s, flags=re.IGNORECASE)

    return s

# --- Log Entry Cleanup Utility ---
def cleanup_log_entries(file_path: str, retention_hours: int) -> None:
    """Removes log entries older than retention_hours hours from the specified log file."""
    try:
        if not os.path.exists(file_path):
            return
        threshold = datetime.now() - timedelta(hours=retention_hours)
        lines_to_keep: List[str] = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                ts_str = line.split('|', 1)[0]
                try:
                    ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                    if ts >= threshold:
                        lines_to_keep.append(line)
                except Exception:
                    # Keep lines we cannot parse
                    lines_to_keep.append(line)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines_to_keep)
    except Exception as e:
        logger.debug(f"• Error cleaning up log entries for {file_path}: {e}", exc_info=True)

# --- Logging Setup ---
def setup_logging() -> None:
    """Configures logging with daily rotation, console output, and custom cleanup."""
    log_level_str = os.getenv(LOG_LEVEL_ENV_VAR, 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    os.makedirs(LOG_FOLDER, exist_ok=True)
    # Use fixed log filenames for app and error logs
    general_log_path = os.path.join(LOG_FOLDER, GENERAL_LOG_FILENAME)
    error_log_path = os.path.join(LOG_FOLDER, ERROR_LOG_FILENAME)
    class SecureColorFormatter(logging.Formatter):
        # Define colors for different log levels
        level_colors = {
            logging.DEBUG: COLOR_GRAY,     # DEBUG in gray
            logging.INFO: COLOR_BLUE,      # INFO in blue
            logging.WARNING: COLOR_YELLOW, # WARNING in yellow
            logging.ERROR: COLOR_RED,      # ERROR in red
            logging.CRITICAL: COLOR_RED,   # CRITICAL in red
        }
        reset_color = COLOR_RESET

        def format(self, record):
            # Mask sensitive info before formatting
            original_args = record.args
            if isinstance(original_args, tuple):
                record.args = tuple(mask_string(arg) for arg in original_args)
            elif isinstance(original_args, dict):
                pass

            # Mask the message content
            record.msg = mask_string(record.getMessage())
            record.args = ()

            # Generate the base formatted log string
            formatted = super().format(record)
            # Restore original args
            record.args = original_args

            # Determine the color for this level
            level = record.levelname
            color = self.level_colors.get(record.levelno, '')

            if record.levelno == logging.INFO:
                # Highlight only the level name
                colored_level = f"{color}{level}{self.reset_color}"
                return formatted.replace(f"|{level}|", f"|{colored_level}|", 1)

            # For DEBUG, WARNING, ERROR, CRITICAL: color the entire line
            return f"{color}{formatted}{self.reset_color}"

    # --- Global Logger Configuration ---
    logger.setLevel(logging.DEBUG)

    # Format strings
    format_str = '%(asctime)s|%(levelname)s|%(name)s|%(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'

    # Create separate formatters
    color_formatter = SecureColorFormatter(fmt=format_str, datefmt=datefmt)
    plain_formatter = logging.Formatter(fmt=format_str, datefmt=datefmt)

    # Convert logging timestamps to Asia/Tehran timezone
    def tehran_time_converter(ts):
        return datetime.fromtimestamp(ts, DEFAULT_TIMEZONE).timetuple()
    color_formatter.converter = tehran_time_converter
    plain_formatter.converter = tehran_time_converter

    # --- Console Handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(color_formatter)
    logger.addHandler(console_handler)

    # --- General Log File Handler ---
    general_file_handler = logging.FileHandler(general_log_path, encoding='utf-8')
    general_file_handler.setLevel(logging.DEBUG)
    general_file_handler.setFormatter(plain_formatter)
    logger.addHandler(general_file_handler)

    # --- Error Log File Handler ---
    error_file_handler = logging.FileHandler(error_log_path, encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(plain_formatter)
    logger.addHandler(error_file_handler)

    # Cleanup old log entries based on retention
    cleanup_log_entries(general_log_path, 2)  # Retain last 2 hours in app.log
    cleanup_log_entries(error_log_path, 48)  # Retain last 48 hours in error.log

    logger.debug(f"• Logging initialized. Console level: {log_level_str}. General logs: '{general_log_path}'. Error logs: '{error_log_path}'.")

# --- Timezone & Market Check ---
def is_market_open(tz: pytz.BaseTzInfo, open_time: dt_time, close_time: dt_time, market_days: List[int]) -> bool:
    """Checks if the current time is within specified market hours and days in the given timezone."""
    try:
        now_local = datetime.now(tz)
        current_time = now_local.time()
        current_weekday = now_local.weekday() # Monday is 0, Sunday is 6

        logger.debug(f"• Market Check: LocalTime={now_local.strftime('%H:%M:%S %Z')}, Weekday={current_weekday}, CurrentTime={current_time}, MarketHours={open_time}-{close_time}, MarketDays={market_days}")

        if current_weekday not in market_days:
            logger.debug("Market Status: CLOSED (Outside Market Days)")
            return False

        is_open = open_time <= current_time < close_time
        logger.debug(f"• Market Status: {'OPEN' if is_open else 'CLOSED'} (Within Market Hours Check)")
        return is_open
    except Exception as e:
        logger.debug(f"• Market hours check failed: {e}", exc_info=True)
        return False # Fail safe: assume market is closed if check fails

# --- API Fetching ---
async def fetch_api_data(
    session: aiohttp.ClientSession, endpoint_name: str, config: Dict[str, Any],
    base_url: str, api_key: str
) -> Optional[Tuple[str, Dict[str, Any], Any]]:
    """Fetches data from a single API endpoint asynchronously."""
    relative_url = config['relative_url']
    full_url = f"{base_url.rstrip('/')}{relative_url.format(api_key=api_key)}"
    request_start_time = time.monotonic()

    # Mask URL for logging
    masked_log_url = mask_string(full_url) # Use existing mask function first
    try:
        parsed_base = urlparse(base_url)
        parsed_full = urlparse(full_url)
        # Replace the netloc (domain) part in the full URL for logging
        # Keeps scheme, path, query etc.
        if parsed_base.netloc and parsed_base.netloc in parsed_full.netloc:
            # Replace domain and re-mask query parameters
            masked_log_url = urlunparse(parsed_full._replace(netloc="********"))
            masked_log_url = mask_string(masked_log_url)
    except Exception:
        # Fallback if URL parsing fails for some reason
        logger.warning("Failed to parse URL for detailed masking, using basic masking.")

    logger.debug(f"• Requesting: {endpoint_name}")

    try:
        # Build headers with dynamic User-Agent
        ua = random.choice(USER_AGENTS) if RANDOMIZE_USER_AGENT else USER_AGENTS[0]
        req_headers = BASE_HEADERS.copy()
        req_headers["User-Agent"] = ua
        async with session.get(full_url, headers=req_headers, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            elapsed_time = time.monotonic() - request_start_time
            log_url_for_status = mask_string(str(response.url)) # Mask URL from response object too
            logger.debug(f"• Response: {endpoint_name} Status={response.status} in {elapsed_time:.2f}s")

            if response.status == 200:
                try:
                    # Prefer built-in JSON parsing
                    data = await response.json()
                    return endpoint_name, config, data
                except aiohttp.ContentTypeError:
                    # Content-Type header may be wrong; try manual JSON parsing
                    try:
                        text_content = await response.text()
                    except Exception as text_err:
                        logger.debug(f"• Error reading text for {endpoint_name}: {text_err}", exc_info=True)
                        return None
                    try:
                        data = json.loads(text_content)
                        return endpoint_name, config, data
                    except json.JSONDecodeError as e:
                        logger.debug(f"• Error: Manual JSON parsing failed for {endpoint_name}: {e}")
                        logger.debug(f"• Raw response snippet for {endpoint_name}: {text_content[:200]}...")
                        return None
                except json.JSONDecodeError as e:
                    logger.debug(f"• Error: Failed to decode JSON for {endpoint_name}: {e}")
                    return None
            else:
                error_text = await response.text()
                logger.debug(f"• Error: {endpoint_name} request failed. Status={response.status}")
                # Optionally log response snippet without URL
                logger.debug(f"• Response snippet for {endpoint_name}: {mask_string(error_text[:500])}...")
                return None

    except asyncio.TimeoutError:
        logger.debug(f"• Error: Timeout fetching {endpoint_name} after {REQUEST_TIMEOUT_SECONDS}s")
        return None
    except aiohttp.ClientError as e:
        logger.debug(f"• Error: Client error fetching {endpoint_name}: {e}", exc_info=False) # Keep exc_info False for common client errors unless debugging needed
        return None
    except Exception as e:
        logger.debug(f"• Error: Unexpected error fetching {endpoint_name}: {e}", exc_info=True)
        return None

# --- Main Execution ---
async def main():
    """Main asynchronous function orchestrating the fetch and aggregation process."""
    setup_logging()

    # Log a clear start banner
    logger.info(f"{COLOR_GREEN}• Market Data Sync START {COLOR_RESET}")

    script_start_time = time.monotonic()
    logger.debug(f"{COLOR_GREEN}• Starting Market Data Sync Cycle {COLOR_RESET}")

    # Load configuration from environment variables
    api_key = os.getenv(API_KEY_ENV_VAR)
    base_url = os.getenv(BASE_URL_ENV_VAR)
    if not api_key or not base_url:
        logger.debug(f"• CRITICAL: Required environment variables '{API_KEY_ENV_VAR}' or '{BASE_URL_ENV_VAR}' are not set. Exiting.")
        return

    # Load the crypto name mapping early
    CRYPTO_NAME_MAP = load_crypto_name_map(CRYPTO_NAME_MAPPING_FILE)
    # Load blacklist entries early
    blacklist = load_blacklist(BLACKLIST_FILE)

    # Get current time in UTC and local timezone
    now_utc = datetime.now(pytz.utc)
    now_local = now_utc.astimezone(DEFAULT_TIMEZONE)
    logger.debug(f"• Cycle time: UTC={now_utc.isoformat()}, Local={now_local.isoformat()}")

    # --- 1. Determine and Fetch Raw Data ---
    apis_to_fetch_this_run: List[Tuple[str, Dict[str, Any]]] = []
    logger.info("• Checking API Fetch Tasks ")
    for name, config in API_ENDPOINTS.items():
        logger.debug(f"• Checking fetch task: '{name}'")
        # Check if globally enabled
        if not config.get('enabled', False):
            logger.debug(f"•  // Skip '{name}': Disabled in config.")
            continue
        # Check market hours if applicable
        if config.get('market_hours_apply', False) and not is_market_open(DEFAULT_TIMEZONE, TSE_MARKET_OPEN_TIME, TSE_MARKET_CLOSE_TIME, TSE_MARKET_DAYS):
            logger.debug(f"•  // Skip '{name}': Market is closed.")
            continue
        # Check fetch interval based on last run time
        fetch_interval = timedelta(minutes=config.get('fetch_interval_minutes', 10))

        # If all checks pass, schedule for fetching
        logger.debug(f"• Scheduling fetch for: '{name}'")
        apis_to_fetch_this_run.append((name, config))

    # Execute API fetches concurrently if any are scheduled
    fetch_results = []
    if apis_to_fetch_this_run:
        logger.debug(f"• Attempting to fetch data for {len(apis_to_fetch_this_run)} endpoints...")
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
        logger.info("• No API endpoints scheduled for fetching in this cycle.")

    # --- Process Fetch Results ---
    processed_fetches = 0
    successful_raw_inserts = 0
    fetch_errors = 0
    if fetch_results:
        insert_time_utc = datetime.now(pytz.utc) # Use a consistent timestamp for all inserts in this batch
        logger.info("• Processing Fetch Results ")
        for i, result in enumerate(fetch_results):
            processed_fetches += 1
            # Get corresponding config using index, assuming order is preserved by gather
            endpoint_name, config = apis_to_fetch_this_run[i]
            fetch_task_name = f"fetch_{endpoint_name}"

            fetch_successful = False # Flag to track if this specific fetch led to data processing
            if isinstance(result, Exception):
                logger.debug(f"• Fetch Task Error (Caught by Gather): '{endpoint_name}'. Exception: {result}", exc_info=False)
                fetch_errors += 1
            elif result is not None:
                # Successful fetch and decode
                _name, _config, data = result

                # Apply transform function if one exists (pass the crypto map)
                if "transform_function" in config and callable(config["transform_function"]):
                    # Pass the globally loaded map
                    data = config["transform_function"](data, CRYPTO_NAME_MAP)
                    logger.debug(f"• Applied transformation function for {endpoint_name}")
                    # Filter out any blacklisted items
                    data = filter_blacklist(data, blacklist)
                    logger.debug(f"• Applied blacklist filter for {endpoint_name}")

                # Determine destination folder: stock endpoints go to STOCK_DATA_FOLDER, others to DATA_FOLDER
                if config['relative_url'].startswith("/Api/Tsetmc"):
                    dest_folder = STOCK_DATA_FOLDER
                else:
                    dest_folder = DATA_FOLDER
                output_filename = os.path.join(dest_folder, config['output_filename'])
                try:
                    os.makedirs(dest_folder, exist_ok=True)
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False,
                                  indent=4 if PRETTY_PRINT_JSON else None,
                                  separators=((',', ':') if not PRETTY_PRINT_JSON else None))
                    logger.debug(f"• Saved latest raw JSON: {os.path.basename(output_filename)}")
                except IOError as e:
                    logger.debug(f"• Failed to write latest JSON file {os.path.basename(output_filename)}: {e}", exc_info=False)

                # Append new records to history
                try:
                    def extract_records(endpoint, raw_data):
                        """
                        Recursively finds all record dicts containing symbol and price fields in the raw_data.
                        """
                        now_iso = datetime.now(pytz.utc).isoformat()
                        recs = []
                        def gather(obj):
                            found = []
                            # List of potential symbol keys
                            sym_keys = ['symbol', 'name', 'l18']
                            # List of potential price keys
                            price_keys = ['price', 'price_toman', 'pc', 'pl']
                            if isinstance(obj, list):
                                for elem in obj:
                                    if isinstance(elem, dict):
                                        has_sym = any(k in elem for k in sym_keys)
                                        has_price = any(k in elem for k in price_keys)
                                        if has_sym and has_price:
                                            found.append(elem)
                                        else:
                                            # Recurse into nested structures
                                            for v in elem.values():
                                                if isinstance(v, (dict, list)):
                                                    found.extend(gather(v))
                                    elif isinstance(elem, (list, dict)):
                                        found.extend(gather(elem))
                            elif isinstance(obj, dict):
                                for v in obj.values():
                                    if isinstance(v, (dict, list)):
                                        found.extend(gather(v))
                            return found
                        items = gather(raw_data)
                        for item in items:
                            symbol = item.get('symbol') or item.get('name') or item.get('l18')
                            price = item.get('price') or item.get('price_toman') or item.get('pc') or item.get('pl')
                            # Determine record timestamp
                            ts = None
                            if 'time_unix' in item:
                                try:
                                    ts = datetime.fromtimestamp(item['time_unix'], tz=pytz.utc).isoformat()
                                except Exception:
                                    ts = now_iso
                            if not ts:
                                ts = now_iso
                            recs.append({'symbol': symbol, 'price': price, 'timestamp': ts})
                        return recs
                    records = extract_records(endpoint_name, data)
                except Exception as hist_err:
                    logger.debug(f"• Error extracting records for {endpoint_name}: {hist_err}", exc_info=True)
            else:
                # Fetch failed (non-200, timeout, decode error, etc.) - Error logged in fetch_api_data
                fetch_errors += 1

            # DB insertion removed; mark as successful
            successful_raw_inserts += 1
            fetch_successful = True # Mark as successful data processed

        logger.debug(f"• Fetch Results Summary:  Processed={processed_fetches},  Errors={fetch_errors}")

    # --- 1.5 Create Consolidated JSON --- #
    # Runs after fetches are processed, regardless of fetch errors, but only if fetches were attempted
    if apis_to_fetch_this_run:
        if successful_raw_inserts > 0 or fetch_errors == 0: # Only run if something was likely updated or no errors occurred
            create_consolidated_json()
        else:
            logger.info("Skipping consolidated JSON creation due to fetch errors and no successful DB inserts.")
    else:
        logger.info("Skipping consolidated JSON creation as no fetches were scheduled.")

    # --- Finish ---
    script_end_time = time.monotonic()
    total_duration = script_end_time - script_start_time
    # Log a clear end banner with duration
    logger.info(f"{COLOR_GREEN}• Market Data Sync END {COLOR_RESET}|{COLOR_GRAY} Duration: {total_duration:.2f}s{COLOR_RESET}")

# --- Script Entry Point ---
if __name__ == "__main__":
    # Load the crypto name mapping early
    CRYPTO_NAME_MAP = load_crypto_name_map(CRYPTO_NAME_MAPPING_FILE)

    # Run the main asynchronous function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Use print as logger might be shutdown
        print(f"\n{COLOR_YELLOW}• Script interrupted by user.{COLOR_RESET}")
    except Exception as e:
        # Catch any unhandled exceptions right at the top level
        # Use logger if available, otherwise print critical error message
        if logger.handlers:
            logger.debug(f"• Unhandled critical exception in main execution: {e}", exc_info=True)
        else:
            print(f"{COLOR_RED}• CRITICAL UNHANDLED EXCEPTION{COLOR_RESET}")
            print(f"{COLOR_RED}✗ Error: {e}{COLOR_RESET}")
            traceback.print_exc()
            print(f"{COLOR_RED}• END CRITICAL EXCEPTION {COLOR_RESET}")