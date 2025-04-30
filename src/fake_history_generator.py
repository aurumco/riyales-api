#!/usr/bin/env python3
"""
Generate fake historical price records for all market JSON files in api/v1/market.
Each symbol will get a set of synthetic price points evenly spaced over the past 24h,
with small random deviations (±5%).
"""
import os
import json
import random
from datetime import datetime, timedelta
import pytz
import logging
import shutil  # for cleanup

# Configuration
INPUT_FOLDER = os.path.join('api', 'v1', 'market')
OUTPUT_FOLDER = os.path.join(INPUT_FOLDER, 'history_fake')
NUM_RECORDS_PER_SYMBOL = 10       # Number of fake points per symbol
MAX_DEVIATION_RATIO = 0.05        # ±5% price deviation from base price
TIMEZONE = pytz.utc               # Use UTC timestamps

# --- Logging Setup ---
logger = logging.getLogger('FakeHistoryGen')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('• %(message)s'))
logger.addHandler(handler)


def extract_items(data):
    """Return the list of items inside JSON (list or first list in dict)."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list):
                return value
    return []


def get_price_key(item):
    """Detect a likely price field in the item dict."""
    for key in ('price', 'price_toman', 'pc', 'pl'):
        if key in item:
            return key
    return None


def get_symbol_key(item):
    """Detect a likely symbol field in the item dict."""
    for key in ('symbol', 'name', 'l18'):
        if key in item:
            return key
    return None


def generate_fake_history(symbol, base_price):
    """Generate NUM_RECORDS_PER_SYMBOL fake price points over the last 24h."""
    now = datetime.now(TIMEZONE)
    interval = timedelta(hours=24) / NUM_RECORDS_PER_SYMBOL
    records = []
    for i in range(NUM_RECORDS_PER_SYMBOL):
        ts = now - interval * (NUM_RECORDS_PER_SYMBOL - i)
        deviation = random.uniform(-MAX_DEVIATION_RATIO, MAX_DEVIATION_RATIO)
        try:
            price = round(base_price * (1 + deviation), 2)
        except Exception:
            price = base_price
        records.append({'symbol': symbol, 'price': price, 'timestamp': ts.isoformat()})
    return records


def main():
    """Main entrypoint: read JSONs, generate fake history, write out raw_<endpoint>.json files."""
    # Cleanup previous fake history to remove old aggregated files
    if os.path.isdir(OUTPUT_FOLDER):
        shutil.rmtree(OUTPUT_FOLDER)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Define input directories: main market and stock subfolder
    input_dirs = [INPUT_FOLDER, os.path.join(INPUT_FOLDER, 'stock')]
    for dir_path in input_dirs:
        if not os.path.isdir(dir_path):
            continue
        for filename in os.listdir(dir_path):
            if not filename.endswith('.json'):
                continue
            input_path = os.path.join(dir_path, filename)

            try:
                with open(input_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load {filename}: {e}")
                continue

            items = extract_items(data)
            all_records = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                price_key = get_price_key(item)
                sym_key = get_symbol_key(item)
                if not price_key or not sym_key:
                    continue
                try:
                    base_price = float(item[price_key])
                except Exception:
                    continue
                symbol = item[sym_key]
                fake_records = generate_fake_history(symbol, base_price)
                all_records.extend(fake_records)

            if not all_records:
                logger.warning(f"No records generated for {filename}")
                continue

            endpoint = os.path.splitext(filename)[0]
            output_file = os.path.join(OUTPUT_FOLDER, f"raw_{endpoint}.json")
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(all_records, f, ensure_ascii=False, separators=(',',':'))
                logger.info(f"Generated {len(all_records)} fake records for '{endpoint}' → {output_file}")
            except Exception as e:
                logger.error(f"Failed to write fake history for {endpoint}: {e}")

    # --- Generate Fake Aggregates for Each Timeframe ---
    INTERVALS = {
        '4h': timedelta(hours=4),
        '12h': timedelta(hours=12),
        '24h': timedelta(days=1),
        '3d': timedelta(days=3),
        '7d': timedelta(weeks=1),
    }
    # Process only freshly generated raw_<endpoint>.json files
    for filename in sorted(os.listdir(OUTPUT_FOLDER)):
        if not filename.startswith('raw_') or not filename.endswith('.json'):
            continue
        endpoint = filename[4:-5]
        raw_path = os.path.join(OUTPUT_FOLDER, filename)
        try:
            with open(raw_path, 'r', encoding='utf-8') as rf:
                raw_records = json.load(rf)
        except Exception as e:
            logger.error(f"Failed to load fake raw for {endpoint}: {e}")
            continue
        # Build base price for each symbol
        base_prices = {}
        for rec in raw_records:
            sym = rec.get('symbol')
            if sym and sym not in base_prices:
                base_prices[sym] = rec.get('price')
        for interval_name, delta in INTERVALS.items():
            agg_data = {}
            for sym, base in base_prices.items():
                entries = []
                now = datetime.now(TIMEZONE)
                step = delta / NUM_RECORDS_PER_SYMBOL
                for i in range(NUM_RECORDS_PER_SYMBOL):
                    ts = now - delta + step * i
                    deviation = random.uniform(-MAX_DEVIATION_RATIO, MAX_DEVIATION_RATIO)
                    price = round(base * (1 + deviation), 3)
                    entries.append({'t': ts.isoformat(), 'm': price})
                agg_data[sym] = entries
            dir_path = os.path.join(OUTPUT_FOLDER, endpoint)
            os.makedirs(dir_path, exist_ok=True)
            out_file = os.path.join(dir_path, f"{interval_name}.json")
            try:
                with open(out_file, 'w', encoding='utf-8') as af:
                    json.dump(agg_data, af, ensure_ascii=False, separators=( ',', ':' ))
                logger.info(f"Generated fake aggregates for '{endpoint}/{interval_name}'")
            except Exception as e:
                logger.error(f"Failed to write fake aggregate for {endpoint}/{interval_name}: {e}")


if __name__ == '__main__':
    main() 