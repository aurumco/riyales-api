import os
import json
from datetime import datetime
import pytz
from statistics import median

# Toggle to output compressed (minified) JSON for history files
COMPRESS_HISTORY_JSON: bool = True

# Maximum number of median entries to keep per symbol per timeframe
MAX_AGG_SERIES_ENTRIES: int = 10

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

class HistoryManager:
    """
    Manages JSON-based historical price storage and sliding-window aggregation.
    """
    def __init__(self, base_folder: str):
        self.base_folder = base_folder
        ensure_dir(self.base_folder)

    def _raw_file(self, endpoint: str) -> str:
        return os.path.join(self.base_folder, f"raw_{endpoint}.json")

    def _agg_file(self, endpoint: str, interval: str) -> str:
        # Store each endpoint's aggregates in its own subfolder, file named by interval
        folder = os.path.join(self.base_folder, endpoint)
        ensure_dir(folder)
        return os.path.join(folder, f"{interval}.json")

    def append_raw(self, endpoint: str, records: list) -> None:
        """
        Append new raw price records for an endpoint.
        Each record must include: symbol, price, timestamp (ISO format string).
        """
        path = self._raw_file(endpoint)
        all_records = []
        if os.path.isfile(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    all_records = json.load(f) or []
            except Exception:
                all_records = []
        all_records.extend(records)
        with open(path, 'w', encoding='utf-8') as f:
            # Write raw history JSON, compressed or pretty
            if COMPRESS_HISTORY_JSON:
                json.dump(all_records, f, ensure_ascii=False, separators=(',', ':'), indent=None)
            else:
                json.dump(all_records, f, ensure_ascii=False, indent=2)

    def compute_aggregates(self, endpoint: str, intervals: dict) -> None:
        """
        For each named interval (name->timedelta), compute median price per symbol over the last period,
        update per-symbol time series (max entries), and purge raw entries older than the max interval.
        """
        raw_path = self._raw_file(endpoint)
        if not os.path.isfile(raw_path):
            return
        try:
            with open(raw_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f) or []
        except Exception:
            return
        # Parse raw_data into records with datetime and float price
        parsed = []
        for rec in raw_data:
            ts_str = rec.get('timestamp')
            price_raw = rec.get('price')
            try:
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=pytz.utc)
                price_val = float(price_raw)
                parsed.append({'symbol': rec['symbol'], 'price': price_val, 'timestamp': ts})
            except Exception:
                continue
        now = datetime.now(pytz.utc)
        max_interval = max(intervals.values())
        # Compute and persist per-interval, per-symbol time series
        for name, delta in intervals.items():
            cutoff = now - delta
            window = [r for r in parsed if r['timestamp'] >= cutoff]
            # Calculate current medians
            groups = {}
            for r in window:
                groups.setdefault(r['symbol'], []).append(r['price'])
            current_medians = []
            for sym, prices in groups.items():
                try:
                    m = median(prices)
                except Exception:
                    continue
                # Use short keys: 's' for symbol, 'm' for median, 't' for timestamp
                current_medians.append({'s': sym, 'm': m, 't': now.isoformat()})
            # Load existing series from file
            agg_path = self._agg_file(endpoint, name)
            series = {}
            if os.path.isfile(agg_path):
                try:
                    with open(agg_path, 'r', encoding='utf-8') as sf:
                        series = json.load(sf) or {}
                except Exception:
                    series = {}
            # Append and trim per-symbol lists
            for rec in current_medians:
                # Unpack using short keys
                sym = rec['s']
                entry = {'t': rec['t'], 'm': rec['m']}
                lst = series.get(sym, [])
                # Only append if no previous entry or last entry is older than the interval
                if lst:
                    last_ts = datetime.fromisoformat(lst[-1]['t'])
                    if last_ts.tzinfo is None:
                        last_ts = last_ts.replace(tzinfo=pytz.utc)
                    if (now - last_ts) < delta:
                        continue
                # Append and trim to max entries
                lst.append(entry)
                if len(lst) > MAX_AGG_SERIES_ENTRIES:
                    lst = lst[-MAX_AGG_SERIES_ENTRIES:]
                series[sym] = lst
            # Save updated time series
            with open(agg_path, 'w', encoding='utf-8') as sf:
                if COMPRESS_HISTORY_JSON:
                    json.dump(series, sf, ensure_ascii=False, separators=(',', ':'), indent=None)
                else:
                    json.dump(series, sf, ensure_ascii=False, indent=2)
        # Purge raw_data older than longest interval
        cutoff_old = now - max_interval
        pruned = [r for r in parsed if r['timestamp'] >= cutoff_old]
        out = [{'symbol': r['symbol'], 'price': r['price'], 'timestamp': r['timestamp'].isoformat()} for r in pruned]
        with open(self._raw_file(endpoint), 'w', encoding='utf-8') as f:
            if COMPRESS_HISTORY_JSON:
                json.dump(out, f, ensure_ascii=False, separators=(',', ':'), indent=None)
            else:
                json.dump(out, f, ensure_ascii=False, indent=2) 