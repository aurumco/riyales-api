import os
import json
from datetime import datetime
import pytz
from statistics import median


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
            json.dump(all_records, f, ensure_ascii=False, indent=2)

    def compute_aggregates(self, endpoint: str, intervals: dict) -> None:
        """
        For each named interval (name->timedelta), compute median price per symbol over the last period,
        write results to an aggregate JSON file, and purge raw entries older than the max interval.
        """
        raw_path = self._raw_file(endpoint)
        if not os.path.isfile(raw_path):
            return
        try:
            with open(raw_path, 'r', encoding='utf-8') as f:
                data = json.load(f) or []
        except Exception:
            return
        # Parse timestamps and convert price to float for numeric median
        parsed = []
        for r in data:
            ts_str = r.get('timestamp')
            price_raw = r.get('price')
            try:
                # Parse timestamp
                ts = datetime.fromisoformat(ts_str)
                # If naive, assume UTC
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=pytz.utc)
                # Convert price to float
                price_val = float(price_raw)
                parsed.append({'symbol': r['symbol'], 'price': price_val, 'timestamp': ts})
            except Exception:
                # Skip records with invalid timestamp or price
                continue
        # Current time UTC (timezone-aware)
        now = datetime.now(pytz.utc)
        max_interval = max(intervals.values())
        for name, delta in intervals.items():
            cutoff = now - delta
            window = [rec for rec in parsed if rec['timestamp'] >= cutoff]
            # Group by symbol
            groups = {}
            for rec in window:
                groups.setdefault(rec['symbol'], []).append(rec['price'])
            agg_list = []
            for sym, prices in groups.items():
                try:
                    m = median(prices)
                except Exception:
                    continue
                agg_list.append({'symbol': sym, 'median': m, 'period_end': now.isoformat()})
            # Write aggregate JSON
            agg_path = self._agg_file(endpoint, name)
            with open(agg_path, 'w', encoding='utf-8') as f:
                json.dump(agg_list, f, ensure_ascii=False, indent=2)
        # Purge raw entries older than the max interval
        cutoff_old = now - max_interval
        new_raw = [rec for rec in parsed if rec['timestamp'] >= cutoff_old]
        out = [{'symbol': rec['symbol'], 'price': rec['price'], 'timestamp': rec['timestamp'].isoformat()} for rec in new_raw]
        with open(raw_path, 'w', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False, indent=2) 