"""protobuf_generator.py

Generates binary Protocol Buffer (.pb) files from the freshly built JSON files that
live under api/v1/market/.  The generated files are stored under api/v2/market/
mirroring the same sub-directory structure and filenames (with the extension
changed to .pb).

The module compiles `protos/market_data.proto` to Python on-the-fly if the
corresponding *_pb2.py file is missing or stale.  It then converts each JSON
payload into the appropriate protobuf message and serialises it to disk.

The public API is the single async function `generate_all_protobuf_files()`
called from `main.py` right after the JSON files are saved.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

# Third-party
from google.protobuf.message import Message  # type: ignore
from google.protobuf import json_format  # type: ignore

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent  # project root (riyales-api)
PROTO_DIR = REPO_ROOT / "protos"
PROTO_FILE = PROTO_DIR / "market_data.proto"
GENERATED_DIR = REPO_ROOT / "src" / "pb_generated"
GENERATED_PY = GENERATED_DIR / "market_data_pb2.py"

V1_DIR = REPO_ROOT / "api" / "v1" / "market"
V2_DIR = REPO_ROOT / "api" / "v2" / "market"

# ---------------------------------------------------------------------------
# Proto compilation helper
# ---------------------------------------------------------------------------

def _ensure_proto_compiled() -> None:
    """Compile the .proto file to Python if it is missing or out-of-date."""
    # Ensure generated dir is importable regardless of compilation
    if str(GENERATED_DIR) not in sys.path:
        sys.path.insert(0, str(GENERATED_DIR))

    if GENERATED_PY.exists():
        proto_mtime = PROTO_FILE.stat().st_mtime
        py_mtime = GENERATED_PY.stat().st_mtime
        if py_mtime >= proto_mtime:
            # Up-to-date – nothing to do
            return

    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    # Use grpc_tools.protoc if available, fallback to system protoc
    try:
        import grpc_tools.protoc as protoc  # type: ignore

        cmd = [
            "protoc",  # dummy – grpc_tools expects argv[0] = program name
            f"-I{PROTO_DIR}",
            f"--python_out={GENERATED_DIR}",
            str(PROTO_FILE),
        ]
        if protoc.main(cmd) != 0:
            raise RuntimeError("protoc compilation via grpc_tools failed")
    except (ImportError, RuntimeError):
        # Fallback: call system protoc
        cmd = [
            "protoc",
            f"-I{PROTO_DIR}",
            f"--python_out={GENERATED_DIR}",
            str(PROTO_FILE),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"protoc compilation failed: {result.stderr or result.stdout}"
            )


# ---------------------------------------------------------------------------
# JSON → protobuf helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def _safe_int(val: Any) -> int:
    try:
        return int(val)
    except Exception:
        return 0


# Mapping configuration: dataset key → (message_factory, item_builder)
HANDLERS: Dict[str, Any] = {}


def _register_handlers(pb_module):  # noqa: N802 – function uses dynamic names
    """Populate the HANDLERS dict once we have the generated protobuf module."""

    def commodity_builder(json_obj: dict, pb_obj: Any):
        for attr in (
            "date",
            "time",
            "symbol",
            "name",
            "unit",
        ):
            if attr in json_obj:
                setattr(pb_obj, attr, str(json_obj[attr]))
        if "price" in json_obj:
            pb_obj.price = _safe_float(json_obj["price"])
        if "change_percent" in json_obj:
            pb_obj.change_percent = _safe_float(json_obj["change_percent"])
        # Localised names
        pb_obj.name_fa = str(json_obj.get("name_fa") or json_obj.get("nameFa", ""))
        pb_obj.name_en = str(json_obj.get("name_en") or json_obj.get("nameEn", ""))

    def crypto_builder(json_obj: dict, pb_obj: Any):
        simple_copy = [
            "date",
            "time",
            "name",
            "price",
            "price_toman",
            "link_icon",
        ]
        for attr in simple_copy:
            if attr in json_obj:
                setattr(pb_obj, attr, str(json_obj[attr]))

        # Localised Persian name (handle both snake_case and camelCase)
        if "name_fa" in json_obj or "nameFa" in json_obj:
            pb_obj.name_fa = str(json_obj.get("name_fa") or json_obj.get("nameFa", ""))

        pb_obj.time_unix = _safe_int(json_obj.get("time_unix"))
        pb_obj.change_percent = _safe_float(json_obj.get("change_percent"))
        pb_obj.market_cap = _safe_int(json_obj.get("market_cap"))

    def currency_builder(json_obj: dict, pb_obj: Any):
        simple_copy = [
            "date",
            "time",
            "symbol",
            "name_en",
            "name",
            "unit",
        ]
        for attr in simple_copy:
            if attr in json_obj:
                setattr(pb_obj, attr, str(json_obj[attr]))
        pb_obj.time_unix = _safe_int(json_obj.get("time_unix"))
        pb_obj.price = _safe_float(json_obj.get("price"))
        pb_obj.change_value = _safe_float(json_obj.get("change_value"))
        pb_obj.change_percent = _safe_float(json_obj.get("change_percent"))

    def gold_builder(json_obj: dict, pb_obj: Any):
        simple_copy = [
            "date",
            "time",
            "symbol",
            "name_en",
            "name",
            "unit",
            "name_fa",
        ]
        for attr in simple_copy:
            if attr in json_obj:
                setattr(pb_obj, attr, str(json_obj[attr]))
        pb_obj.time_unix = _safe_int(json_obj.get("time_unix"))
        pb_obj.price = _safe_float(json_obj.get("price"))
        pb_obj.change_value = _safe_float(json_obj.get("change_value"))
        pb_obj.change_percent = _safe_float(json_obj.get("change_percent"))

    def stock_builder(json_obj: dict, pb_obj: Any):
        # Straight mapping for the many scalar fields
        scalar_fields = [
            "time",
            "l18",
            "l30",
            "isin",
            "cs",
        ]
        for attr in scalar_fields:
            if attr in json_obj:
                setattr(pb_obj, attr, str(json_obj[attr]))

        int_fields = [
            "id",
            "cs_id",
            "z",
            "bvol",
            "mv",
            "tmin",
            "tmax",
            "pmin",
            "pmax",
            "py",
            "pf",
            "pl",
            "plc",
            "pc",
            "pcc",
            "tno",
            "tvol",
            "tval",
            "Buy_CountI",
            "Buy_CountN",
            "Sell_CountI",
            "Sell_CountN",
            "Buy_I_Volume",
            "Buy_N_Volume",
            "Sell_I_Volume",
            "Sell_N_Volume",
        ]
        for attr in int_fields:
            if attr in json_obj and hasattr(pb_obj, attr):
                setattr(pb_obj, attr, _safe_int(json_obj[attr]))

        float_fields = ["eps", "pe", "plp", "pcp"]
        for attr in float_fields:
            if attr in json_obj and hasattr(pb_obj, attr):
                setattr(pb_obj, attr, _safe_float(json_obj[attr]))

        # Aggregate order-book levels (1..5) into repeated message
        for level in range(1, 6):
            marker = f"zd{level}"
            if marker not in json_obj:
                continue  # no data for this level
            level_pb = pb_obj.order_levels.add()
            level_pb.zd = _safe_int(json_obj.get(f"zd{level}"))
            level_pb.qd = _safe_int(json_obj.get(f"qd{level}"))
            level_pb.pd = _safe_int(json_obj.get(f"pd{level}"))
            level_pb.po = _safe_int(json_obj.get(f"po{level}"))
            level_pb.qo = _safe_int(json_obj.get(f"qo{level}"))
            level_pb.zo = _safe_int(json_obj.get(f"zo{level}"))

    # Register in handler map
    HANDLERS.update(
        {
            "commodity": (
                lambda: pb_module.CommodityData(),
                lambda json_root, pb_root: [
                    commodity_builder(item, pb_root.metal_precious.add())
                    for item in json_root.get("metal_precious", [])
                ],
            ),
            "cryptocurrency": (
                lambda: pb_module.CryptoData(),
                lambda json_root, pb_root: [
                    crypto_builder(item, pb_root.items.add()) for item in json_root
                ],
            ),
            "currency": (
                lambda: pb_module.CurrencyData(),
                lambda json_root, pb_root: [
                    currency_builder(item, pb_root.items.add())
                    for item in json_root.get("currency", [])
                ],
            ),
            "gold": (
                lambda: pb_module.GoldData(),
                lambda json_root, pb_root: [
                    gold_builder(item, pb_root.items.add())
                    for item in json_root.get("gold", [])
                ],
            ),
            # stock-like datasets
            "tse_ifb_symbols": (
                lambda: pb_module.StockData(),
                lambda json_root, pb_root: [
                    stock_builder(item, pb_root.items.add()) for item in json_root
                ],
            ),
            "debt_securities": (
                lambda: pb_module.StockData(),
                lambda json_root, pb_root: [
                    stock_builder(item, pb_root.items.add()) for item in json_root
                ],
            ),
            "futures": (
                lambda: pb_module.StockData(),
                lambda json_root, pb_root: [
                    stock_builder(item, pb_root.items.add()) for item in json_root
                ],
            ),
            "housing_facilities": (
                lambda: pb_module.StockData(),
                lambda json_root, pb_root: [
                    stock_builder(item, pb_root.items.add()) for item in json_root
                ],
            ),
        }
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def generate_all_protobuf_files() -> None:  # noqa: D401 – simple function
    """Convert all JSON files under v1 to protobuf counterparts under v2."""

    # Compile proto and import generated module dynamically
    _ensure_proto_compiled()
    import importlib  # inline import to avoid circular issues

    pb_module = importlib.import_module("market_data_pb2", package=None)  # type: ignore

    # Populate handler registry once
    if not HANDLERS:
        _register_handlers(pb_module)

    # Walk through v1 directory (including stock sub-folder)
    for json_path in V1_DIR.rglob("*.json"):
        rel_path = json_path.relative_to(V1_DIR)
        dataset_name = json_path.stem  # filename without extension
        if dataset_name not in HANDLERS:
            # Skip unsupported dataset (yet)
            continue

        factory, builder = HANDLERS[dataset_name]
        pb_root = factory()

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception as ex:
            print(f"[protobuf] Failed to read {json_path}: {ex}")
            continue

        try:
            builder(json_data, pb_root)
        except Exception as ex:
            print(f"[protobuf] Failed to build message for {dataset_name}: {ex}")
            continue

        # Ensure destination directory exists
        dest_path = V2_DIR / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        # Change extension to .pb (binary). Keep same filename stem
        dest_path = dest_path.with_suffix(".pb")

        # Write binary wire format
        try:
            with open(dest_path, "wb") as f:
                f.write(pb_root.SerializeToString())
        except Exception as ex:
            print(f"[protobuf] Failed to write {dest_path}: {ex}")
            continue

        # Touch file timestamp to now
        os.utime(dest_path, None) 
