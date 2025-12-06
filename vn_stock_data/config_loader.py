# vn_stock_data/config_loader.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


def load_yaml_config(relative_path: str) -> Dict[str, Any]:
    """
    Load a YAML config file from include/config folder.

    relative_path: ví dụ "stock_list.yml", "eod_prices.yml"
    """
    # file này nằm trong vn_stock_data/, đi lên 1 level là project root
    base_dir = Path(__file__).resolve().parents[1]
    config_path = base_dir / "include" / "config" / relative_path

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
