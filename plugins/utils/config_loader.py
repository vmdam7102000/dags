# plugins/utils/config_loader.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml


def load_yaml_config(relative_path: str) -> Dict[str, Any]:
    """
    Load a YAML config file from the project include/config folder.

    relative_path: relative path under include/config, e.g. "stock_list.yml"
    """
    # This file lives under plugins/utils, so go up two levels to reach project root
    base_dir = Path(__file__).resolve().parents[2]
    config_path = base_dir / "include" / "config" / relative_path

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
