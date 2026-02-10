"""OPERA Accountability Framework - Duplicate detection and accountability analysis."""

import yaml
from pathlib import Path

__version__ = "0.1.0"

# Load configuration on import
_config_path = Path(__file__).parent.parent.parent / "config.yaml"
with open(_config_path, 'r') as f:
    CONFIG = yaml.safe_load(f)

__all__ = ['CONFIG', '__version__']
