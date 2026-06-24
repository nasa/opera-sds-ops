"""OPERA Accountability Framework - Duplicate detection and accountability analysis."""

from importlib.resources import files as _pkg_files

import yaml

__version__ = "0.1.0"

# Load configuration on import.
#
# ``config.yaml`` lives inside the package (``src/opera_accountability/``) so
# it is available via :mod:`importlib.resources` for both editable installs
# (``pip install -e .``) and wheel installs (``pip install .``). Previously
# the loader walked up from ``__file__`` to the project root, which worked
# for editable installs but silently broke for installed wheels where the
# project root no longer contains ``config.yaml``.
_config_resource = _pkg_files('opera_accountability').joinpath('config.yaml')
CONFIG = yaml.safe_load(_config_resource.read_text())

__all__ = ['CONFIG', '__version__']
