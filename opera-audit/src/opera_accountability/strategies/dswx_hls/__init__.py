"""DSWx-HLS accountability strategy.

Maps DSWx-HLS outputs back to their HLS (S30/L30) inputs in the same time
window to compute accountability. Selected via
``products.DSWX_HLS.accountability.strategy: dswx_hls`` in ``config.yaml``.
"""

from .accountability import analyze_accountability

__all__ = ["analyze_accountability"]
