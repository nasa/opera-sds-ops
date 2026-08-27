"""DSWx-S1 accountability strategy.

Consolidated from Riley's ``accountability_tools/dswx_s1/`` pipeline. The
five-step workflow (survey → mapping → tile-set resolution → cycle indexing →
real-coverage validation) is orchestrated by :func:`run` in
:mod:`opera_accountability.strategies.dswx_s1.pipeline`.
"""

from .pipeline import run

__all__ = ["run"]
