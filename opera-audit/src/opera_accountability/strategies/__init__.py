"""Accountability strategies.

Each product with ``accountability.enabled: true`` in ``config.yaml`` selects a
strategy module here via its ``accountability.strategy`` key. Strategy modules
expose a ``run(...)`` entrypoint invoked by the CLI.
"""
