"""Backward-compatible imports for the unified :mod:`opera_accountability.cmr` client.

New code should import synchronous and asynchronous CMR helpers from ``cmr``.
This module remains so existing PCM-derived callers do not break immediately.
"""

from .cmr import (
    async_cmr_post,
    async_cmr_post_items,
    async_cmr_posts,
    extract_fields,
    extract_native_ids,
    fetch_post_url,
    giveup_cmr_requests,
    params_to_request_body,
    paramss_to_request_body,
    try_request_get,
)

__all__ = [
    "async_cmr_post",
    "async_cmr_post_items",
    "async_cmr_posts",
    "extract_fields",
    "extract_native_ids",
    "fetch_post_url",
    "giveup_cmr_requests",
    "params_to_request_body",
    "paramss_to_request_body",
    "try_request_get",
]
