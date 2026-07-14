"""Unified synchronous and asynchronous CMR client."""

import asyncio
import contextlib
import itertools
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import aiohttp
import backoff
import requests
from requests.exceptions import HTTPError

from . import CONFIG

logger = logging.getLogger(__name__)

CMR_URLS = {
    "PROD": CONFIG["cmr"]["url"],
    "UAT": CONFIG["cmr"]["url_uat"]
}


def _fatal_code(err: requests.exceptions.RequestException) -> bool:
    """Decide whether ``backoff`` should give up on this exception.

    ``backoff`` invokes this from inside the retry loop, so raising here
    aborts the loop with an ``AttributeError`` instead of retrying. Two
    classes of exceptions can reach us:

    * **HTTP errors** (``HTTPError`` raised by ``raise_for_status``) carry a
      populated ``response`` — keep retrying for the standard set of
      transient / throttling status codes.
    * **Transport errors** (``ConnectionError``, ``Timeout``, ``DNS``…)
      have ``response is None``. These are *always* transient by nature, so
      keep retrying until ``max_time`` is hit. Returning ``False`` here was
      the missing case that previously bubbled an ``AttributeError`` mid
      pagination.
    """
    response = getattr(err, "response", None)
    if response is None:
        return False
    return response.status_code not in [401, 418, 429, 500, 502, 503, 504]


def _backoff_logger(details):
    """Log backoff attempts."""
    logger.warning(
        f"Backing off for {details['wait']:0.1f} seconds after {details['tries']} tries. "
        f"Total time elapsed: {details['elapsed']:0.1f} seconds."
    )


@backoff.on_exception(
    backoff.constant,
    requests.exceptions.RequestException,
    max_time=CONFIG["cmr"].get("retry_max_time", 300),
    giveup=_fatal_code,
    on_backoff=_backoff_logger,
    interval=CONFIG["cmr"].get("retry_interval", 15),
)
def _do_cmr_request(url: str, params: dict, headers: Optional[dict] = None) -> tuple[list[dict], Optional[str]]:
    """
    Execute a single CMR request with retry logic.

    Args:
        url: CMR endpoint URL
        params: Query parameters
        headers: Optional headers (for pagination)

    Returns:
        Tuple of (granule list, search-after token)
    """
    if headers is None:
        headers = {}

    logger.debug(f"Querying {url} with params {params}")
    response = requests.get(url, params=params, headers=headers, timeout=CONFIG["cmr"]["timeout"])
    response.raise_for_status()

    response_json = response.json()
    granules = response_json.get("items", [])
    search_after = response.headers.get("CMR-Search-After", None)

    return granules, search_after


@backoff.on_exception(
    backoff.constant,
    requests.exceptions.RequestException,
    max_time=CONFIG["cmr"].get("retry_max_time", 300),
    giveup=_fatal_code,
    on_backoff=_backoff_logger,
    interval=CONFIG["cmr"].get("retry_interval", 15),
)
def _do_cmr_post_request(
    url: str,
    data: str,
    headers: Optional[dict] = None,
) -> tuple[dict, Optional[str]]:
    """Execute one form-encoded CMR POST page with shared retry settings."""
    request_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    request_headers.update(headers or {})
    response = requests.post(
        url,
        data=data,
        headers=request_headers,
        timeout=CONFIG["cmr"]["timeout"],
    )
    response.raise_for_status()
    return response.json(), response.headers.get("CMR-Search-After")


def query_cmr_post(data: str, url: Optional[str] = None) -> list[dict]:
    """Return all items from a form-encoded CMR POST query."""
    url = url or CMR_URLS["PROD"]
    if "page_size=" not in data:
        data += f"&page_size={CONFIG['cmr']['page_size']}"
    items = []
    headers = {}
    while True:
        response_json, search_after = _do_cmr_post_request(url, data, headers)
        items.extend(response_json.get("items", []))
        if not search_after:
            return items
        headers = {"CMR-Search-After": search_after}


def query_cmr(
    collection_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    venue: str = "PROD",
    skip_temporal: bool = False,
) -> list[dict]:
    """
    Query CMR for granules with pagination and retry logic.

    Args:
        collection_id: CMR collection concept ID
        start_date: Start of temporal range (optional)
        end_date: End of temporal range (optional)
        venue: 'PROD' or 'UAT'
        skip_temporal: If True, omit temporal filter (for static products with no time extent)

    Returns:
        List of granule dicts (CMR UMM JSON format)
    """
    cmr_url = CMR_URLS[venue]
    granules = []

    params = {
        "collection_concept_id": collection_id,
        "page_size": CONFIG["cmr"]["page_size"]
    }

    # Add temporal range if specified and not skipped
    if not skip_temporal and (start_date or end_date):
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ") if start_date else ""
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ") if end_date else ""
        params["temporal[]"] = f"{start_str},{end_str}"

    # Start timer and show initial message
    start_time = time.time()

    # Show initial progress
    print(f"\rQuerying CMR ({venue}): 0 granules retrieved | 00:00", end="", file=sys.stderr)
    sys.stderr.flush()

    # First request with text progress
    page_granules, search_after = _do_cmr_request(cmr_url, params)
    granules.extend(page_granules)

    # Print progress to stderr so it doesn't interfere with stdout
    elapsed = int(time.time() - start_time)
    elapsed_str = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
    print(f"\rQuerying CMR ({venue}): {len(granules)} granules retrieved | {elapsed_str}", end="", file=sys.stderr)
    sys.stderr.flush()

    # Paginate through remaining results
    while search_after:
        headers = {"CMR-Search-After": search_after}
        page_granules, search_after = _do_cmr_request(cmr_url, params, headers)
        granules.extend(page_granules)

        # Update progress with elapsed time
        elapsed = int(time.time() - start_time)
        elapsed_str = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
        print(f"\rQuerying CMR ({venue}): {len(granules)} granules retrieved | {elapsed_str}", end="", file=sys.stderr)
        sys.stderr.flush()

    # Final newline
    print(file=sys.stderr)

    logger.info(f"Retrieved {len(granules)} granules from CMR")
    return granules


def query_cmr_by_native_id_patterns(
    collection_id: str,
    native_id_patterns: list[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    venue: str = "PROD",
) -> list[dict]:
    """Query a collection for granules matching native-ID wildcard patterns.

    This uses the same retry, timeout, venue, and CMR-Search-After behavior as
    :func:`query_cmr`, but accepts repeated ``native-id`` patterns. It is used
    by DSWx-S1 coverage validation to ask whether the complete RTC burst set
    for a tile was available around an acquisition.

    :param collection_id: CMR collection concept ID.
    :param native_id_patterns: CMR wildcard patterns such as
        ``OPERA_L2_RTC-S1_T001-000001-IW1_*``.
    :param start_date: Optional temporal-window start.
    :param end_date: Optional temporal-window end.
    :param venue: ``PROD`` or ``UAT``.
    :returns: Matching CMR UMM JSON granule records.
    """
    if not collection_id:
        raise ValueError(f"No collection concept ID configured for venue {venue}")
    if not native_id_patterns:
        return []

    params = {
        "collection_concept_id": collection_id,
        "native-id": native_id_patterns,
        "options[native-id][pattern]": "true",
        "page_size": CONFIG["cmr"]["page_size"],
    }
    if start_date or end_date:
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ") if start_date else ""
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ") if end_date else ""
        params["temporal[]"] = f"{start_str},{end_str}"

    cmr_url = CMR_URLS[venue]
    granules: list[dict] = []
    headers: dict[str, str] = {}

    while True:
        page, search_after = _do_cmr_request(cmr_url, params, headers)
        granules.extend(page)
        if not search_after:
            break
        headers = {"CMR-Search-After": search_after}

    logger.info(
        "Retrieved %d CMR granules matching %d native-ID patterns",
        len(granules),
        len(native_id_patterns),
    )
    return granules


def query_cmr_by_short_name(
    short_name: str,
    provider: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    venue: str = "PROD"
) -> list[dict]:
    cmr_url = CMR_URLS[venue]
    granules = []

    params = {
        "short_name": short_name,
        "page_size": CONFIG["cmr"]["page_size"]
    }
    if provider:
        params["provider"] = provider

    if start_date or end_date:
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ") if start_date else ""
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ") if end_date else ""
        params["temporal[]"] = f"{start_str},{end_str}"

    start_time = time.time()

    print(f"\rQuerying CMR ({venue}): 0 granules retrieved | 00:00", end="", file=sys.stderr)
    sys.stderr.flush()

    page_granules, search_after = _do_cmr_request(cmr_url, params)
    granules.extend(page_granules)

    elapsed = int(time.time() - start_time)
    elapsed_str = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
    print(f"\rQuerying CMR ({venue}): {len(granules)} granules retrieved | {elapsed_str}", end="", file=sys.stderr)
    sys.stderr.flush()

    while search_after:
        headers = {"CMR-Search-After": search_after}
        page_granules, search_after = _do_cmr_request(cmr_url, params, headers)
        granules.extend(page_granules)

        elapsed = int(time.time() - start_time)
        elapsed_str = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
        print(f"\rQuerying CMR ({venue}): {len(granules)} granules retrieved | {elapsed_str}", end="", file=sys.stderr)
        sys.stderr.flush()

    print(file=sys.stderr)

    logger.info(f"Retrieved {len(granules)} granules from CMR")
    return granules


def _async_fatal_status(status: int) -> bool:
    """Return whether an HTTP status should fail without retrying."""
    return status not in (401, 408, 418, 429, 500, 502, 503, 504)


async def _async_post_json(
    session: aiohttp.ClientSession,
    url: str,
    data: str,
    headers: dict[str, str],
) -> tuple[dict, Optional[str]]:
    """POST one CMR page with configured exponential retry/backoff."""
    max_attempts = CONFIG["cmr"].get("async_max_attempts", 7)
    backoff_base = CONFIG["cmr"].get("async_backoff_base", 1)
    timeout = aiohttp.ClientTimeout(total=CONFIG["cmr"]["timeout"])

    for attempt in range(max_attempts):
        try:
            async with session.post(
                url,
                data=data,
                headers=headers,
                timeout=timeout,
            ) as response:
                if response.status >= 400:
                    if _async_fatal_status(response.status) or attempt == max_attempts - 1:
                        response.raise_for_status()
                    await asyncio.sleep(backoff_base * (2**attempt))
                    continue
                return await response.json(), response.headers.get("CMR-Search-After")
        except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError):
            if attempt == max_attempts - 1:
                raise
            await asyncio.sleep(backoff_base * (2**attempt))

    raise RuntimeError("CMR retry loop exited unexpectedly")


async def async_cmr_post(
    url: str,
    data: str,
    session: aiohttp.ClientSession,
    sem: Optional[asyncio.Semaphore] = None,
    output_path: Optional[str] = None,
) -> list[dict]:
    """Query all CMR pages asynchronously using CMR-Search-After.

    The return value preserves the legacy shape: a list of response-page
    dictionaries. When ``output_path`` is supplied, items are streamed to
    JSONL and an empty list is returned.
    """
    context = sem if sem is not None else contextlib.nullcontext()
    page_size = CONFIG["cmr"]["page_size"]
    if "page_size=" not in data:
        data += f"&page_size={page_size}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Client-Id": f"nasa.jpl.opera.sds.ops.{os.environ.get('USER', 'unknown')}",
    }
    pages = []
    total_fetched = 0
    total_hits = None

    async with context:
        while True:
            response_json, search_after = await _async_post_json(
                session, url, data, headers
            )
            items = response_json.get("items", [])
            if total_hits is None:
                hits = response_json.get("hits")
                total_hits = hits if isinstance(hits, int) else None
            total_fetched += len(items)

            if output_path:
                with Path(output_path).open("a") as stream:
                    for item in items:
                        stream.write(json.dumps(item) + "\n")
            else:
                pages.append(response_json)

            if (
                not search_after
                or len(items) < page_size
                or (total_hits is not None and total_fetched >= total_hits)
            ):
                break
            headers["CMR-Search-After"] = search_after

    return pages


async def async_cmr_post_items(
    url: str,
    data: str,
    session: aiohttp.ClientSession,
    sem: Optional[asyncio.Semaphore] = None,
) -> list[dict]:
    """Return flattened items from an asynchronous paginated CMR POST."""
    pages = await async_cmr_post(url, data, session, sem)
    return list(
        itertools.chain.from_iterable(page.get("items", []) for page in pages)
    )


async def async_cmr_posts(
    url: str,
    request_bodies: list[str],
    sem: Optional[asyncio.Semaphore] = None,
    output_dir: Optional[str] = None,
) -> list:
    """Run multiple asynchronous CMR POST queries concurrently."""
    concurrency = 1 if len(request_bodies) == 1 else min(len(request_bodies), 15)
    sem = sem or asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession() as session:
        if output_dir:
            output = Path(output_dir)
            output.mkdir(parents=True, exist_ok=True)
            paths = [output / f"cmr_batch_{index}.jsonl" for index in range(len(request_bodies))]
            await asyncio.gather(
                *(
                    async_cmr_post(url, body, session, sem, str(path))
                    for body, path in zip(request_bodies, paths)
                )
            )
            return [str(path) for path in paths]

        page_groups = await asyncio.gather(
            *(async_cmr_post(url, body, session, sem) for body in request_bodies)
        )
    return list(itertools.chain.from_iterable(page_groups))


def giveup_cmr_requests(error) -> bool:
    """Compatibility predicate for legacy callers using backoff decorators."""
    if isinstance(error, aiohttp.ClientResponseError):
        return _async_fatal_status(error.status)
    if isinstance(error, HTTPError) and error.response is not None:
        return _fatal_code(error)
    return False


@backoff.on_exception(
    backoff.expo,
    exception=(aiohttp.ClientResponseError, aiohttp.ClientOSError),
    max_tries=CONFIG["cmr"].get("async_max_attempts", 7),
    jitter=None,
    giveup=giveup_cmr_requests,
)
async def fetch_post_url(session: aiohttp.ClientSession, url, data: str, headers):
    """Legacy single-page POST API retained for PCM-derived callers."""
    return await session.post(
        url,
        data=data,
        headers=headers,
        raise_for_status=True,
        timeout=aiohttp.ClientTimeout(total=CONFIG["cmr"]["timeout"]),
    )


def try_request_get(request_url, params, headers=None, raise_for_status=True):
    """Compatibility blocking GET using the unified configured timeout."""
    response = requests.get(
        request_url,
        params=params,
        headers=headers,
        timeout=CONFIG["cmr"]["timeout"],
    )
    if raise_for_status:
        response.raise_for_status()
    return response


def extract_native_ids(paths: Iterable[str]) -> set[str]:
    """Read streamed CMR JSONL files and return their native IDs."""
    return {
        item["meta"]["native-id"]
        for path in paths
        for item in _iter_jsonl(path)
    }


def extract_fields(paths: Iterable[str], fields: list[str]) -> list[dict]:
    """Extract dot-delimited fields from streamed CMR JSONL results."""
    return [
        {field: _get_nested(item, field) for field in fields}
        for path in paths
        for item in _iter_jsonl(path)
    ]


def _iter_jsonl(path: str):
    with Path(path).open() as stream:
        for line in stream:
            yield json.loads(line)


def _get_nested(obj, path: str):
    for key in path.split("."):
        if not isinstance(obj, dict) or key not in obj:
            raise KeyError(f"Key '{key}' not found in path '{path}'")
        obj = obj[key]
    return obj


def paramss_to_request_body(paramss: Iterable[dict]) -> list[str]:
    """Convert multiple parameter dictionaries to CMR POST bodies."""
    return [params_to_request_body(params) for params in paramss]


def params_to_request_body(params: dict) -> str:
    """Convert query parameters to a CMR form-encoded POST body."""
    parts = []
    for key, value in params.items():
        if key == "token" and value is None:
            continue
        if isinstance(value, Iterable) and not isinstance(value, str):
            repeated_key = key if key.endswith("[]") else f"{key}[]"
            parts.extend(f"&{repeated_key}={item}" for item in value)
        else:
            parts.append(f"&{key}={value}")
    return "".join(parts)
