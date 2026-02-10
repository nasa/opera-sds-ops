"""CMR client for querying granules with retry and pagination."""

import logging
import sys
import time
from datetime import datetime
from typing import Optional

import backoff
import requests

from . import CONFIG

logger = logging.getLogger(__name__)

CMR_URLS = {
    'PROD': CONFIG['cmr']['url'],
    'UAT': CONFIG['cmr']['url_uat']
}


def _fatal_code(err: requests.exceptions.RequestException) -> bool:
    """Check if error code should stop retrying."""
    return err.response.status_code not in [401, 418, 429, 500, 502, 503, 504]


def _backoff_logger(details):
    """Log backoff attempts."""
    logger.warning(
        f"Backing off for {details['wait']:0.1f} seconds after {details['tries']} tries. "
        f"Total time elapsed: {details['elapsed']:0.1f} seconds."
    )


@backoff.on_exception(
    backoff.constant,
    requests.exceptions.RequestException,
    max_time=300,
    giveup=_fatal_code,
    on_backoff=_backoff_logger,
    interval=15
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

    logger.debug(f'Querying {url} with params {params}')
    response = requests.get(url, params=params, headers=headers, timeout=CONFIG['cmr']['timeout'])
    response.raise_for_status()

    response_json = response.json()
    granules = response_json.get('items', [])
    search_after = response.headers.get('CMR-Search-After', None)

    return granules, search_after


def query_cmr(
    collection_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    venue: str = 'PROD'
) -> list[dict]:
    """
    Query CMR for granules with pagination and retry logic.

    Args:
        collection_id: CMR collection concept ID
        start_date: Start of temporal range (optional)
        end_date: End of temporal range (optional)
        venue: 'PROD' or 'UAT'

    Returns:
        List of granule dicts (CMR UMM JSON format)
    """
    cmr_url = CMR_URLS[venue]
    granules = []

    params = {
        'collection_concept_id': collection_id,
        'page_size': CONFIG['cmr']['page_size']
    }

    # Add temporal range if specified
    if start_date or end_date:
        start_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ') if start_date else ''
        end_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ') if end_date else ''
        params['temporal[]'] = f'{start_str},{end_str}'

    # Start timer and show initial message
    start_time = time.time()

    # Show initial progress
    print(f"\rQuerying CMR ({venue}): 0 granules retrieved | 00:00", end='', file=sys.stderr)
    sys.stderr.flush()

    # First request with text progress
    page_granules, search_after = _do_cmr_request(cmr_url, params)
    granules.extend(page_granules)

    # Print progress to stderr so it doesn't interfere with stdout
    elapsed = int(time.time() - start_time)
    elapsed_str = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
    print(f"\rQuerying CMR ({venue}): {len(granules)} granules retrieved | {elapsed_str}", end='', file=sys.stderr)
    sys.stderr.flush()

    # Paginate through remaining results
    while search_after:
        headers = {'CMR-Search-After': search_after}
        page_granules, search_after = _do_cmr_request(cmr_url, params, headers)
        granules.extend(page_granules)

        # Update progress with elapsed time
        elapsed = int(time.time() - start_time)
        elapsed_str = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
        print(f"\rQuerying CMR ({venue}): {len(granules)} granules retrieved | {elapsed_str}", end='', file=sys.stderr)
        sys.stderr.flush()

    # Final newline
    print(file=sys.stderr)

    logger.info(f"Retrieved {len(granules)} granules from CMR")
    return granules
