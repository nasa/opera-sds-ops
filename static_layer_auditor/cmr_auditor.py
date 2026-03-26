from datetime import datetime, timezone
import logging
import os
import re
from typing import List, Optional, Set, Iterator
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests


logger = logging.getLogger("burst-audit-tool")


def fetch_cmr_page(
    collection_id: str,
    page_num: int,
    updated_since: Optional[str],
    page_size: int = 2000
) -> List[str]:
    """
    Fetch a single page of CMR granules using page_num pagination.

    Args:
        collection_id: CMR collection concept ID
        page_num: Page number (1-indexed)
        updated_since: ISO8601 datetime for incremental updates
        page_size: Results per page (max 2000)

    Returns:
        List of producer granule IDs from this page
    """
    base_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
    params = {
        "collection_concept_id": collection_id,
        "page_size": page_size,
        "page_num": page_num
    }

    if updated_since:
        params["updated_since"] = updated_since

    # CSLC baseline collection contains accidental global products
    # We use revision_date filtering to exclude them from queries
    if collection_id == "C2777443834-ASF":
        params["revision_date[]"] = [",2024-04-20T00:00:00Z", "2024-05-15T00:00:00Z,"]

    # Session with retry logic for transient network errors
    session = requests.Session()
    retries = requests.adapters.Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))

    resp = session.get(base_url, params=params)
    resp.raise_for_status()

    entries = resp.json().get("feed", {}).get("entry", [])
    native_ids = [e["producer_granule_id"] for e in entries]

    session.close()
    return native_ids


def fetch_cmr_granules_parallel(
    collection_id: str,
    updated_since: Optional[str] = None,
    max_workers: int = 5,
    batch_size: int = 50000
) -> Iterator[Set[str]]:
    """
    Query NASA CMR for granules using parallel page fetching, yielding burst IDs in batches.

    Fetches pages in parallel while respecting rate limits, extracts burst IDs,
    and yields them in batches for incremental processing.

    Args:
        collection_id: The concept ID of the CMR collection to search
        updated_since: ISO8601 datetime string (UTC) for incremental updates
        max_workers: Number of parallel CMR requests (default: 5)
        batch_size: Number of burst IDs to accumulate before yielding (default: 50000)

    Yields:
        Set[str]: Batches of unique burst IDs
    """
    logger.info(f"Starting parallel CMR fetch for {collection_id} with {max_workers} workers")

    # Probe first page to get total hit count
    first_page_ids = fetch_cmr_page(collection_id, 1, updated_since)

    # Get total hits from a separate probe request
    session = requests.Session()
    params = {
        "collection_concept_id": collection_id,
        "page_size": 1
    }
    if updated_since:
        params["updated_since"] = updated_since
    if collection_id == "C2777443834-ASF":
        params["revision_date[]"] = [",2024-04-20T00:00:00Z", "2024-05-15T00:00:00Z,"]

    resp = session.get("https://cmr.earthdata.nasa.gov/search/granules.json", params=params)
    resp.raise_for_status()
    total_hits = int(resp.headers.get("CMR-Hits", 0))
    session.close()

    logger.info(f"Total hits for {collection_id}: {total_hits}")

    page_size = 2000
    total_pages = (total_hits + page_size - 1) // page_size

    if total_pages <= 1:
        # Only one page, return immediately
        burst_batch = {extract_burst_id_from_native(nid) for nid in first_page_ids if nid}
        burst_batch.discard(None)
        if burst_batch:
            yield burst_batch
        return

    logger.info(f"Fetching {total_pages} pages in parallel")

    burst_batch = set()
    total_processed = 0

    # Process first page
    for native_id in first_page_ids:
        burst_id = extract_burst_id_from_native(native_id)
        if burst_id:
            burst_batch.add(burst_id)

    total_processed += len(first_page_ids)
    logger.info(f"Processed page 1: {len(first_page_ids)} granules, {total_processed}/{total_hits} total")

    # Fetch remaining pages in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all page fetch tasks (pages 2 through total_pages)
        futures = {
            executor.submit(fetch_cmr_page, collection_id, page_num, updated_since): page_num
            for page_num in range(2, total_pages + 1)
        }

        # Process results as they complete
        for future in futures:
            try:
                native_ids = future.result()

                # Extract burst IDs from this page
                for native_id in native_ids:
                    burst_id = extract_burst_id_from_native(native_id)
                    if burst_id:
                        burst_batch.add(burst_id)

                total_processed += len(native_ids)
                page_num = futures[future]
                logger.info(f"Processed page {page_num}: {len(native_ids)} granules, {total_processed}/{total_hits} total")

                # Yield batch if full
                if len(burst_batch) >= batch_size:
                    logger.info(f"Yielding batch of {len(burst_batch)} unique burst IDs")
                    yield burst_batch
                    burst_batch = set()

            except Exception as e:
                page_num = futures[future]
                logger.error(f"Error fetching page {page_num}: {e}")
                raise

    # Yield any remaining bursts
    if burst_batch:
        logger.info(f"Yielding final batch of {len(burst_batch)} unique burst IDs")
        yield burst_batch

    logger.info(f"Completed parallel fetch: {total_processed} granules processed")


def extract_burst_id_from_native(native_id: str) -> Optional[str]:
    """
    Extract the full burst ID from a native filename or identifier.

    This searches for and returns a substring in the format 'T###-######-IW#'.

    Args:
        native_id (str): The string to extract the burst ID from.

    Returns:
        Optional[str]: The extracted burst ID if found, otherwise None.
    """
    m = re.search(r"(T\d{3}-\d{6}-IW\d)", native_id)
    return m.group(1) if m else None


def update_cache_batch(audit_collection: str, burst_batch: Set[str], is_final: bool = False):
    """
    Incrementally update burst cache with a batch of burst IDs.

    Uses an efficient append-only strategy during batch updates, with final deduplication
    and sorting only when is_final=True.

    Args:
        audit_collection: Name of the collection being audited
        burst_batch: Set of burst IDs to add to cache (can be empty if is_final=True)
        is_final: If True, perform deduplication and sorting (default: False)
    """
    cache_path = os.path.join("burst_inventory", f"{audit_collection}_cmr_cache.csv")

    # Skip empty batches unless this is the final deduplication pass
    if not burst_batch and not is_final:
        return

    if is_final:
        # Final pass: deduplicate and sort entire cache
        logger.info(f"Performing final deduplication and sort for {cache_path}")

        if os.path.exists(cache_path):
            # Read all cached bursts
            cached_df = pd.read_csv(cache_path, header=None, names=["burst_id"])
            all_bursts = set(cached_df["burst_id"].tolist())
            all_bursts.update(burst_batch)
        else:
            all_bursts = burst_batch

        # Write deduplicated and sorted
        sorted_bursts = sorted(all_bursts)
        final_df = pd.DataFrame(sorted_bursts)
        final_df.to_csv(cache_path, index=False, header=False)
        logger.info(f"Final cache contains {len(sorted_bursts)} unique bursts in {cache_path}")

    else:
        # Incremental update: append without deduplication (fast)
        sorted_bursts = sorted(burst_batch)
        batch_df = pd.DataFrame(sorted_bursts)
        batch_df.to_csv(
            cache_path,
            mode='a',
            index=False,
            header=False
        )
        logger.info(f"Appended {len(burst_batch)} bursts to {cache_path} (incremental update)")


def get_cache_date(audit_collection: str) -> Optional[str]:
    """
    Get the last update date for a cache using file modification time.

    Args:
        audit_collection: Name of the collection cache (e.g., "RTC_static")

    Returns:
        ISO8601 date string if cache exists, None otherwise
    """
    cache_path = os.path.join("burst_inventory", f"{audit_collection}_cmr_cache.csv")

    if os.path.exists(cache_path):
        mtime = os.path.getmtime(cache_path)
        cache_date = datetime.fromtimestamp(mtime, tz=timezone.utc)
        return cache_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    return None


def run_audit(
    collection_id: str,
    audit_collection: str,
    max_workers: int = 5,
    batch_size: int = 50000
):
    """
    Audits a given collection on CMR using parallel fetching and maintains a local cache of unique burst IDs.

    Uses file modification time to determine when to perform incremental vs full audits.

    Args:
        collection_id: CMR collection concept ID
        audit_collection: Name for the audit cache (e.g., "RTC_static")
        max_workers: Number of parallel CMR page requests (default: 5)
        batch_size: Number of burst IDs per batch (default: 50000)
    """
    # Check if cache exists and get its modification date
    cache_date = get_cache_date(audit_collection)

    if cache_date:
        logger.info(f"Found existing cache for {audit_collection}, last updated {cache_date}")
        logger.info("Performing incremental update for granules modified since cache date")
    else:
        logger.info(f"No cache found for {audit_collection}, performing full audit")

    # Stream bursts in batches using parallel CMR fetching
    batch_count = 0
    last_batch = None

    for burst_batch in fetch_cmr_granules_parallel(
        collection_id=collection_id,
        updated_since=cache_date,
        max_workers=max_workers,
        batch_size=batch_size
    ):
        batch_count += 1
        logger.info(f"Processing batch {batch_count} with {len(burst_batch)} unique burst IDs")

        # Append this batch to cache (no deduplication yet)
        update_cache_batch(audit_collection, burst_batch, is_final=False)
        last_batch = burst_batch

    # Final pass: deduplicate and sort the entire cache
    if last_batch is not None:
        logger.info("Finalizing cache with deduplication and sorting")
        update_cache_batch(audit_collection, set(), is_final=True)
    else:
        logger.info("No new bursts found in this audit")
