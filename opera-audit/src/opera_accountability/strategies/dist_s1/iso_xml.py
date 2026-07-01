from __future__ import annotations

import hashlib
import logging
import netrc
import os
import random
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

# Default retry configuration (ported from PCM cmr_iso_xml_utils.py)
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0  # seconds
DEFAULT_MAX_DELAY = 30.0  # seconds
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Thread-local storage for session reuse (ported from PCM cmr_iso_xml_utils.py)
_thread_local = threading.local()

# Cache configuration (module-level state, ported from PCM cmr_iso_xml_utils.py)
_cache_config = {
    "enabled": False,
    "cache_dir": None,
    "hits": 0,
    "misses": 0,
}
_cache_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Caching (ported from PCM cmr_iso_xml_utils.py)
# ---------------------------------------------------------------------------

def configure_iso_xml_cache(cache_dir=None, enabled=True):
    """Configure ISO XML file caching.

    :param cache_dir: Directory path for cache files. ``None`` disables caching.
    :param enabled: Whether caching is enabled (default ``True`` when *cache_dir* given).
    :returns: Dict with cache configuration status.
    """
    global _cache_config

    with _cache_lock:
        if cache_dir:
            cache_path = Path(cache_dir)
            cache_path.mkdir(parents=True, exist_ok=True)
            _cache_config["cache_dir"] = cache_path
            _cache_config["enabled"] = enabled
            _cache_config["hits"] = 0
            _cache_config["misses"] = 0
            logger.info(f"ISO XML cache enabled at: {cache_path}")
        else:
            _cache_config["enabled"] = False
            _cache_config["cache_dir"] = None
            logger.debug("ISO XML cache disabled")

        return {
            "enabled": _cache_config["enabled"],
            "cache_dir": str(_cache_config["cache_dir"]) if _cache_config["cache_dir"] else None,
        }


def get_cache_stats():
    """Return cache hit/miss statistics."""
    with _cache_lock:
        hits = _cache_config["hits"]
        misses = _cache_config["misses"]
        total = hits + misses
        hit_rate = (hits / total * 100) if total > 0 else 0
        return {"hits": hits, "misses": misses, "total": total, "hit_rate": hit_rate}


def _get_cache_key(url):
    url_parts = url.split("/")
    filename = url_parts[-1] if url_parts else ""
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:12]
    if filename.endswith(".iso.xml"):
        base_name = filename[:-8]
        return f"{base_name}_{url_hash}.xml"
    return f"{url_hash}.xml"


def _get_from_cache(url):
    if not _cache_config["enabled"] or not _cache_config["cache_dir"]:
        return None
    cache_path = _cache_config["cache_dir"] / _get_cache_key(url)
    try:
        if cache_path.exists():
            content = cache_path.read_text(encoding="utf-8")
            with _cache_lock:
                _cache_config["hits"] += 1
            logger.debug(f"Cache hit for {url}")
            return content
    except Exception as exc:
        logger.debug(f"Cache read error for {url}: {exc}")
    return None


def _save_to_cache(url, content):
    if not _cache_config["enabled"] or not _cache_config["cache_dir"] or content is None:
        return
    cache_path = _cache_config["cache_dir"] / _get_cache_key(url)
    try:
        cache_path.write_text(content, encoding="utf-8")
        logger.debug(f"Cached ISO XML for {url}")
    except Exception as exc:
        logger.debug(f"Cache write error for {url}: {exc}")


def _calculate_backoff_delay(attempt, base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
    """Exponential backoff with jitter (ported from PCM cmr_iso_xml_utils.py)."""
    delay = base_delay * (2 ** attempt)
    delay = delay * (0.5 + random.random())  # jitter ×0.5–1.5
    return min(delay, max_delay)


def _apply_earthdata_credentials(session: requests.Session) -> requests.Session:
    """Apply Earthdata Login credentials to an existing session.

    Credential sources (checked in order):
    1. EARTHDATA_TOKEN environment variable (Bearer token)
    2. ~/.netrc entry for urs.earthdata.nasa.gov (username/password)

    Raises RuntimeError with a clear message if no credentials are found.
    """
    token = os.environ.get("EARTHDATA_TOKEN")
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
        return session

    try:
        netrc_file = netrc.netrc()
        auth = netrc_file.authenticators("urs.earthdata.nasa.gov")
        if auth:
            session.auth = (auth[0], auth[2])
            return session
    except (FileNotFoundError, netrc.NetrcParseError):
        pass

    raise RuntimeError(
        "Earthdata Login credentials required for ISO-XML downloads.\n"
        "Provide credentials via one of:\n"
        "  1. EARTHDATA_TOKEN environment variable (Bearer token)\n"
        "  2. ~/.netrc entry for urs.earthdata.nasa.gov\n"
        "     machine urs.earthdata.nasa.gov login <user> password <pass>\n"
        "Register at https://urs.earthdata.nasa.gov/users/new if needed."
    )


def _get_earthdata_session() -> requests.Session:
    """Get a thread-local requests.Session with connection pooling and EDL credentials.

    Reusing sessions significantly improves performance for multiple requests
    to the same host by keeping connections alive (ported from PCM cmr_iso_xml_utils.py).
    """
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _apply_earthdata_credentials(session)
        _thread_local.session = session
    return _thread_local.session


def extract_iso_xml_url(product: dict, prefer_s3: bool = False) -> str:
    s3_url = None
    https_url = None

    for related_url in product.get("umm", {}).get("RelatedUrls", []):
        url = related_url.get("URL", "")
        if url.startswith("s3://") and url.endswith("iso.xml"):
            s3_url = url
            if prefer_s3:
                return s3_url
        elif url.startswith(("http://", "https://")) and url.endswith("iso.xml"):
            https_url = url.replace("earthdatacloud.nasa.gov", "alaska.edu")
            if not prefer_s3:
                return https_url

    if prefer_s3 and s3_url:
        return s3_url
    if https_url:
        return https_url
    if s3_url:
        return s3_url

    native_id = product.get("meta", {}).get("native-id") or product.get("umm", {}).get("GranuleUR")
    raise RuntimeError(f"No iso.xml URL found for {native_id}")


def _get_s3_object(url: str, max_retries: int, base_delay: float) -> bytes:
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as err:
        raise ImportError("S3 iso.xml access requires optional boto3/botocore dependencies.") from err

    parsed_url = urlparse(url)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")
    s3_client = boto3.client("s3")
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except (BotoCoreError, ClientError) as err:
            last_exception = err
            if attempt < max_retries:
                time.sleep(base_delay * (2 ** attempt))

    raise last_exception


def _get_http_content(url: str, max_retries: int, base_delay: float) -> bytes:
    session = _get_earthdata_session()
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = session.get(url, timeout=30, allow_redirects=True)
            if response.status_code == 401:
                raise RuntimeError(
                    f"401 Unauthorized fetching {url}. "
                    "Check your Earthdata Login credentials "
                    "(EARTHDATA_TOKEN env var or ~/.netrc for urs.earthdata.nasa.gov)."
                )
            response.raise_for_status()
            return response.content
        except RequestException as err:
            last_exception = err
            status_code = getattr(getattr(err, "response", None), "status_code", None)
            if status_code is not None and 400 <= status_code < 500 and status_code != 429:
                raise
            if attempt < max_retries:
                delay = _calculate_backoff_delay(attempt, base_delay)
                logger.debug(
                    f"Request error fetching {url}: {err}, "
                    f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(delay)

    raise last_exception


def obtain_iso_xml(url: str, max_retries: int = 3, base_delay: float = 1.0) -> ET.Element:
    if url.startswith("s3://"):
        content = _get_s3_object(url, max_retries, base_delay)
    elif url.startswith(("http://", "https://")):
        content = _get_http_content(url, max_retries, base_delay)
    else:
        raise ValueError(f"Unsupported URL protocol: {url}")
    return ET.fromstring(content)


def fetch_iso_xml(iso_xml_url, timeout=30, max_retries=DEFAULT_MAX_RETRIES,
                  base_delay=DEFAULT_BASE_DELAY, max_delay=DEFAULT_MAX_DELAY):
    """Fetch ISO XML content with caching and connection pooling.

    Ported from PCM ``cmr_iso_xml_utils.py``.
    Returns XML content as *str*, or ``None`` on failure.
    """
    cached_content = _get_from_cache(iso_xml_url)
    if cached_content is not None:
        return cached_content

    with _cache_lock:
        _cache_config["misses"] += 1

    session = _get_earthdata_session()
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            response = session.get(iso_xml_url, timeout=timeout)
            response.raise_for_status()
            content = response.text
            _save_to_cache(iso_xml_url, content)
            return content
        except requests.exceptions.HTTPError as exc:
            last_exception = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                delay = _calculate_backoff_delay(attempt, base_delay, max_delay)
                logger.debug(
                    f"Retryable HTTP {status_code} error fetching {iso_xml_url}, "
                    f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(delay)
                continue
            logger.warning(f"Failed to fetch ISO XML from {iso_xml_url}: {exc}")
            return None
        except requests.exceptions.RequestException as exc:
            last_exception = exc
            if attempt < max_retries:
                delay = _calculate_backoff_delay(attempt, base_delay, max_delay)
                logger.debug(
                    f"Request error fetching {iso_xml_url}: {exc}, "
                    f"retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(delay)
                continue
            logger.warning(
                f"Failed to fetch ISO XML from {iso_xml_url} after {max_retries + 1} attempts: {exc}"
            )
            return None

    logger.warning(f"Failed to fetch ISO XML from {iso_xml_url} after {max_retries + 1} attempts: {last_exception}")
    return None


# ---------------------------------------------------------------------------
# ISO XML parsing helpers (ported from PCM cmr_iso_xml_utils.py)
# ---------------------------------------------------------------------------

def parse_iso_xml_input_granules(xml_content, filename_filter=None):
    """Parse ISO XML content and extract input granule filenames from FileName elements.

    :param xml_content: XML content as *str*.
    :param filename_filter: Optional callable ``(str) -> bool``.
    :returns: List of input granule filenames.
    """
    if xml_content is None:
        return []
    try:
        root = ET.fromstring(xml_content)
        input_granules = []
        for elem in root.iter():
            tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag_name == "FileName" and elem.text:
                text = elem.text.strip()
                if filename_filter is None or filename_filter(text):
                    input_granules.append(text)
        return input_granules
    except ET.ParseError as exc:
        logger.warning(f"Failed to parse ISO XML: {exc}")
        return []


def fetch_and_parse_iso_xml_input_granules(iso_xml_url, filename_filter=None, timeout=60):
    """Convenience: fetch ISO XML and extract input granule filenames."""
    xml_content = fetch_iso_xml(iso_xml_url, timeout=timeout)
    return parse_iso_xml_input_granules(xml_content, filename_filter=filename_filter)


# ---------------------------------------------------------------------------
# CSLC lineage helpers for DISP-S1 (ported from PCM cmr_iso_xml_utils.py)
# ---------------------------------------------------------------------------

def cslc_filename_filter(filename):
    """Return ``True`` for regular CSLC files, excluding STATIC and COMPRESSED."""
    return "CSLC" in filename and "STATIC" not in filename and "COMPRESSED" not in filename


def normalize_cslc_filename(filename):
    """Strip ``.h5`` extension from CSLC filenames."""
    return filename[:-3] if filename.endswith(".h5") else filename


def fetch_cslc_input_granules_from_iso_xml(iso_xml_url, timeout=60):
    """Fetch ISO XML and extract CSLC input granule IDs (without ``.h5``).

    Specialized for DISP-S1 lineage tracking.
    """
    granules = fetch_and_parse_iso_xml_input_granules(
        iso_xml_url, filename_filter=cslc_filename_filter, timeout=timeout
    )
    return [normalize_cslc_filename(g) for g in granules]


def extract_dist_input_granules(root: ET.Element | str | bytes) -> set[str]:
    """Extract comma-separated PostRtcOperaIds from ISO XML.
    
    Exact port of Kevin's cmr_audit_dist_s1.py:344-357
    """
    if isinstance(root, (str, bytes)):
        root = ET.fromstring(root)
    
    # Use explicit namespaces matching Kevin's original
    namespaces = {
        "eos": "http://earthdata.nasa.gov/schema/eos",
        "gco": "http://www.isotc211.org/2005/gco",
    }
    
    for attr in root.findall(".//eos:AdditionalAttribute", namespaces):
        name = attr.find(".//eos:name/gco:CharacterString", namespaces)
        if name is not None and name.text == "PostRtcOperaIds":
            value_elem = attr.find(".//eos:value/gco:CharacterString", namespaces)
            if value_elem is not None and value_elem.text:
                return {x.strip() for x in value_elem.text.split(",")}
    return set()
