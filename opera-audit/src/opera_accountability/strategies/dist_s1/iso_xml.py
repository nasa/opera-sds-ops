from __future__ import annotations

import logging
import netrc
import os
import time
import xml.etree.ElementTree as ET
from typing import Iterable
from urllib.parse import urlparse

import requests
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


def _get_earthdata_session() -> requests.Session:
    """Build a requests.Session with Earthdata Login credentials.

    Credential sources (checked in order):
    1. EARTHDATA_TOKEN environment variable (Bearer token)
    2. ~/.netrc entry for urs.earthdata.nasa.gov (username/password)

    Raises RuntimeError with a clear message if no credentials are found.
    """
    session = requests.Session()

    token = os.environ.get('EARTHDATA_TOKEN')
    if token:
        session.headers['Authorization'] = f'Bearer {token}'
        return session

    try:
        netrc_file = netrc.netrc()
        auth = netrc_file.authenticators('urs.earthdata.nasa.gov')
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
                time.sleep(base_delay * (2 ** attempt))

    raise last_exception


def obtain_iso_xml(url: str, max_retries: int = 3, base_delay: float = 1.0) -> ET.Element:
    if url.startswith("s3://"):
        content = _get_s3_object(url, max_retries, base_delay)
    elif url.startswith(("http://", "https://")):
        content = _get_http_content(url, max_retries, base_delay)
    else:
        raise ValueError(f"Unsupported URL protocol: {url}")
    return ET.fromstring(content)


def _local_name(tag: str) -> str:
    return tag.rsplit('}', 1)[-1]


def _character_strings(elem: ET.Element) -> Iterable[str]:
    for child in elem.iter():
        if _local_name(child.tag) == "CharacterString" and child.text:
            yield child.text.strip()


def extract_dist_input_granules(root: ET.Element | str | bytes) -> set[str]:
    if isinstance(root, (str, bytes)):
        root = ET.fromstring(root)

    # Look for PostRtcOperaIds in CharacterString elements anywhere in the XML
    found_marker = False
    for elem in root.iter():
        if _local_name(elem.tag) == "CharacterString" and elem.text:
            text = elem.text.strip()
            if text == "PostRtcOperaIds":
                found_marker = True
            elif found_marker and "," in text and "RTC-S1" in text:
                # Found the RTC IDs after the marker
                return {item.strip() for item in text.split(",") if item.strip()}
            elif "," in text and "RTC-S1" in text:
                # Direct RTC ID list found (no marker)
                return {item.strip() for item in text.split(",") if item.strip()}
    return set()
