"""SLC annotation XML extraction via HTTP range requests.

Determines the authoritative burst count for a Sentinel-1 IW SLC by
reading the ``<burstList>`` elements in ESA's annotation XML files,
which are embedded inside the SLC ZIP archive hosted at ASF.

Instead of downloading the entire multi-GB ZIP, this module uses HTTP Range
requests to fetch only the ZIP central directory and the annotation XML
entries (typically ~0.6-1.4 MB total).

Ported from Gerald's ``slc_annotation_extract.py`` on PCM OPERA-2518 branch.
"""

from __future__ import annotations

import bisect
import io
import logging
import os
import pathlib
import re
import xml.etree.ElementTree as ET
import zipfile

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Resolve SLC download URL via CMR
# ---------------------------------------------------------------------------

CMR_GRANULE_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"


def get_slc_download_url(slc_native_id: str) -> str:
    """Query CMR for the SLC granule and return its HTTPS download URL."""
    body = f"provider=ASF&native_id={slc_native_id}&page_size=1"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    resp = requests.post(CMR_GRANULE_URL, data=body, headers=headers)
    resp.raise_for_status()
    data = resp.json()

    if data.get("hits", 0) == 0:
        raise ValueError(f"No granule found in CMR for native_id: {slc_native_id}")

    item = data["items"][0]
    related_urls = item["umm"].get("RelatedUrls", [])
    for ru in related_urls:
        url = ru.get("URL", "")
        if url.startswith("https://") and url.endswith(".zip"):
            return url

    # Fallback: any HTTPS URL containing the product name
    product_name = slc_native_id.rsplit("-", 1)[0]  # strip "-SLC" suffix
    for ru in related_urls:
        url = ru.get("URL", "")
        if "https://" in url and product_name in url:
            return url

    raise ValueError(
        f"Could not find HTTPS download URL in CMR response for {slc_native_id}. "
        f"RelatedUrls: {[ru.get('URL') for ru in related_urls]}"
    )


# ---------------------------------------------------------------------------
# 2. EarthData Login authentication
# ---------------------------------------------------------------------------

def _parse_netrc(machine: str) -> tuple[str, str] | None:
    """Parse ~/.netrc for *machine*, tolerating malformed macdef blocks."""
    netrc_path = pathlib.Path.home() / ".netrc"
    if not netrc_path.exists():
        return None
    text = netrc_path.read_text()
    pattern = rf"machine\s+{re.escape(machine)}\s+login\s+(\S+)\s+password\s+(\S+)"
    m = re.search(pattern, text)
    if m:
        return m.group(1), m.group(2)
    return None


def get_edl_token(edl_endpoint: str = "urs.earthdata.nasa.gov") -> str:
    """Obtain an EDL Bearer token using ~/.netrc credentials or EARTHDATA_TOKEN env var."""
    # Check environment variable first (opera-audit convention)
    env_token = os.environ.get("EARTHDATA_TOKEN")
    if env_token:
        return env_token

    from requests.auth import HTTPBasicAuth

    creds = _parse_netrc(edl_endpoint)
    if creds is None:
        raise RuntimeError(
            f"No entry for {edl_endpoint} in ~/.netrc and EARTHDATA_TOKEN not set. "
            f"Add: machine {edl_endpoint} login <user> password <pass>"
        )
    username, password = creds
    auth = HTTPBasicAuth(username, password)

    # Check for existing tokens
    resp = requests.get(f"https://{edl_endpoint}/api/users/tokens", auth=auth)
    resp.raise_for_status()
    tokens = resp.json()
    if tokens:
        return tokens[0]["access_token"]

    # Create a new token
    resp = requests.post(f"https://{edl_endpoint}/api/users/token", auth=auth)
    resp.raise_for_status()
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# 3. HTTP Range-request file-like wrapper for remote ZIP reading
# ---------------------------------------------------------------------------

class HTTPRangeFile:
    """File-like object backed by HTTP Range requests.

    Implements read/seek/tell so that ``zipfile.ZipFile`` can open a
    remote ZIP without downloading the entire file.
    """

    def __init__(self, url: str, token: str):
        self.url = url
        self._pos = 0
        self._request_count = 0
        self._bytes_downloaded = 0

        # ASF datapool URLs redirect:
        #   datapool.asf → 307 → sentinel1.asf → 302 → EDL → sentinel1 → CloudFront
        # The final CloudFront URL is a signed URL (query-string auth).
        self._resolved_url = self._resolve_redirect(url, token)
        self._range_session = requests.Session()

        # Discover file size via a small Range GET (CloudFront may reject HEAD).
        probe = self._range_session.get(
            self._resolved_url, headers={"Range": "bytes=-1"}
        )
        if probe.status_code == 206:
            cr = probe.headers.get("Content-Range", "")
            if "/" in cr:
                self._size = int(cr.rsplit("/", 1)[1])
            else:
                raise IOError(f"Unexpected Content-Range header: {cr}")
        elif probe.status_code == 200:
            self._size = int(probe.headers["Content-Length"])
            logger.warning(
                "Server ignored Range header; reads will download full content"
            )
        else:
            raise IOError(f"Probe request failed with HTTP {probe.status_code}")

        self._request_count += 1
        self._bytes_downloaded += len(probe.content)
        logger.debug("Remote ZIP: %s bytes (%.1f GB)", self._size, self._size / 1e9)

    @staticmethod
    def _resolve_redirect(url: str, token: str) -> str:
        """Follow ASF → EDL → CloudFront redirect chain."""
        # Step 1: get first redirect
        r1 = requests.get(url, allow_redirects=False)
        if r1.status_code not in (301, 302, 303, 307, 308):
            raise IOError(f"Expected redirect from {url}, got HTTP {r1.status_code}")
        location = r1.headers["Location"]
        logger.debug("Redirect step 1: %s -> %s", url, location)

        # Step 2: follow with Bearer token (handles EDL OAuth + CloudFront)
        headers = {"Authorization": f"Bearer {token}"}
        r2 = requests.get(location, headers=headers, allow_redirects=True, stream=True)
        r2.close()
        r2.raise_for_status()
        logger.debug("Resolved URL: %s", r2.url)
        return r2.url

    @property
    def size(self) -> int:
        return self._size

    # -- file-like interface ------------------------------------------------

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size < 0:
            size = self._size - self._pos

        start = self._pos
        end = min(start + size - 1, self._size - 1)
        if start > end:
            return b""

        headers = {"Range": f"bytes={start}-{end}"}
        resp = self._range_session.get(self._resolved_url, headers=headers)

        if resp.status_code == 200:
            raise IOError(
                "Server returned 200 instead of 206 Partial Content. "
                "Range requests may not be supported for this URL."
            )
        if resp.status_code != 206:
            raise IOError(
                f"HTTP {resp.status_code} for Range bytes={start}-{end}"
            )

        data = resp.content
        self._pos += len(data)
        self._request_count += 1
        self._bytes_downloaded += len(data)
        return data


# ---------------------------------------------------------------------------
# 4. Extract annotation XMLs from remote ZIP
# ---------------------------------------------------------------------------

def extract_annotations(zip_url: str, token: str) -> dict[str, bytes]:
    """Extract annotation XML files from a remote SLC ZIP via range requests.

    Returns a dict mapping ZIP entry path → XML bytes.
    """
    remote = HTTPRangeFile(zip_url, token)

    annotations: dict[str, bytes] = {}
    with zipfile.ZipFile(remote) as zf:
        for entry in zf.namelist():
            if (
                "/annotation/" in entry
                and entry.endswith(".xml")
                and "/calibration/" not in entry
            ):
                logger.debug("Extracting: %s", entry)
                annotations[entry] = zf.read(entry)

    logger.info(
        "Extracted %d annotation XMLs (%d HTTP requests, %.1f KB downloaded)",
        len(annotations),
        remote._request_count,
        remote._bytes_downloaded / 1024,
    )
    return annotations


# ---------------------------------------------------------------------------
# 5. Parse burst information from annotation XML
# ---------------------------------------------------------------------------

def parse_burst_count(xml_bytes: bytes) -> int:
    """Return burst count from the <burstList count="N"> element."""
    root = ET.fromstring(xml_bytes)
    burst_list = root.find(".//{*}burstList")
    if burst_list is None:
        burst_list = root.find(".//burstList")
    if burst_list is not None:
        return int(burst_list.get("count", "0"))
    return 0


def parse_burst_anx_times(annotations: dict[str, bytes]) -> dict[str, list[float]]:
    """Parse azimuthAnxTime for each burst, keyed by subswath.

    Only processes one polarization per subswath (VV preferred) since burst
    structure is identical across polarizations.

    Returns e.g. {"IW1": [2126.26, 2129.02, 2131.78, 2134.53], ...}
    """
    # First pass: collect available (subswath, polarization) pairs
    entries: dict[str, dict[str, str]] = {}  # subswath -> {pol -> path}
    for path in sorted(annotations):
        filename = path.rsplit("/", 1)[-1]
        parts = filename.split("-")
        subswath = parts[1].upper()
        polarization = parts[3].upper()
        entries.setdefault(subswath, {})[polarization] = path

    # Second pass: parse one polarization per subswath (prefer VV)
    result: dict[str, list[float]] = {}
    for subswath in sorted(entries):
        pols = entries[subswath]
        chosen_path = pols.get("VV") or next(iter(pols.values()))
        xml_bytes = annotations[chosen_path]
        root = ET.fromstring(xml_bytes)

        burst_list = root.find(".//{*}burstList")
        if burst_list is None:
            burst_list = root.find(".//burstList")
        if burst_list is None:
            continue

        anx_times = []
        for burst_el in burst_list:
            if burst_el.tag.endswith("burst") or burst_el.tag == "burst":
                anx_el = burst_el.find("{*}azimuthAnxTime")
                if anx_el is None:
                    anx_el = burst_el.find("azimuthAnxTime")
                if anx_el is not None and anx_el.text:
                    anx_times.append(float(anx_el.text))
        if anx_times:
            result[subswath] = anx_times

    return result


def derive_burst_ids(
    anx_times: dict[str, list[float]],
    track: int,
    reference_burst_num: int,
    reference_anx_time: float,
    reference_subswath: str,
) -> list[str]:
    """Derive complete burst IDs from annotation ANX times + one known ASF burst.

    Algorithm:
    1. For the reference subswath, find the burst index whose ANX time is
       closest to reference_anx_time.  That burst's burst_num is known
       (reference_burst_num), and all other burst_nums in the subswath
       follow by consecutive indexing.
    2. For each non-reference subswath, find the same-row burst using the
       IW1 -> IW2 -> IW3 firing order (subswaths fire in this order within
       each burst row, so earlier subswaths have smaller ANX times).
       That burst shares the reference's burst_num, and the rest of the
       subswath again follows by consecutive indexing.

    Returns list of ASF-format burst IDs (e.g. ["173_370215_IW1", ...]).
    """
    # Validate: need at least 2 bursts in reference subswath
    ref_times = anx_times.get(reference_subswath)
    if not ref_times or len(ref_times) < 2:
        raise ValueError(
            f"Need at least 2 bursts in {reference_subswath} to compute T_cycle, "
            f"got {len(ref_times) if ref_times else 0}"
        )
    if ref_times[1] - ref_times[0] <= 0:
        raise ValueError(
            f"Invalid T_cycle={ref_times[1] - ref_times[0]} "
            f"from {reference_subswath} ANX times"
        )

    # Derive burst_nums per subswath using consecutive indexing from an
    # anchor burst whose burst_num is known.
    burst_ids = []
    for subswath in sorted(anx_times):
        sw_times = anx_times[subswath]

        if subswath == reference_subswath:
            # Anchor = the reference burst itself
            anchor_idx = min(
                range(len(sw_times)),
                key=lambda i: abs(sw_times[i] - reference_anx_time),
            )
            anchor_num = reference_burst_num
        else:
            # Find the same-row burst in this subswath.  Within each burst
            # row the subswaths fire in order IW1 -> IW2 -> IW3, so
            # subswaths earlier in that order have smaller ANX times.
            if subswath < reference_subswath:
                # Fires before the reference — same-row burst is just
                # before reference_anx_time.
                idx = bisect.bisect_right(sw_times, reference_anx_time) - 1
                if idx < 0:
                    idx = 0
                    row_delta = 1
                else:
                    row_delta = 0
            else:
                # Fires after the reference — same-row burst is just
                # after reference_anx_time.
                idx = bisect.bisect_left(sw_times, reference_anx_time)
                if idx >= len(sw_times):
                    idx = len(sw_times) - 1
                    row_delta = -1
                else:
                    row_delta = 0

            anchor_idx = idx
            anchor_num = reference_burst_num + row_delta

        # Generate consecutive burst_nums from the anchor
        for i in range(len(sw_times)):
            burst_num = anchor_num + (i - anchor_idx)
            burst_ids.append(f"{track:03d}_{burst_num:06d}_{subswath}")

    return burst_ids


def analyze_annotations(annotations: dict[str, bytes]) -> list[dict]:
    """Parse all annotation XMLs and return per-subswath burst info."""
    results = []
    for path in sorted(annotations):
        filename = path.rsplit("/", 1)[-1]
        parts = filename.split("-")
        subswath = parts[1].upper()
        polarization = parts[3].upper()
        burst_count = parse_burst_count(annotations[path])
        results.append({
            "subswath": subswath,
            "polarization": polarization,
            "burst_count": burst_count,
            "filename": filename,
        })
    return results
