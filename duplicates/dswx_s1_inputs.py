"""Input-based duplicate detection for OPERA DSWx-S1, called from duplicate_check.py.

Name-based checks group DSWx-S1 granules by (tile, acquisition timestamp, sensor). That misses most
duplicates, because the acquisition timestamp in a DSWx-S1 granule ID is derived from its RTC-S1 inputs
and shifts when the product is rebuilt from a different input set. This module compares the inputs
themselves.

Two granules are duplicates if they are over the same MGRS tile, in the same acquisition pass, and built
from overlapping RTC-S1 bursts:

  overlap (default)    same tile and sensor, coverage starts chained within --pass-tolerance-seconds, and
                       burst sets that intersect. Sets are the connected components of "shares at least
                       one burst", so a product triggered before all of its inputs arrived is found.
  exact                same tile, exact temporal extent, and equal burst sets.

A burst is a unique (RTC burst ID, RTC acquisition time, RTC sensor); the RTC production time and file
suffix are dropped, so reprocessed RTCs of the same burst count once.

Keeper rule: latest production time by default (`--keeper latest`), or most bursts then latest
(`--keeper most-bursts`). Each other member is compared with the keeper:

  same_bursts   the keeper was built from the same bursts                 -> listed for removal
  subset        the keeper's bursts include all of this member's          -> listed for removal
  not_covered   this member has bursts the keeper lacks                   -> reported, never listed

So removal never loses coverage, whichever keeper rule is used. Members are also listed only if they
were generated inside --generated-after / --generated-before (default: any time).

Work is cached under --cache-dir so long sweeps resume after an interruption:

  inventories/<window>/inventory.csv   cheap CMR CSV listing for the coverage window. Re-swept on every
                                       run unless --reuse-inventory; an interrupted sweep resumes.
  signatures.jsonl                     RTC burst set per granule UR, fetched from UMM for pass candidates
                                       only. A granule's inputs never change, so this is reused forever.

Coverage windows are safe to split: every member of a duplicate set covers the same pass, so CMR's
temporal intersection returns them together. Revision-date windows are not (a newly revised keeper's
older duplicates fall outside the window), so they are refused.
"""

import csv
import json
import logging
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from hashlib import sha1
from pathlib import Path

import backoff
import requests

logger = logging.getLogger(__name__)

CMR_BASE_URLS = {
    'PROD': 'https://cmr.earthdata.nasa.gov/search',
    'UAT': 'https://cmr.uat.earthdata.nasa.gov/search',
}

DSWX_S1_PATTERN = re.compile(r'(?P<id>OPERA_L3_DSWx-S1_(?P<tile_id>T[^\W_]{5})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                             r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+)')

# Not anchored at the end: input entries carry a file suffix (.h5, _VV.tif, _mask.tif, ...) to ignore.
RTC_S1_PATTERN = re.compile(r'^OPERA_L2_RTC-S1_(?P<burst_id>\w{4}-\w{6}-\w{3})_'
                            r'(?P<acquisition_ts>\d{8}T\d{6}Z)_(?P<creation_ts>\d{8}T\d{6}Z)_'
                            r'(?P<sensor>S1[A-D])_30_v\d+[.]\d+')

CMR_TIME_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'
GRANULE_TIME_FMT = '%Y%m%dT%H%M%SZ'
QUERY_TIME_FMT = '%Y-%m-%dT%H:%M:%SZ'

CRITERIA_OVERLAP = 'overlap'
CRITERIA_EXACT = 'exact'
CRITERIA = [CRITERIA_OVERLAP, CRITERIA_EXACT]

KEEPER_LATEST = 'latest'
KEEPER_MOST_BURSTS = 'most-bursts'
KEEPER_RULES = [KEEPER_LATEST, KEEPER_MOST_BURSTS]

KEEPER_RULE_DESCRIPTIONS = {
    KEEPER_LATEST: 'latest production time, then granule UR',
    KEEPER_MOST_BURSTS: 'most RTC bursts, then latest production time, then granule UR',
}

RELATION_SAME = 'same_bursts'
RELATION_SUBSET = 'subset'
RELATION_NOT_COVERED = 'not_covered'
REMOVABLE_RELATIONS = {RELATION_SAME, RELATION_SUBSET}

# Measured over the full collection (cache_20261109): consecutive coverage starts within one pass are at
# most 24 s apart (high latitude) and separate passes at least 5,884 s apart (adjacent orbits at high
# latitude), with nothing in between. Over-wide grouping is harmless - different passes share no bursts -
# so this leaves headroom on the side that matters.
DEFAULT_PASS_TOLERANCE_SECONDS = 60

DEFAULT_CACHE_DIR = 'dswx_s1_cache'

INVENTORY_FIELDS = ['granule_ur', 'tile_id', 'sensor', 'creation_ts', 'time_start', 'time_end']
INVENTORY_FILENAME = 'inventory.csv'
INVENTORY_STATE_FILENAME = 'inventory.state.json'
SIGNATURES_FILENAME = 'signatures.jsonl'

BURST_KEY_SEP = '|'
CSV_PAGE_SIZE = 2000
UMM_BATCH_SIZE = 200
UMM_WORKERS = 6
NON_RTC_SAMPLE_LIMIT = 20

# Every signatures.jsonl line is json.dumps({'granule_ur': ..., ...}), so the UR can be read without
# parsing the line - which matters when a month's run filters a multi-GB cache.
_SIGNATURE_LINE_PREFIX = '{"granule_ur": "'


# ---------------------------------------------------------------------------------------------------------
# CMR access
# ---------------------------------------------------------------------------------------------------------

def _fatal_code(err: Exception) -> bool:
    if isinstance(err, requests.exceptions.RequestException) and err.response is not None:
        return err.response.status_code not in [401, 418, 429, 500, 502, 503, 504]
    return False


def _backoff_logger(details):
    logger.warning(f"Backing off {details['target']} function for {details['wait']:0.1f} "
                   f"seconds after {details['tries']} tries.")
    logger.warning(f"Total time elapsed: {details['elapsed']:0.1f} seconds.")


_retry = backoff.on_exception(backoff.constant, requests.exceptions.RequestException, max_time=300,
                              giveup=_fatal_code, on_backoff=_backoff_logger, interval=15)


@_retry
def _get_csv_page(base_url, params, search_after=None):
    """One page of the CSV granule listing. Returns (rows, next search-after token)."""
    headers = {'CMR-Search-After': search_after} if search_after else {}
    response = requests.get(f'{base_url}/granules.csv', params=params, headers=headers, timeout=300)
    response.raise_for_status()

    return list(csv.DictReader(response.text.splitlines())), response.headers.get('CMR-Search-After')


@_retry
def _post_umm_batch(base_url, ccid, granule_urs):
    """UMM for a list of granule URs, POSTed because the list is too long for a query string."""
    body = [('collection_concept_id', ccid), ('page_size', str(len(granule_urs)))]
    body.extend(('granule_ur[]', granule_ur) for granule_ur in granule_urs)

    response = requests.post(f'{base_url}/granules.umm_json', data=body, timeout=300)
    response.raise_for_status()

    return response.json().get('items', [])


def _temporal_param(start, end):
    return f"{start.strftime(QUERY_TIME_FMT) if start else ''},{end.strftime(QUERY_TIME_FMT) if end else ''}"


# ---------------------------------------------------------------------------------------------------------
# Stage 1: inventory
# ---------------------------------------------------------------------------------------------------------

def inventory_dir(cache_dir, start, end):
    """One inventory per coverage window, so runs over different windows never mix."""
    def label(t):
        return t.strftime(GRANULE_TIME_FMT) if t else 'open'

    return Path(cache_dir) / 'inventories' / f'{label(start)}_{label(end)}'


def sweep_inventory(base_url, ccid, start, end, cache_dir, reuse=False):
    """Sweep the CSV listing for the coverage window into the cache. Returns the inventory path.

    An interrupted sweep always resumes. A complete one is reused only with `reuse`, because a window
    reaching into the present keeps gaining granules.
    """
    directory = inventory_dir(cache_dir, start, end)
    directory.mkdir(parents=True, exist_ok=True)
    inventory_path = directory / INVENTORY_FILENAME
    state_path = directory / INVENTORY_STATE_FILENAME

    params = {'collection_concept_id': ccid, 'page_size': CSV_PAGE_SIZE}

    if start is not None or end is not None:
        params['temporal[]'] = _temporal_param(start, end)

    search_after = None
    rows_written = 0
    state = json.loads(state_path.read_text()) if state_path.exists() else None

    if state and state.get('complete') and reuse:
        logger.info(f'Reusing complete inventory of {state["rows"]:,} granules at {inventory_path}')
        return inventory_path

    if state and not state.get('complete'):
        search_after = state['search_after']
        rows_written = state['rows']
        _truncate_inventory(inventory_path, rows_written)
        logger.info(f'Resuming inventory sweep from row {rows_written:,}')
    else:
        with open(inventory_path, 'w', newline='') as f:
            csv.writer(f).writerow(INVENTORY_FIELDS)

    pages = 0

    while True:
        rows, next_search_after = _get_csv_page(base_url, params, search_after)

        if rows:
            with open(inventory_path, 'a', newline='') as f:
                writer = csv.writer(f)

                for row in rows:
                    granule_ur = row['Granule UR']
                    match = DSWX_S1_PATTERN.match(granule_ur)

                    if match is None:
                        raise RuntimeError(f'Failed to parse granule UR {granule_ur} with pattern '
                                           f'{DSWX_S1_PATTERN.pattern}')

                    writer.writerow([granule_ur, match['tile_id'], match['sensor'], match['creation_ts'],
                                     row['Start Time'], row['End Time']])

            rows_written += len(rows)

        pages += 1
        search_after = next_search_after
        complete = search_after is None or not rows

        # Written after the rows, so an interrupt in between leaves extra rows that a resume rewinds
        state_path.write_text(json.dumps({'search_after': search_after, 'rows': rows_written,
                                          'complete': complete}))
        logger.info(f'Inventoried {rows_written:,} granules over {pages} page(s)')

        if complete:
            break

    return inventory_path


def _truncate_inventory(inventory_path, rows):
    """Rewrite the inventory keeping its header plus the first `rows` data rows."""
    if not inventory_path.exists():
        raise RuntimeError(f'{inventory_path} is missing but a resume state file exists; delete '
                           f'{inventory_path.with_name(INVENTORY_STATE_FILENAME)} to start the sweep over')

    with open(inventory_path, newline='') as f:
        lines = f.readlines()

    if len(lines) - 1 != rows:
        logger.info(f'Rewinding inventory from {len(lines) - 1:,} to {rows:,} rows')

        with open(inventory_path, 'w', newline='') as f:
            f.writelines(lines[:rows + 1])


def read_inventory(inventory_path):
    with open(inventory_path, newline='') as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------------------------------------
# Candidate grouping (criteria 1 and 2)
# ---------------------------------------------------------------------------------------------------------

def find_candidates(granules, criteria=CRITERIA_OVERLAP, pass_tolerance_seconds=DEFAULT_PASS_TOLERANCE_SECONDS):
    """Groups of more than one granule sharing a tile and pass; only these need the UMM fetch."""
    if criteria == CRITERIA_EXACT:
        groups = _group_by_exact_extent(granules)
    else:
        groups = _group_by_pass(granules, pass_tolerance_seconds)

    candidates = [members for members in groups if len(members) > 1]

    logger.info(f'{len(groups):,} {criteria} candidate groups, {len(candidates):,} with more than one '
                f'granule, covering {sum(len(m) for m in candidates):,} granules')

    return candidates


def _group_by_exact_extent(granules):
    groups = defaultdict(list)

    for granule in granules:
        groups[(granule['tile_id'], granule['time_start'], granule['time_end'])].append(granule)

    return list(groups.values())


def _group_by_pass(granules, tolerance_seconds):
    """Same tile and sensor, coverage starts chained within the tolerance (each within it of the last)."""
    tolerance = timedelta(seconds=tolerance_seconds)
    ordered = sorted(((g['tile_id'], g['sensor'], parse_cmr_time(g['time_start']), g) for g in granules),
                     key=lambda item: item[:3])

    groups = []
    cluster = []
    previous = None

    for tile_id, sensor, start, granule in ordered:
        if not (cluster and (tile_id, sensor) == previous[:2] and start - previous[2] <= tolerance):
            cluster = []
            groups.append(cluster)

        cluster.append(granule)
        previous = (tile_id, sensor, start)

    return groups


def parse_cmr_time(timestamp):
    """CMR timestamp to datetime, to the second. Slicing is much faster than strptime at 5M granules."""
    return datetime(int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10]),
                    int(timestamp[11:13]), int(timestamp[14:16]), int(timestamp[17:19]))


# ---------------------------------------------------------------------------------------------------------
# Stage 2: signatures
# ---------------------------------------------------------------------------------------------------------

def rtc_input_signature(input_granules):
    """Reduce an InputGranules list to its unique RTC bursts.

    Returns (burst set hash, raw list hash, sorted burst keys, non-RTC entries). The raw hash tells a
    re-run of the same job apart from a rebuild from different RTC production runs of the same bursts.
    """
    burst_keys = set()
    non_rtc = []

    for entry in input_granules:
        match = RTC_S1_PATTERN.match(entry)

        if match is None:
            non_rtc.append(entry)
        else:
            burst_keys.add(BURST_KEY_SEP.join((match['burst_id'], match['acquisition_ts'], match['sensor'])))

    sorted_keys = sorted(burst_keys)

    return (sha1('\n'.join(sorted_keys).encode()).hexdigest(),
            sha1('\n'.join(sorted(input_granules)).encode()).hexdigest(),
            sorted_keys,
            non_rtc)


def _cached_granule_ur(line):
    if line.startswith(_SIGNATURE_LINE_PREFIX):
        return line[len(_SIGNATURE_LINE_PREFIX):line.index('"', len(_SIGNATURE_LINE_PREFIX))]

    return json.loads(line)['granule_ur']


def fetch_signatures(base_url, ccid, candidates, cache_dir):
    """Fetch and cache RTC burst sets for candidate granules that aren't cached yet."""
    signatures_path = Path(cache_dir) / SIGNATURES_FILENAME
    expected = {g['granule_ur']: g for members in candidates for g in members}
    cached = set()

    if signatures_path.exists():
        with open(signatures_path) as f:
            for line in f:
                if line.strip():
                    granule_ur = _cached_granule_ur(line)

                    # Records from before burst keys were cached can't be intersected; re-fetch them
                    if granule_ur in expected and ('"bursts":' in line or '"missing": true' in line):
                        cached.add(granule_ur)

    pending = sorted(set(expected) - cached)
    logger.info(f'{len(cached):,} candidate signatures cached, {len(pending):,} to fetch')

    if not pending:
        return

    batches = [pending[i:i + UMM_BATCH_SIZE] for i in range(0, len(pending), UMM_BATCH_SIZE)]
    fetched = mismatches = 0

    with open(signatures_path, 'a') as out, ThreadPoolExecutor(max_workers=UMM_WORKERS) as pool:
        results = pool.map(lambda batch: _post_umm_batch(base_url, ccid, batch), batches)

        for batch, items in zip(batches, results):
            returned = set()

            for item in items:
                umm = item['umm']
                granule_ur = umm['GranuleUR']
                returned.add(granule_ur)
                inputs = umm.get('InputGranules', [])
                signature, raw_signature, burst_keys, non_rtc = rtc_input_signature(inputs)

                if not _umm_agrees_with_inventory(umm, expected[granule_ur]):
                    mismatches += 1

                out.write(json.dumps({
                    'granule_ur': granule_ur,
                    'n_input_entries': len(inputs),
                    'n_bursts': len(burst_keys),
                    'burst_sig': signature,
                    'raw_input_sig': raw_signature,
                    'bursts': burst_keys,
                    'non_rtc_inputs': non_rtc[:NON_RTC_SAMPLE_LIMIT],
                    'n_non_rtc_inputs': len(non_rtc),
                }) + '\n')

            # Deleted between the sweep and now: record the miss rather than retrying it forever
            for granule_ur in set(batch) - returned:
                logger.warning(f'CMR did not return UMM for {granule_ur}')
                out.write(json.dumps({'granule_ur': granule_ur, 'missing': True}) + '\n')

            out.flush()
            fetched += len(batch)
            logger.info(f'Fetched {fetched:,}/{len(pending):,} candidate signatures')

    if mismatches:
        logger.warning(f'{mismatches:,} granule(s) had UMM metadata disagreeing with the CSV inventory')


def _umm_agrees_with_inventory(umm, granule):
    agrees = True
    tile_ids = [a['Values'][0] for a in umm.get('AdditionalAttributes', [])
                if a['Name'] == 'MGRS_TILE_ID' and a.get('Values')]

    if tile_ids and tile_ids[0] != granule['tile_id']:
        logger.warning(f"{granule['granule_ur']}: MGRS_TILE_ID {tile_ids[0]} does not match the granule ID")
        agrees = False

    temporal = umm.get('TemporalExtent', {}).get('RangeDateTime', {})
    umm_range = (temporal.get('BeginningDateTime'), temporal.get('EndingDateTime'))

    if umm_range != (granule['time_start'], granule['time_end']):
        logger.warning(f"{granule['granule_ur']}: UMM temporal extent {umm_range} does not match the CSV "
                       f"inventory ({granule['time_start']}, {granule['time_end']})")
        agrees = False

    return agrees


def read_signatures(cache_dir, wanted):
    """Cached signatures for the wanted granule URs. Returns (records, missing from CMR, lacking bursts).

    Later lines supersede earlier ones. Burst keys are interned to ints (`burst_ids`): identity is all
    the comparison needs, and the full collection holds ~68M burst references.
    """
    signatures_path = Path(cache_dir) / SIGNATURES_FILENAME
    by_granule = {}
    missing = set()
    without_bursts = set()
    interned = {}

    if not signatures_path.exists():
        return by_granule, missing, without_bursts

    with open(signatures_path) as f:
        for line in f:
            if not line.strip() or _cached_granule_ur(line) not in wanted:
                continue

            record = json.loads(line)
            granule_ur = record['granule_ur']
            by_granule.pop(granule_ur, None)
            missing.discard(granule_ur)
            without_bursts.discard(granule_ur)

            if record.get('missing'):
                missing.add(granule_ur)
                continue

            keys = record.pop('bursts', None)

            if keys is None:
                without_bursts.add(granule_ur)
                continue

            record['burst_ids'] = frozenset(interned.setdefault(k, len(interned)) for k in keys)
            by_granule[granule_ur] = record

    return by_granule, missing, without_bursts


# ---------------------------------------------------------------------------------------------------------
# Stage 3: confirm duplicate sets (criterion 3), choose keepers, classify members
# ---------------------------------------------------------------------------------------------------------

def keeper_sort_key(keeper_rule):
    if keeper_rule == KEEPER_MOST_BURSTS:
        return lambda g: (g['n_bursts'], g['creation_ts'], g['granule_ur'])

    return lambda g: (g['creation_ts'], g['granule_ur'])


def confirm_sets(candidates, signature_map, criteria, keeper_rule):
    """Duplicate sets among the candidates, each ordered keeper first. Also returns uncached URs."""
    split = split_by_shared_bursts if criteria == CRITERIA_OVERLAP else split_by_equal_bursts
    sort_key = keeper_sort_key(keeper_rule)
    confirmed = []
    uncached = set()

    for members in candidates:
        records = []

        for granule in members:
            record = signature_map.get(granule['granule_ur'])

            if record is None:
                uncached.add(granule['granule_ur'])
            else:
                records.append({**granule, **record})

        for group in split(records):
            if len(group) > 1:
                group.sort(key=sort_key, reverse=True)
                confirmed.append(group)

    return confirmed, uncached


def split_by_equal_bursts(records):
    by_signature = defaultdict(list)

    for record in records:
        by_signature[record['burst_sig']].append(record)

    return list(by_signature.values())


def split_by_shared_bursts(records):
    """Connected components of "shares at least one burst" (overlap alone isn't transitive)."""
    parent = list(range(len(records)))

    def find(index):
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]

        return index

    first_seen = {}

    for index, record in enumerate(records):
        for burst_id in record['burst_ids']:
            if burst_id in first_seen:
                left, right = find(first_seen[burst_id]), find(index)

                if left != right:
                    parent[right] = left
            else:
                first_seen[burst_id] = index

    components = defaultdict(list)

    for index, record in enumerate(records):
        components[find(index)].append(record)

    return list(components.values())


def relation_to_keeper(member, keeper):
    if member['burst_ids'] == keeper['burst_ids']:
        return RELATION_SAME

    if member['burst_ids'] < keeper['burst_ids']:
        return RELATION_SUBSET

    return RELATION_NOT_COVERED


def duplicate_kind(group):
    """How the set arose. Descriptive only, never used to decide removal.

      identical_inputs   same burst sets, identical raw input lists: the same job ran again
      rtc_relisting      same burst sets, different input counts: reprocessed RTCs listed alongside originals
      rtc_revision       same burst sets, same count, different contents: rebuilt from another RTC run
      incomplete_inputs  burst sets form a subset chain: triggered before all inputs arrived
      divergent_inputs   burst sets overlap with no chain
    """
    if len({m['burst_sig'] for m in group}) > 1:
        ordered = sorted((m['burst_ids'] for m in group), key=len)

        if all(smaller <= larger for smaller, larger in zip(ordered, ordered[1:])):
            return 'incomplete_inputs'

        return 'divergent_inputs'

    raw_sigs = {m.get('raw_input_sig') for m in group}

    if None in raw_sigs:
        return 'unknown'  # signature cached before raw-list hashes were recorded

    if len(raw_sigs) == 1:
        return 'identical_inputs'

    return 'rtc_relisting' if len({m['n_input_entries'] for m in group}) > 1 else 'rtc_revision'


def in_generation_window(granule, generated_after, generated_before):
    creation = datetime.strptime(granule['creation_ts'], GRANULE_TIME_FMT)

    return ((generated_after is None or creation >= generated_after) and
            (generated_before is None or creation < generated_before))


def set_record(group, keeper_rule, generated_after=None, generated_before=None):
    """One confirmed duplicate set as reported. `group` is ordered keeper first."""
    keeper = group[0]
    other_rule = KEEPER_MOST_BURSTS if keeper_rule == KEEPER_LATEST else KEEPER_LATEST
    other_keeper = max(group, key=keeper_sort_key(other_rule))
    members = []

    for g in group:
        is_keeper = g is keeper
        in_window = in_generation_window(g, generated_after, generated_before)
        relation = None if is_keeper else relation_to_keeper(g, keeper)

        members.append({
            'granule_ur': g['granule_ur'],
            'creation_ts': g['creation_ts'],
            'time_start': g['time_start'],
            'time_end': g['time_end'],
            'n_input_entries': g['n_input_entries'],
            'n_bursts': g['n_bursts'],
            'is_keeper': is_keeper,
            'relation_to_keeper': relation,
            'in_generation_window': in_window,
            'eligible_for_deletion': not is_keeper and in_window and relation in REMOVABLE_RELATIONS,
        })

    starts = [g['time_start'] for g in group]
    set_id = (keeper['tile_id'], min(starts),
              sha1('\n'.join(sorted(g['granule_ur'] for g in group)).encode()).hexdigest()[:8])

    return {
        'set_id': repr(set_id),
        'tile_id': keeper['tile_id'],
        'sensor': keeper['sensor'],
        'beginning_date_time': min(starts),
        'ending_date_time': max(g['time_end'] for g in group),
        'duplicate_kind': duplicate_kind(group),
        'keeper': keeper['granule_ur'],
        'keeper_n_bursts': keeper['n_bursts'],
        'n_bursts_min': min(g['n_bursts'] for g in group),
        'n_bursts_max': max(g['n_bursts'] for g in group),
        # The other keeper rule would keep a different product
        'keeper_disagreement': other_keeper is not keeper,
        'duplicate_products': [m['granule_ur'] for m in members if m['eligible_for_deletion']],
        'not_covered_by_keeper': [m['granule_ur'] for m in members if m['relation_to_keeper'] == RELATION_NOT_COVERED],
        'outside_generation_window': [m['granule_ur'] for m in members
                                      if not m['is_keeper'] and not m['in_generation_window']
                                      and m['relation_to_keeper'] in REMOVABLE_RELATIONS],
        'members': members,
    }


# ---------------------------------------------------------------------------------------------------------
# Report, in duplicate_check.py's schema
# ---------------------------------------------------------------------------------------------------------

def criteria_description(criteria, pass_tolerance_seconds):
    if criteria == CRITERIA_EXACT:
        return ('same tile ID, same temporal extent (exact match), and the same set of unique '
                '(RTC burst ID, RTC acquisition time, RTC sensor) inputs')

    return (f'same tile ID, same acquisition pass (same sensor, coverage starts chained within '
            f'{pass_tolerance_seconds}s), and RTC burst sets that intersect - sets are the connected '
            f'components of "shares at least one unique (RTC burst ID, RTC acquisition time, RTC sensor)"')


def companion_path(path, suffix):
    path = Path(path)

    return path.with_name(f'{path.stem}{suffix}')


def _empty_facet():
    return {'n_granules': 0, 'n_duplicates': 0, 'percent_duplicates': -1.0, 'duplicates': {}}


def build_report(args, ccid, granules, candidates, records, missing, uncached, without_bursts, n_non_rtc):
    """The report dict plus the flat removal and not-covered lists."""
    months = defaultdict(_empty_facet)
    dates = defaultdict(_empty_facet)

    for granule in granules:
        months[granule['time_start'][:7]]['n_granules'] += 1
        dates[granule['time_start'][:10]]['n_granules'] += 1

    removal = []
    not_covered = []
    kinds = Counter()
    relations = Counter()
    set_sizes = []
    n_copies = n_disagreements = n_outside_window = 0
    max_spread = timedelta(0)

    for record in records:
        removable = record['duplicate_products']
        removal.extend(removable)
        not_covered.extend(record['not_covered_by_keeper'])
        n_outside_window += len(record['outside_generation_window'])
        n_copies += len(record['members']) - 1
        n_disagreements += record['keeper_disagreement']
        kinds[record['duplicate_kind']] += 1
        relations.update(m['relation_to_keeper'] for m in record['members'] if not m['is_keeper'])

        starts = [parse_cmr_time(m['time_start']) for m in record['members']]
        max_spread = max(max_spread, max(starts) - min(starts))

        if removable:
            set_sizes.append(len(removable))

        # `latest_product` is kept for consumers of the name-based schema; it is the keeper, which under
        # --keeper most-bursts is not always the latest-produced member.
        entry = {
            'latest_product': record['keeper'],
            'duplicate_products': removable,
            'keeper': record['keeper'],
            'duplicate_kind': record['duplicate_kind'],
            'keeper_n_bursts': record['keeper_n_bursts'],
            'keeper_disagreement': record['keeper_disagreement'],
            'not_covered_by_keeper': record['not_covered_by_keeper'],
            'outside_generation_window': record['outside_generation_window'],
        }

        for facet in (months[record['beginning_date_time'][:7]], dates[record['beginning_date_time'][:10]]):
            facet['n_duplicates'] += len(removable)
            facet['duplicates'][record['set_id']] = entry

    for facet in (*months.values(), *dates.values()):
        facet['percent_duplicates'] = facet['n_duplicates'] / facet['n_granules'] * 100 if facet['n_granules'] else 0.0

    n_granules = len(granules)

    summary = {
        'product': args.product,
        'venue': args.venue,
        'ccid': ccid,
        'detection': 'rtc_inputs',
        'criteria_name': args.criteria,
        'criteria': criteria_description(args.criteria, args.pass_tolerance_seconds),
        'pass_tolerance_seconds': args.pass_tolerance_seconds if args.criteria == CRITERIA_OVERLAP else None,
        'keeper_rule': KEEPER_RULE_DESCRIPTIONS[args.keeper],
        'removal_rule': 'non-keepers whose RTC bursts the keeper covers (same or subset), generated inside '
                        'the generation window',
        'coverage_window': _temporal_param(args.start_date, args.end_date),
        'generation_window': _temporal_param(args.generated_after, args.generated_before),
        'n_granules': n_granules,
        'n_duplicates': len(removal),
        'percent_duplicates': len(removal) / n_granules * 100 if n_granules else 0.0,
        'min_duplicates_per_granule': min(set_sizes) if set_sizes else None,
        'max_duplicates_per_granule': max(set_sizes) if set_sizes else None,
        'avg_duplicates_per_granule': sum(set_sizes) / len(set_sizes) if set_sizes else None,
        'n_candidate_groups': len(candidates),
        'n_duplicate_sets': len(set_sizes),
        'n_duplicate_sets_all_generations': len(records),
        'n_duplicate_copies_all_generations': n_copies,
        'n_not_covered_by_keeper': len(not_covered),
        'n_duplicates_outside_generation_window': n_outside_window,
        'relations_to_keeper': dict(sorted(relations.items())),
        'duplicate_kinds': dict(sorted(kinds.items())),
        'n_sets_with_keeper_disagreement': n_disagreements,
        'max_coverage_start_spread_within_a_set': str(max_spread),
        'n_non_rtc_input_entries': n_non_rtc,
        'n_granules_missing_from_cmr': len(missing),
        'n_candidates_without_signature': len(uncached),
        'n_candidates_without_burst_keys': len(without_bursts),
    }

    report = {'summary': summary}

    if args.facet in {'months', 'both'}:
        report['months'] = dict(sorted(months.items()))
    if args.facet in {'dates', 'both'}:
        report['dates'] = dict(sorted(dates.items()))

    return report, removal, not_covered


def analyze(args, ccid, granules):
    """Confirm duplicate sets for an inventory from cached signatures. No CMR queries.

    Returns (report, removal list, not-covered list, set records)."""
    candidates = find_candidates(granules, args.criteria, args.pass_tolerance_seconds)
    wanted = {g['granule_ur'] for members in candidates for g in members}
    signature_map, missing, without_bursts = read_signatures(args.cache_dir, wanted)
    confirmed, uncached = confirm_sets(candidates, signature_map, args.criteria, args.keeper)
    uncached -= missing | without_bursts

    if uncached:
        logger.warning(f'{len(uncached):,} candidate(s) have no cached signature; the report undercounts')

    if without_bursts:
        logger.warning(f'{len(without_bursts):,} cached signature(s) lack burst keys; the report undercounts')

    records = [set_record(group, args.keeper, args.generated_after, args.generated_before)
               for group in sorted(confirmed, key=lambda g: (g[0]['tile_id'], g[0]['time_start'], g[0]['granule_ur']))]
    n_non_rtc = sum(r.get('n_non_rtc_inputs', 0) for r in signature_map.values())
    report, removal, not_covered = build_report(args, ccid, granules, candidates, records, missing, uncached,
                                                without_bursts, n_non_rtc)

    return report, removal, not_covered, records


def check(args, ccid):
    """Entry point from duplicate_check.py: sweep, fetch signatures, analyze, write outputs."""
    start_time = datetime.now()

    if not args.use_temporal:
        raise RuntimeError('DSWX_S1 input-based detection needs a coverage (temporal) window: a revision-date '
                           'window misses the older duplicates of a newly revised product. Drop '
                           '--use-revision, or pass --name-only for the name-based check.')

    base_url = CMR_BASE_URLS[args.venue]
    Path(args.cache_dir).mkdir(parents=True, exist_ok=True)

    inventory_path = sweep_inventory(base_url, ccid, args.start_date, args.end_date, args.cache_dir,
                                     reuse=args.reuse_inventory)
    granules = read_inventory(inventory_path)
    logger.info(f'Found {len(granules):,} granules')

    if not granules:
        logger.info('No data found!')
        return

    fetch_signatures(base_url, ccid, find_candidates(granules, args.criteria, args.pass_tolerance_seconds),
                     args.cache_dir)

    report, removal, not_covered, records = analyze(args, ccid, granules)
    report['summary']['report_run_time'] = str(datetime.now() - start_time)
    write_outputs(args, report, removal, not_covered, records)


def write_outputs(args, report, removal, not_covered, records):
    sets_path = companion_path(args.output, '.sets.jsonl')
    not_covered_path = companion_path(args.duplicate_list, '.not_covered.txt')

    with open(args.output, 'w') as f:
        json.dump(report, f, indent=2)

    Path(args.duplicate_list).write_text(''.join(f'{g}\n' for g in removal))
    not_covered_path.write_text(''.join(f'{g}\n' for g in not_covered))

    with open(sets_path, 'w') as f:
        f.writelines(json.dumps(r) + '\n' for r in records)

    s = report['summary']
    logger.info(f"Criteria {s['criteria_name']}, keeper rule: {s['keeper_rule']}")
    logger.info(f"Found {s['n_duplicates']:,} removable duplicates in {s['n_duplicate_sets']:,} sets out of "
                f"{s['n_granules']:,} granules ({s['percent_duplicates']:.2f}%)")
    logger.info(f"Relations to keeper: {s['relations_to_keeper']}")

    if s['n_not_covered_by_keeper']:
        logger.warning(f"{s['n_not_covered_by_keeper']:,} duplicate(s) hold bursts their keeper lacks and are NOT "
                       f"listed for removal; see {not_covered_path}")

    logger.info(f'Wrote JSON report to {args.output}')
    logger.info(f'Wrote {len(removal):,} granule IDs for removal to {args.duplicate_list}')
    logger.info(f'Wrote all {len(records):,} duplicate sets to {sets_path}')
