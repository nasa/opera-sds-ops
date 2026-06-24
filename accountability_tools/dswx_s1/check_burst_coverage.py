import json
import requests
import backoff
import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime, timedelta
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)


CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4'

CCID_RTC = 'C2777436413-ASF'

RTC_PATTERN = re.compile(r'OPERA_L2_RTC-S1_(?P<burst_id>\w{4}-\w{6}-\w{3})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                         r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+')

MGRS_TILE_DB = 'MGRS_tile_collection_v0.3.sqlite'

COVERAGE_THRESHOLD = 4

QUERY = """
    SELECT bursts
    FROM mgrs_burst_db
    WHERE mgrs_set_id = ?
"""


def _fatal_code(err: Exception) -> bool:
    if isinstance(err, requests.exceptions.RequestException) and err.response is not None:
        return err.response.status_code not in [401, 418, 429, 500, 502, 503, 504]
    return False


def _backoff_logger(details):
    logger.warning(
        f"Backing off {details['target']} function for {details['wait']:0.1f} "
        f"seconds after {details['tries']} tries."
    )
    logger.warning(f"Total time elapsed: {details['elapsed']:0.1f} seconds.")


@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=_fatal_code,
                      on_backoff=_backoff_logger,
                      interval=15)
def _do_cmr_query(url, params):
    logger.debug(f'Querying {url} with params {params}')
    response = requests.get(url, params=params)
    response.raise_for_status()
    response_json = response.json()

    response_items = response_json['items']

    if len(response_items) > 0:
        logger.debug(f'Most recent granule retrieved: {response_items[-1]["umm"]["GranuleUR"]}')

    return [i['umm']['GranuleUR'] for i in response_items]


def _db_init(thread_local):
    thread_local.conn = sqlite3.connect(MGRS_TILE_DB)
    logger.debug(f'Connected to DB @ {MGRS_TILE_DB}')


def _query_for_rtcs_from_native_id(native_id, burst_ids):
    # 1. Build list of native IDs
    # 2. Build temporal range
    # 3. Submit query
    # 4. Dedupe results
    params = {
        'collection_concept_id': CCID_RTC,
        'page_size': 2000,
        'options[native-id][pattern]': 'true'
    }

    native_id_list = [
        f'OPERA_L2_RTC-S1_{burst_id}_*' for burst_id in burst_ids
    ]

    params['native-id'] = native_id_list

    match = RTC_PATTERN.fullmatch(native_id)

    if not match:
        raise ValueError(f'Invalid native ID: {native_id}')

    acquisition_ts = match.group('acquisition_ts')
    acquisition_dt = datetime.strptime(acquisition_ts, '%Y%m%dT%H%M%SZ')
    temporal_start_dt = acquisition_dt - timedelta(hours=1)
    temporal_end_dt = acquisition_dt + timedelta(hours=1)
    temporal_start = temporal_start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    temporal_end = temporal_end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    params['temporal[]'] = f'{temporal_start},{temporal_end}'

    matching_rtcs = _do_cmr_query(CMR_URL, params)

    logger.info(f'Found {len(matching_rtcs)} matching RTCs in CMR, deduping...')

    uniq_rtcs = set()

    for rtc_id in matching_rtcs:
        match = RTC_PATTERN.fullmatch(rtc_id)

        if not match:
            raise ValueError(f'Invalid RTC ID: {rtc_id}')

        uniq_groups = (
            match.group('burst_id'),
            match.group('acquisition_ts'),
            match.group('sensor'),
        )

        uniq_rtcs.add(uniq_groups)

    return len(uniq_rtcs)


def _tile_set_has_sufficient_coverage(tile_set_id_cyc_sensor, identified_rtcs, thread_local):
    tile_set_id = tile_set_id_cyc_sensor.split('$')[0]

    if len(identified_rtcs) >= COVERAGE_THRESHOLD:
        logger.info(f'Tile set {tile_set_id_cyc_sensor} already has sufficient coverage from missing RTCs')
        return True, tile_set_id_cyc_sensor, len(identified_rtcs)

    logger.info(f'Querying CMR to check absolute coverage for tile set {tile_set_id_cyc_sensor}')

    conn: sqlite3.Connection = thread_local.conn

    cursor = conn.cursor()
    cursor.execute(QUERY, (tile_set_id,))

    row = cursor.fetchone()

    logger.debug(f'Result for DB query for {tile_set_id}: {row}')

    burst_ids = json.loads(row[0].replace("'", '"'))
    burst_ids = [bid.replace('_', '-').upper() for bid in burst_ids]

    coverage = _query_for_rtcs_from_native_id(identified_rtcs[0], burst_ids)

    return coverage >= COVERAGE_THRESHOLD, tile_set_id_cyc_sensor, coverage


def _reduce_to_common(tile_set_mapping):
    rtc_mapping = {}
    reduced_mapping = {}

    # Map RTCs to tile sets containing those RTCs
    for tile_set in tile_set_mapping:
        for rtc in tile_set_mapping[tile_set]:
            if rtc not in rtc_mapping:
                rtc_mapping[rtc] = []
            rtc_mapping[rtc].append(tile_set)

    # Loop until all RTC mappings have been reduced
    while len(rtc_mapping) > 0:
        # Take the RTC mapped to the most tile sets from the initial mapping and add it to the reduced mapping
        top_key = sorted(rtc_mapping.items(), key=lambda x: len(x[1]), reverse=True)[0][0]
        top_tile_sets = rtc_mapping.pop(top_key)
        reduced_mapping[top_key] = top_tile_sets

        # For all remaining RTC mappings, remove all tile sets that were matched above,
        # deleting the mapping if none remain
        for rtc in list(rtc_mapping.keys()):
            for tile_set in top_tile_sets:
                try:
                    rtc_mapping[rtc].remove(tile_set)
                except ValueError:
                    ...

            if len(rtc_mapping[rtc]) == 0:
                del rtc_mapping[rtc]

    return reduced_mapping


def main():
    thread_local = threading.local()

    with open('missing_mgrs_set_cycle_indices.json') as fp:
        missing = json.load(fp)

    logger.info(f'Loaded {len(missing):,} missing mgrs set cycles')

    dropped = []
    valid = []

    with ThreadPoolExecutor(initializer=_db_init, initargs=(thread_local,)) as executor:
        futures = []

        with tqdm(total=len(missing), leave=False) as pbar:
            for k, v in missing.items():
                futures.append(executor.submit(_tile_set_has_sufficient_coverage, k, v, thread_local))

            for future in as_completed(futures):
                is_valid, tile_set_id, coverage = future.result()

                if is_valid:
                    valid.append((tile_set_id, coverage))
                else:
                    dropped.append((tile_set_id, coverage))
                pbar.update()

    logger.info(f'Dropped {len(dropped):,} missing mgrs set cycles ({len(valid):,} valid sets remaining)')

    report = {
        'valid': {
            'count': len(valid),
            'tile_sets': [
                {
                    ts_id: {
                        'coverage': coverage,
                        'native-id': missing[ts_id][0]
                    }
                } for ts_id, coverage in valid
            ]
        },
        'dropped': {
            'count': len(dropped),
            'tile_sets': [
                {
                    ts_id: {
                        'coverage': coverage,
                        'native-id': missing[ts_id][0]
                    }
                } for ts_id, coverage in dropped
            ]
        },
    }

    coverage_outfile = 'missing_mgrs_sets_by_coverage.json'
    with open(coverage_outfile, 'w') as fp:
        json.dump(report, fp, indent=2)
    logger.info(f'Wrote coverage report to {coverage_outfile}')

    logger.info('Reducing valid tile sets by common RTCs')
    valid_common_mapping = _reduce_to_common({k[0]: missing[k[0]] for k in valid})

    reduced_outfile = 'missing_rtc_mgrs_set_mappings_with_sufficient_coverage_reduced.json'
    with open(reduced_outfile, 'w') as fp:
        json.dump(valid_common_mapping, fp, indent=2)
    logger.info(f'Wrote reduced mappings to {reduced_outfile}')


if __name__ == '__main__':
    with logging_redirect_tqdm():
        main()
