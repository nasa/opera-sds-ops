import argparse
import json
import logging
import re
from datetime import datetime

import backoff
import requests


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)


CMR_URL = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4'

CCID_RTC = 'C2777436413-ASF'
CCID_DSWX = 'C2949811996-POCLOUD'

RTC_PATTERN = re.compile(r'OPERA_L2_RTC-S1_(?P<burst_id>\w{4}-\w{6}-\w{3})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                         r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+')
DSWX_PATTERN = re.compile(r'(?P<id>OPERA_L3_DSWx-S1_(?P<tile_id>T[^\W_]{5})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                          r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+)')


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
def _do_cmr_query(url, params, func=None, headers=None):
    if headers is None:
        headers = {}
    logger.info(f'Querying {url} with params {params} and headers {headers}')
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    response_json = response.json()

    response_items = response_json['items']

    if len(response_items) > 0:
        logger.info(f'Most recent granule retrieved: {response_items[-1]["umm"]["GranuleUR"]}')

    if func is not None:
        response_items = func(response_items)
        if not isinstance(response_items, list):
            raise TypeError(f'Expecting a list, got {type(response_items)}')

    return response_items, response.headers.get('CMR-Search-After', None)


def query_cmr(cmr_url, ccid, start, end, func=None):
    granules = []

    params = {
        'collection_concept_id': ccid,
        'page_size': 2000
    }

    start_q_str = start.strftime('%Y-%m-%dT%H:%M:%SZ') if start is not None else ''
    end_q_str = end.strftime('%Y-%m-%dT%H:%M:%SZ') if end is not None else ''

    if start is not None or end is not None:
        params['temporal[]'] = f'{start_q_str},{end_q_str}'

    query_result, search_after = _do_cmr_query(cmr_url, params, func=func)
    granules.extend(query_result)

    while search_after is not None:
        headers = {'CMR-Search-After': search_after}
        query_result, search_after = _do_cmr_query(cmr_url, params, func=func, headers=headers)
        granules.extend(query_result)

    return granules


def reduce_input_rtc_list(input_files):
    return list(set(
        [g.removesuffix('.h5').removesuffix('.tif').removesuffix('_VH').removesuffix('_HV').removesuffix('_VV')
          .removesuffix('_HH').removesuffix('_mask') for g in input_files]
    ))


def survey(ccid, pattern, start, end, uniq_groups, fields, func):
    grouping_products_map = {}

    granules = query_cmr(CMR_URL, ccid, start, end, func)
    logger.info(f'Found {len(granules):,} granules')

    for granule_tuple in granules:
        granule_dict = {f: v for f, v in zip(fields, granule_tuple)}
        granule_id = granule_tuple[0]

        match = pattern.match(granule_id)

        if match is None:
            raise RuntimeError(f'Failed to parse granule ID {granule_id} with pattern {pattern.pattern}')

        group_dict = match.groupdict()

        id_tuple = tuple([group_dict[grp] for grp in uniq_groups])
        granule_dict['_timestamp'] = group_dict['creation_ts']

        if id_tuple not in grouping_products_map:
            grouping_products_map[id_tuple] = []
        grouping_products_map[id_tuple].append(granule_dict)

    for id_tuple in grouping_products_map:
        grouping_products_map[id_tuple].sort(key=lambda x: x['_timestamp'], reverse=True)
        grouping_products_map[id_tuple] = grouping_products_map[id_tuple][0]
        del grouping_products_map[id_tuple]['_timestamp']

    return list(grouping_products_map.values())


parser = argparse.ArgumentParser()


def _datetime_arg(s):
    return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')


parser.add_argument(
    '-s', '--start-date',
    default=None,
    type=_datetime_arg,
    help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z"
)

parser.add_argument(
    '-e', '--end-date',
    default=None,
    type=_datetime_arg,
    help="The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z"
)

args = parser.parse_args()


logger.info('Starting RTC survey')

rtc_products = survey(
    CCID_RTC,
    RTC_PATTERN,
    args.start_date, args.end_date,
    ('burst_id', 'acquisition_ts', 'sensor'),
    ('id', 'revision_timestamp'),
    lambda x: [
        (
            i['umm']['GranuleUR'],
            i['meta']['revision-date']
        ) for i in x
    ]
)

logger.info('RTC survey finished. Writing to "rtc_products.json"')

with open('rtc_products.json', 'w') as f:
    json.dump(rtc_products, f, indent=2)

logger.info('Starting DSWx-S1 survey')

dswx_products = survey(
    CCID_DSWX,
    DSWX_PATTERN,
    args.start_date, args.end_date,
    ('tile_id', 'acquisition_ts', 'sensor'),
    ('id', 'input_rtcs'),
    lambda x: [
        (
            i['umm']['GranuleUR'],
            reduce_input_rtc_list(i['umm']['InputGranules'])
        ) for i in x
    ]
)

logger.info('DSWx-S1 survey finished. Writing to "dswx_products.json"')

with open('dswx_products.json', 'w') as f:
    json.dump(dswx_products, f, indent=2)

logger.info('Surveys complete')
