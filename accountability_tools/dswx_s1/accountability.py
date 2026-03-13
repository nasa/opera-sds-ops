from datetime import datetime
import json
import re
import pickle
import logging
from functools import cache


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)


RTC_PATTERN = re.compile(r'OPERA_L2_RTC-S1_(?P<burst_id>\w{4}-\w{6}-\w{3})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                         r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+')
DSWX_PATTERN = re.compile(r'(?P<id>OPERA_L3_DSWx-S1_(?P<tile_id>T[^\W_]{5})_(?P<acquisition_ts>\d{8}T\d{6}Z)_'
                          r'(?P<creation_ts>\d{8}T\d{6}Z)_(?P<sensor>S1[A-D])_30_v\d+[.]\d+)')

GRANULE_TIME_FMT = '%Y%m%dT%H%M%SZ'

DSWX_S1AB_START_TIME = datetime(2023, 12, 15, 15, 8, 12)
DSWX_S1C_START_TIME = datetime(2025, 5, 20)


@cache
def rtc_to_id_tuple(rtc_id):
    match = RTC_PATTERN.match(rtc_id)
    match_dict = match.groupdict()
    return match_dict['burst_id'], match_dict['acquisition_ts'], match_dict['sensor']


def should_include_rtc(rtc_id):
    _, acquisition_ts, sensor = rtc_to_id_tuple(rtc_id)

    acquisition_ts = datetime.strptime(acquisition_ts, GRANULE_TIME_FMT)

    if sensor in {'S1A', 'S1B'}:
        return acquisition_ts >= DSWX_S1AB_START_TIME
    elif sensor == 'S1C':
        return acquisition_ts >= DSWX_S1C_START_TIME
    else:
        raise NotImplementedError(f'Do not know the DSWx-S1 processing start time for sensor {sensor} ({rtc_id})')


with open('rtc_products.json') as fp:
    rtc_rep = json.load(fp)

logger.info(f'Loaded RTC survey with {len(rtc_rep):,} products')
rtc_rep_filtered = [rtc for rtc in rtc_rep if should_include_rtc(rtc['id'])]
logger.info(f'Filtered RTC products from {len(rtc_rep):,} to {len(rtc_rep_filtered):,}')
rtc_rep = rtc_rep_filtered

with open('dswx_products.json') as fp:
    dswx_rep = json.load(fp)

logger.info(f'Loaded DSWx-S1 survey with {len(dswx_rep):,} products')

rtc_to_dswx_map = {}

logger.info('Mapping DSWx RTC inputs to products')

for dswx in dswx_rep:
    dswx_id = dswx['id']
    for rtc in dswx['input_rtcs']:
        id_tuple = rtc_to_id_tuple(rtc)
        if id_tuple not in rtc_to_dswx_map:
            rtc_to_dswx_map[id_tuple] = []
        rtc_to_dswx_map[id_tuple].append(dswx_id)

logger.info(f'Mapped {len(rtc_to_dswx_map):,} RTCs')

with open('rtc_to_dswx_map.pickle', 'wb') as fp:
    pickle.dump(rtc_to_dswx_map, fp)

logger.info('Saved mapping to "rtc_to_dswx_map.pickle"')

rtc_id_to_latest_map = {}

logger.info('Mapping surveyed RTCs to latest unique products')
for rtc_dict in rtc_rep:
    rtc_id = rtc_dict['id']
    id_tuple = rtc_to_id_tuple(rtc_id)
    rtc_id_to_latest_map[id_tuple] = rtc_id

with open('rtc_id_to_latest_map.pickle', 'wb') as fp:
    pickle.dump(rtc_id_to_latest_map, fp)

logger.info('Saved mapping to "rtc_id_to_latest_map.pickle"')

del rtc_rep, rtc_rep_filtered, dswx_rep

used_rtc_ids_set = set(rtc_to_dswx_map.keys())
avail_rtc_ids_set = set(rtc_id_to_latest_map.keys())

logger.info(f'RTC count used in DSWx: {len(used_rtc_ids_set):,}')
logger.info(f'RTC count from survey:  {len(avail_rtc_ids_set):,}')
logger.info(f'Used % of available:    {(len(used_rtc_ids_set) / len(avail_rtc_ids_set)) * 100:.4f}%')

superset = avail_rtc_ids_set.intersection(used_rtc_ids_set)

if not superset:
    raise ValueError('Survey RTCs are not a superset of used RTCs')

missing_rtc_ids_set = avail_rtc_ids_set - used_rtc_ids_set

logger.info(f'Unused RTC count:       {len(missing_rtc_ids_set):,}')

if len(missing_rtc_ids_set) == 0:
    logger.info('No missing RTCs found! No further action is needed')
    exit(0)

with open('missing_rtc_ids_set.pickle', 'wb') as fp:
    pickle.dump(missing_rtc_ids_set, fp)

logger.info('Saved missing RTC set to "missing_rtc_ids_set.pickle"')

missing_rtc_products = [rtc_id_to_latest_map[rtc_id] for rtc_id in list(missing_rtc_ids_set)]
missing_rtc_products.sort(key=lambda x: rtc_to_id_tuple(x))

with open('missing_rtc_products.json', 'w') as fp:
    json.dump(missing_rtc_products, fp, indent=2)

logger.info('Saved list of missing RTC product IDs to "missing_rtc_products.json"')
logger.info('Finished accountability')
