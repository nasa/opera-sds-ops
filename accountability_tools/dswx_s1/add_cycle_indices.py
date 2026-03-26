import pickle
import re
import json
from rtc_utils import determine_acquisition_cycle_for_rtc_granule, rtc_granule_regex
from tqdm import tqdm
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)


with open('missing_rtcs_to_tile_sets.pickle', 'rb') as fp:
    base_map = pickle.load(fp)

logger.info('Loaded MGRS tile set to missing RTC mapping')

expanded_map = {}

logger.info('Remapping RTCs to TileSet$AcquisitionCycle$Sensor')

for tile_set in tqdm(base_map):
    rtc_ids = base_map[tile_set]

    for rtc in rtc_ids:
        acquisition_cycle = determine_acquisition_cycle_for_rtc_granule(rtc)
        sensor = re.match(rtc_granule_regex, rtc).groupdict()['sensor']

        mgrs_set_id__cycle_index_sensor = f'{tile_set}${acquisition_cycle}${sensor}'

        if mgrs_set_id__cycle_index_sensor not in expanded_map:
            expanded_map[mgrs_set_id__cycle_index_sensor] = []
        expanded_map[mgrs_set_id__cycle_index_sensor].append(rtc)

expanded_map = {
    k: list(sorted(expanded_map[k]))
    for k in sorted(
        expanded_map.keys(),
        key=lambda x: (int(x.split('$')[0].split('_')[1]),
                       int(x.split('$')[0].split('_')[2]),
                       int(x.split('$')[1]),
                       x.split('$')[2])
    )
}

logger.info('Remapping completed')

with open('missing_mgrs_set_cycle_indices.pickle', 'wb') as fp:
    pickle.dump(expanded_map, fp)

logger.info('Saved mappinng to "missing_mgrs_set_cycle_indices.pickle"')

with open('missing_mgrs_set_cycle_indices.json', 'w') as fp:
    json.dump(expanded_map, fp, indent=2)

logger.info('Saved mapping to "missing_mgrs_set_cycle_indices.json"')
logger.info('Finished MGRS tile set-aquisition cycle-sensor to missing RTC mappings')
