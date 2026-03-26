import json
import pickle
import sqlite3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger = logging.getLogger(__name__)

MGRS_TILE_DB = 'MGRS_tile_collection_v0.3.sqlite'


mgrs_set_to_rtc_map = {}

with open('missing_rtc_products.json') as fp:
    rtcs = json.load(fp)

logger.info(f'Loaded {len(rtcs):,} missing RTC products')

query = f"""
    SELECT mgrs_set_id, land_ocean_flag
    FROM mgrs_burst_db
    WHERE (
        SELECT 1
        FROM json_each(bursts)
        WHERE value = ?
    )
"""


def _db_init(thread_local):
    thread_local.conn = sqlite3.connect(MGRS_TILE_DB)


def _rtc_to_mgrs_sets(rtc, thread_local):
    conn = thread_local.conn

    cursor = conn.cursor()
    burst_id = rtc.split('_')[3].lower().replace('-', '_')
    cursor.execute(query, (burst_id,))

    mgrs_sets = []
    lofs = []

    for row in cursor.fetchall():
        mgrs_sets.append(row[0])
        lofs.append(row[1])

    # mgrs_sets = [row[0] for row in cursor.fetchall()]
    # lofs = [row[1] for row in cursor.fetchall()]
    return rtc, mgrs_sets, lofs


local = threading.local()

logger.info('Beginning mapping of missing RTCs to MGRS tile set IDs')

dropped_sets = 0

with ThreadPoolExecutor(initializer=_db_init, initargs=(local,)) as pool:
    futures = []

    for rtc in tqdm(rtcs):
        futures.append(pool.submit(_rtc_to_mgrs_sets, rtc, local))

    with tqdm(total=len(futures)) as pbar:
        for future in as_completed(futures):
            rtc, mgrs_sets, lofs = future.result()

            for mgrs_set_id, lof in zip(mgrs_sets, lofs):
                if lof == 'water':
                    dropped_sets += 1
                    continue

                if mgrs_set_id not in mgrs_set_to_rtc_map:
                    mgrs_set_to_rtc_map[mgrs_set_id] = []
                mgrs_set_to_rtc_map[mgrs_set_id].append(rtc)

            pbar.update()

logger.info(f'Finished mapping RTCs to {len(mgrs_set_to_rtc_map):,} MGRS tile set IDs '
            f'(Dropped {dropped_sets:,} sets over water)')

with open('missing_rtcs_to_tile_sets.pickle', 'wb') as fp:
    pickle.dump(mgrs_set_to_rtc_map, fp)

logger.info('Saved MGRS tile set mappings to "missing_rtcs_to_tile_sets.pickle"')
logger.info('Finished MGRS mapping')
