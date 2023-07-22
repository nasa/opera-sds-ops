'''
query LPDAAC for given tile id and temporal extent

usage: python3 granule_by_tile.py -t <tile id> [-s <start time>] [-e <end time>]
     - tile id of form 5 or 6 characters long MGRS tile
     - start time and end time of form YYYY-MM-DD

ex: python3 granules_by_tile.py -t T06WVS -s 2019-01-01 -e 2022-12-31  
'''

import json
import argparse
import requests
from datetime import datetime


EARTHDATA_ENDPOINT = 'https://cmr.earthdata.nasa.gov'


def gen_url(start, end, tile, page_number):
    assert page_number > 0

    url = f'{EARTHDATA_ENDPOINT}/search/granules.umm_json?' + \
           'provider=LPCLOUD' + \
          f'&attribute[]=string,MGRS_TILE_ID,{tile}' + \
          f'&ShortName=HLSL30&ShortName=HLSS30' + \
          f'&temporal[]={start}, {end}' + \
           '&page_size=100' + \
          f'&page_num={page_number}'

    return url


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='get HLS links from LPDAAC')

    parser.add_argument(
        '-t', 
        '--tile', 
        required=True,
        help='HLS tile ID, ex: T06WVS'
    )

    parser.add_argument(
        '-s',
        '--start-time',
        required=False,
        default='2014-01-01',
        help='temporal start time (YYYY-MM-DD)'
    )

    parser.add_argument(
        '-e',
        '--end-time',
        required=False,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='temporal end time (YYYY-MM-DD)'
    )

    args = parser.parse_args()

    tile = args.tile
    if tile[0] == 'T':
        tile = tile[1:]

    # pad to comply with ISO 8601 standard
    start = args.start_time + 'T00:00:00Z'
    end = args.end_time + 'T23:59:59Z'

    page_number = 1
    response = requests.get(gen_url(start, end, tile, page_number)).json()
    granules = [d.get('meta', {}).get('native-id') for d in response['items']] 
    total = response['hits']

    while len(granules) < total:
        page_number += 1
        response = requests.get(gen_url(start, end, tile, page_number)).json()
        granules.extend([d.get('meta', {}).get('native-id') for d in response['items']])
       
    print('\n'.join(granules))
