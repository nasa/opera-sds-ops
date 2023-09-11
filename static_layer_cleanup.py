import boto3
import argparse
from os import path
from typing import Set
import multiprocessing as mp


RS_BUCKET = 'opera-pst-rs-pop1'
PREFIXES = {
    'cslc' : 'products/CSLC_S1/OPERA_L2_CSLC-S1-STATIC_',
    'rtc'  : 'products/RTC_S1/' 
}

s3 = boto3.resource('s3')
bucket = s3.Bucket(RS_BUCKET)


def get_burst_ids(product_type : str, version : str) -> Set[str]:

    burst_ids = set()
    
    for obj in bucket.objects.filter(Prefix=PREFIXES.get(product_type)):


        prod = obj.key.split('/')[2]
        prod_version = prod[71:74]
        burst_id = prod[24:39]

        if prod_version == version:
            burst_ids.add(burst_id)

    return burst_ids


# def _find_latest(product_type : str, version : str, burst_id : str) -> str:

#     prefix = PREFIXES.get(product_type) + burst_id
#     prefix_path = path.dirname(prefix)

#     static_layers = set()

#     for obj in bucket.objects.filter(Prefix=prefix):
#         prod = obj.key.split('/')[2]
#         v = prod[58:61]

#         if v == version and 'static_layers' in prod:
#             static_layers.add(path.join(prefix_path, prod))

#     static_layers = list(static_layers)
#     sort = lambda prod : prod[79:95]
#     static_layers.sort(key=sort)
#     if len(static_layers) == 0:
#         return f'NO STATIC LAYERS FOUND FOR {burst_id}'
#     return static_layers[-1]


def cleanup(product_type : str, version : str, burst_id : str, dryrun : bool) -> None:

    prefix = PREFIXES.get(product_type) + burst_id
    prefix_path = path.dirname(prefix)

    static_layers = set()

    for obj in bucket.objects.filter(Prefix=prefix):
        prod = obj.key.split('/')[2]
        prd_version = prod[71:74]

        if prd_version == version:
            static_layers.add(path.join(prefix_path, prod))

    static_layers = list(static_layers)
    sort = lambda prod : prod[66:81]
    static_layers.sort(key=sort)


    for prefix in static_layers[:-1]:
        if args.dryrun:
            print(f'(dryrun) DELETE: {prefix}')
        else:
            bucket.objects.filter(Prefix=prefix).delete()

    print(f'KEEP: {static_layers[-1]}')
	
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Cleanup redundant static layers')
    parser.add_argument('product_type', 
                        choices=['cslc', 'rtc'], 
                        help='static layer product type')
    parser.add_argument('version', 
                        choices=['0.0', '0.1', '0.2'], 
                        help='static layer version number')
    parser.add_argument('--file',
                        required=False,
                        help='file containing list of burst ids to cleanup')
    parser.add_argument('--dryrun',
                            default=False,
                            action='store_true',
                            help='only output files to be deleted')
    args = parser.parse_args()

    if args.file:
        with open(args.file) as f:
            burst_ids = [b.rstrip('\n') for b in f]
    else:
        burst_ids = get_burst_ids(args.product_type, args.version)

    # # TODO: this step should be parallelized 
    for burst_id in burst_ids:
        cleanup(args.product_type, args.version, burst_id, args.dryrun)