import datetime as dt
import boto3
import argparse


RS_BUCKET = 'opera-pst-rs-pop1'


def format_prefix(hls_id):
    '''
    convert hls file prefix to DSWx prefix
    '''

    tile = hls_id[8:14]
    year = int(hls_id[15:19])
    doy = int(hls_id[19:22])
    time_of_day = hls_id[22:29]

    # convert doy to date/month
    date = dt.datetime(int(year), 1, 1) + dt.timedelta(int(doy)-1)
    date_str = date.strftime('%Y%m%d') + time_of_day
    
    # filename subject to change during cal/val
    return f'products/OPERA_L3_DSWx-HLS_{tile}_{date_str}Z_'   


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='convert HLS granules to s3 DSWx prefixes')
    parser.add_argument('file')
    args = parser.parse_args()

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(RS_BUCKET)
    
    with open(args.file, 'r') as f:
        
        prods = set()
        for hls in f:
            prefix = format_prefix(hls)

            for obj in bucket.objects.filter(Prefix=prefix):
                prod = obj.key.split('/')[1] # just want prefix, not individual files
                prods.add(prod)

    for prod in prods:
        print(f's3://opera-pst-rs-pop1/products/{prod}/')
