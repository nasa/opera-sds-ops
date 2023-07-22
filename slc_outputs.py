import os
import argparse
import elasticsearch
from dotenv import dotenv_values
from elasticsearch import RequestsHttpConnection


config = {
    **dotenv_values("./.env"),
    **os.environ
}


kwargs = {
    "http_auth": (config["ES_USER"], config["ES_PASSWORD"]),
    "connection_class": RequestsHttpConnection,
    "use_ssl": True,
    "verify_certs": False,
    "ssl_show_warn": False,
}


es = elasticsearch.Elasticsearch(hosts=[config["ES_BASE_URL"]], **kwargs)


def get_body(slc) -> dict:
    return {
        "query": {
            "bool": {
                "must": [{"match": {
                    "metadata.input_granule_id.keyword": slc
                }}],
                "must_not": [],
                "should": []
            }
        },
        "from": 0,
        "size": 500,
        "sort": [],
        "aggs": {}
    }


# TODO
def gt_timestamp(doc, ts):
    '''
    parse filename in output filename and
        - return True if product is older than given timestamp
        - return False otherwise
    '''
    pass


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='get output products from list of SLC granules')
    parser.add_argument('file')
    parser.add_argument('-s', 
                        '-start-time', 
                        required=False, 
                        help='filter by processing start time')
    parser.add_argument('-v', 
                        '--verbose',
                        required=False,
                        action='store_true',
                        help='verbose mode')
    parser.add_argument('--missing-only',
                        required=False,
                        default=False,
                        action='store_true',
                        help='only output SLC inputs with no downstream products')
    args = parser.parse_args()

    with open(args.file, 'r') as f:

        docs = []
        for granule in f.readlines():
            
            granule = granule.strip('\n')
            body = get_body(granule)

            if args.verbose:
                print(f'\n>>> {granule}: ')

            # indices should be parameterized 
            cslc_res = es.search(index='grq_v0.1_l2_cslc_s1', body=body, size=500)
            # rtc_res = es.search(index='grq_v0.1_l2_rtc_s1', body=body, size=500)
            # res = cslc_res['hits']['hits'] + rtc_res['hits']['hits']
            res = cslc_res['hits']['hits']

            # print SLC if no products asssociated with it and missing-only flag set
            if args.missing_only:
                if len(res) == 0:
                    print(granule)
                continue
            
            docs.extend(res)

            for hit in docs:
                print(hit['_id'])


