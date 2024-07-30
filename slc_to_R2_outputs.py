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


def check_slc(slc) -> dict:
    return {
        "query": {
            "bool": {
                "must": [{"match": {
                    "metadata.id.keyword": slc
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


RTC_INDEX = 'grq_v1.0_l2_rtc_s1-*'
CSLC_INDEX = 'grq_v1.0_l2_cslc_s1-2024*'
SLC_INDEX = 'grq_1_l1_s1_slc-2024*'


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='get output products from list of SLC granules')
    parser.add_argument('file')
    parser.add_argument('product_type', choices=['rtc', 'cslc', 'slc'])
    parser.add_argument('--missing', default=False, action='store_true')
    args = parser.parse_args()

    if args.product_type == 'cslc':
        INDEX = CSLC_INDEX
        LOOKUP = get_body
    elif args.product_type == 'rtc':
        INDEX = RTC_INDEX
        LOOKUP = get_body
    else:
        INDEX = SLC_INDEX
        LOOKUP = check_slc

    with open(args.file, 'r') as f:
        
        for granule in f.readlines():
            # remove -SLC if it exists (not in _id in GRQ)
            granule = granule.replace('-SLC', '').strip('\n')

            res = es.search(index=INDEX, body=LOOKUP(granule), size=500)
            
            if args.missing:
                num_hits = len(res['hits']['hits'])
                if num_hits == 0:
                    print(granule)
            else:
                for hit in (res['hits']['hits']):
                    print(hit['_id'])
            


