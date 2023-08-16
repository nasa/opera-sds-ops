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


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='get output products from list of SLC granules')
    parser.add_argument('file')
    args = parser.parse_args()

    with open(args.file, 'r') as f:
        for granule in f.readlines():
            granule = granule.strip('\n')
            body = get_body(granule)
            # print(f'>>> {granule}: ')
            # cslc_res = es.search(index='grq_v0.0_l2_cslc_s1', body=body, size=500)
            rtc_res = es.search(index='grq_v0.4_l2_rtc_s1', body=body, size=500)
            # for hit in cslc_res['hits']['hits']:
            #     print(hit['_id'])
            for hit in rtc_res['hits']['hits']:
                print(hit['_id'])
            # if len(rtc_res['hits']['hits']) == 0:
                # print(granule)
            # print()


