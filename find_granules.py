import boto3
import os
import argparse
import botocore
from elasticsearch import Elasticsearch
from elasticsearch import helpers
# list of input product example is like this
# HLS.S30.T34HCH.2025024T082139.v2.0

RS_BUCKET = 'opera-int-rs-pop1'

# retrieved from https://wiki.jpl.nasa.gov/pages/viewpage.action?spaceKey=operasds&title=PCM+UIs+and+Hosts+Information
GRQ_IP_DICT = {
    "ops-fwd": "100.104.82.12",
    "ops-pop1": "100.104.82.32",
    "int-fwd": "100.104.49.12",
    "int-pop1": "100.104.49.22",
    "pst": "100.104.62.13"
}


def get_input_granule_type(granule):
    # HLS.S30.T34HCH.2025024T082139.v2.0
    if "HLS" in granule:
        prod_type = granule.split(".")[0] + "_" + granule.split(".")[1]
    elif "SLC" in granule:
        prod_type = granule.split("_")[2]
    return prod_type


def get_prefix(granule):
    '''
    given granule name retrieve s3 prefix
    '''
    par_dir = get_input_granule_type(granule)
    prefix = "inputs/" + par_dir + "/" + granule + "-r1"
    return prefix


def get_typical_gran_file(granule):
    '''
    given a granule get a typical file of that type
    so we can s3 it
    '''
    if "SLC" in granule:
        return granule + ".zip"
    if "HLS.S30" in granule or "HLS.L30" in granule:
        return granule + "-r1.context.json"


def check_granule_s3(granule, prefix, bucket="opera-int-rs-pop1"):
    '''
    given granule check if in s3
    '''
    s3 = boto3.resource('s3')
    exist_flag = False

    gran_file = get_typical_gran_file(granule)
    gran_s3_path = prefix + "/" + gran_file
    input_bucket = bucket
    try:
        s3.Object(input_bucket, gran_s3_path).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            print(gran_s3_path, "doesn't exist")
            exist_flag = False
        else:
            # Something else has gone wrong.
            raise
    else:
        exist_flag = True
        # print(gran_s3_path + " found in s3")

    return exist_flag


def check_granule_grq(granule, host=None, venue=None):
    '''
    use elastic search to search grq
    '''
    exist_flag = False
    if venue:
        host = get_grq_host_from_venue(venue)

    index_name = get_grq_index_from_granule(granule, host=host)
    search_result = is_granule_present_query(index_name, granule, host=host)
    if search_result:
        exist_flag = True

    return exist_flag


def find_missing_granules(granule_list, bucket='opera-int-rs-pop1', output_file=None, host=None, venue=None):
    '''
    given a list of granules create a list of missing and found granules in s3 and grq
    '''
    s3_missing_granule = []
    s3_found_granule = []
    grq_missing_granule = []
    grq_found_granule = []
    both_missing_granule = []
    both_found_granule = []

    gran_list = open(granule_list, "r")
    for granule in gran_list:
        granule = granule.strip()
        # print("checking granule: ", granule)
        # get s3 prefix
        prefix = get_prefix(granule)
        s3_found = check_granule_s3(granule, prefix)

        grq_found = check_granule_grq(granule, venue=venue)

        if grq_found and s3_found:
            both_found_granule.append(granule)
        elif not grq_found and not s3_found:
            both_missing_granule.append(granule)
        elif s3_found:
            s3_found_granule.append(granule)
        elif not s3_found:
            s3_missing_granule.append(granule)
        elif grq_found:
            grq_found_granule.append(granule)
        elif not grq_found:
            grq_missing_granule.append(granule)

    return s3_missing_granule, s3_found_granule, grq_missing_granule, grq_found_granule, both_missing_granule, both_found_granule


def report_found_granules(s3_found_granule, grq_found_granule, both_found_granule, output_file=None):
    '''
    stdout print report of found matching granules from input list
    or output results to txt file
    '''
    print("Found Granule Report")
    print("--------------------")
    print()
    if s3_found_granule:
        print("Granules found in only s3")
        print("-------------------------")
        for granule in s3_found_granule:
            print(granule)
        print()
    if grq_found_granule:
        print("Granules found in only grq")
        print("--------------------------")
        for granule in grq_found_granule:
            print(granule)
        print()
    if both_found_granule:
        print("Granules found in both s3 and grq")
        print("---------------------------------")
        for granule in both_found_granule:
            print(granule)
        print()

    return None


def is_granule_present_query(index_name, granule, host="http://localhost:9200"):
    '''
    index names [
    grq_1_l1_s1_slc-2025.02
    grq_v0.9_l3_disp_s1-2025.01
    grq_v1.0_l2_rtc_s1-2025.02
    grq_v1.0_l3_dswx_hls-2025.01
    grq_v1.0_l3_dswx_hls-2025.02
    grq_v1.1_l2_cslc_s1-2025.02
    grq_v1.2.12_triaged_job
    grq_v2.0_l2_hls_s30-2025.01
    ]
    '''
    query_dict = {
        "query": {
            "bool": {
                "must": [{
                    "match_phrase_prefix": {
                        "metadata.id": granule
                    }}
                ]
            }
        }
    }
    es = Elasticsearch([host])
    # setting scroll and size to 1 just cause we only need to chekc if it appears once
    search_result = list(helpers.scan(es, query_dict, index=index_name, scroll="1m", size=1))
    return search_result


def get_grq_host_from_venue(venue):
    grq_ip = GRQ_IP_DICT[venue]
    host_url = "http://" + grq_ip + ":9200/"
    return host_url


def get_grq_index_from_granule(granule, host="http://localhost:9200"):
    '''
    parse granule to find grq index name among a list of applicable grq indexes
    retrieve all grq index. find
    '''
    es = Elasticsearch([host])
    prod_type = get_input_granule_type(granule).lower()
    index_regex = "grq*" + prod_type + "*"
    index_list = list(es.indices.get_alias(index=index_regex).keys())
    return index_list


def main():
    '''
    return which files are missing given a list
    '''
    parser = argparse.ArgumentParser()
    # Optional key-value arguments:
    parser.add_argument('-i', '--input', help="input list of granules")
    parser.add_argument('-o', '--output_file', default="found_granules_outputs.txt", type=str, help="output file name")
    parser.add_argument('-b', '--bucket', default='opera-int-rs-pop1', help="s3 bucket")
    parser.add_argument('-ho', '--host', help="es host")
    parser.add_argument('-v', '--venue', help="used for grq host if ssl issues with host example: int-pop1")
    args = parser.parse_args()
    s3_missing_granule, s3_found_granule, grq_missing_granule, grq_found_granule, both_missing_granule, both_found_granule = find_missing_granules(args.input, bucket=args.bucket, output_file=args.output_file, host=args.host, venue=args.venue)
    report_found_granules(s3_found_granule, grq_found_granule, both_found_granule)


if __name__ == '__main__':
    main()
