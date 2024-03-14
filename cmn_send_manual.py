import argparse
import json
from datetime import datetime

from hysds.es_util import get_mozart_es, get_grq_es
from hysds_commons.job_utils import submit_mozart_job

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_user_rules():
    mozart_es = get_mozart_es()

    user_rules_index = "user_rules-grq"
    results = mozart_es.query(index=user_rules_index)

    user_rules = []
    for result in results:
        user_rules.append(result["_source"])

    return user_rules


def update_user_rule_query(query, _id):
    new_query = query.copy()

    # Ensure 'bool' field exists in query
    if "bool" not in new_query:
        new_query["bool"] = {}

    # Ensure 'must' field exists in 'bool'
    if "must" not in new_query["bool"]:
        new_query["bool"]["must"] = []
 
    new_query["bool"]["must"].append({"match": {"_id": _id}})

    return new_query


def get_user_rule_by_ds(ds_type):
    user_rules = get_user_rules()

    for user_rule in user_rules:
        if "hysds-io-send_notify_msg" in user_rule["job_type"] and user_rule["rule_name"].endswith(ds_type):
            return user_rule

    raise Exception(f"No user rules found for dataset: {ds_type}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A command line tool that submits on-demand cmn send jobs.\n\n"
        "Example invocation:\n"
        "python cmr_send_manual.py --products <product id file> "
                                  "--dataset-types L2_RTC_S1,L2_CSLC_S1 "
                                 "[--override-disabled-user-rules]",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--products",
        help="A file containing a list of GRQ document _id's for the products to be delivered. "
             "The products must be of the dataset type passed as an argument.",
        required=True,
    )
    parser.add_argument(
        "--dataset-type",
        help="GRQ dataset type for products listed in file.",
        required=True,
    )

    args = parser.parse_args()
    logger.info("Product id file: %s", args.products)
    logger.info("Datasets Type: %s", args.dataset_type)

    grq_es = get_grq_es()

    # Get the user rule associated with the dataset cmn send job
    user_rule = get_user_rule_by_ds(args.dataset_type)

    # Prepend the rule name with "manual-" so that we know it was triggered via this script.
    # This value will show up as a tag in the job.
    user_rule["rule_name"] = "manual-" + user_rule["rule_name"]

    job_submission_accountability = dict(
        num_jobs_successfully_submitted=0,
        num_jobs_unsuccessfully_submitted=0,
        datasets_successfully_submitted=[],
        datasets_unsuccessfully_submitted=[],
    )

    with open(args.products) as f:
        products = [p.strip('\n') for p in f.readlines()]

    for product in products:
        query_string = json.loads(user_rule["query_string"])
        updated_query_string = update_user_rule_query(query_string, product)

        index = f"grq_v1.0_{args.dataset_type.lower()}-*"
        results = grq_es.search(index=index, body={"query": updated_query_string})["hits"]["hits"]

        n = len(results)
        # Expect 1 result in GRQ for each product _id
        if n != 1:
            logging.warning(f"{n} documents for {product} found in GRQ")
            continue
        else:
            logging.info(f"Found {product} in GRQ")

        for result in results:
            try:
                job_type = user_rule["job_type"].replace("hysds-io-", "", 1) if user_rule["job_type"].startswith("hysds-io-") else user_rule["job_type"]
                job_type_components = job_type.split(":")
                job_name = f"job-{job_type_components[0]}__{job_type_components[1]}-{result['_id']}"
                mozart_job_id = submit_mozart_job(result, user_rule, component="grq", job_name=job_name)
                logger.info("The job was successfully submitted with Job ID: {}".format(mozart_job_id))
                job_submission_accountability["num_jobs_successfully_submitted"] += 1
                job_submission_accountability["datasets_successfully_submitted"].append(result["_id"])
            except Exception as e:
                logger.error("Failed to submit job: {}".format(e))
                job_submission_accountability["num_jobs_unsuccessfully_submitted"] += 1
                job_submission_accountability["datasets_unsuccessfully_submitted"].append(result["_id"])

logger.info(json.dumps(job_submission_accountability, indent=2, separators=(",", ": ")))
