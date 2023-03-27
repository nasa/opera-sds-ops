import argparse
import json
import logging
import sys
from datetime import datetime

from elasticsearch import Elasticsearch

# Set up configuration
logging_level = logging.INFO
log_folder = "es_query_executor"

# Set up command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--host", help="Elasticsearch host and port", required=True)
parser.add_argument("--index", help="Elasticsearch index name", required=True)
parser.add_argument("--query_file", help="Path to the JSON file containing the Elasticsearch query", required=True)
parser.add_argument("--action", help="Action to invoke for query, i.e. 'search' or 'delete'", required=True)
args = parser.parse_args()

# Set up logging to a rolling file, within a new logs sub-folder
logging.basicConfig(
    filename=f"{log_folder}/{datetime.now().strftime('%Y%m%d')}.log",
    level=logging_level,
    format="%(asctime)s %(message)s"
)

# Connect to Elasticsearch
es = Elasticsearch([args.host])

# Load the query from the JSON file
with open(args.query_file, "r") as f:
    query = json.load(f)

# Execute the query on Elasticsearch with specified index per action requested
if (args.action == "search"):
    res = es.search(index=args.index, body=query)

    # Log the detailed results
    logging.info(res)

    print("Affected documents:", res['hits']['total']['value'])
elif (args.action == "delete"):
    res = es.delete_by_query(index=args.index, body=query)

    # Log the detailed results
    logging.info(res)

    print("Affected documents:", res['deleted'])
else:
    print("Invalid --action value. Choose from 'search' or 'delete'")
    sys.exit(1)




