import argparse
import json
import logging
import sys
from datetime import datetime

from elasticsearch import Elasticsearch

# Set up configuration
logging_level = logging.INFO
log_folder = sys.argv[0]

# Set up command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--host", help="Elasticsearch host and port", required=True)
parser.add_argument("--index", help="Elasticsearch index name", required=True)
parser.add_argument("--query_file", help="Path to the JSON file containing the Elasticsearch query", required=True)
args = parser.parse_args()

# Set up logging to a rolling file, within a new logs sub-folder
logging.basicConfig(
    filename=f"{log_folder}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    level=logging_level,
    format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler(log_folder+"/rolling.log", mode="a")]
)

# Connect to Elasticsearch
es = Elasticsearch([args.host])

# Load the query from the JSON file
with open(args.query_file, "r") as f:
    query = json.load(f)

# Execute the query on Elasticsearch with specified index
res = es.search(index=args.index, body=query)

# Log the detailed results
logging.info(res)

# Print the document IDs that were affected by the query
doc_ids = [hit["_id"] for hit in res["hits"]["hits"]]
print("Affected document IDs:", doc_ids)
