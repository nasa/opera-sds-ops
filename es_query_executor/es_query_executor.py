import argparse
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import sys
from datetime import datetime

from elasticsearch import Elasticsearch

# Set up logging configuration
logging_level = logging.INFO
logging_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logging_file = 'es_query_executor.log'
logging_num_backup_files = 14 # means keep 14 days of logs

# Set up command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--host", help="Elasticsearch host and port", required=True)
parser.add_argument("--index", help="Elasticsearch index name", required=True)
parser.add_argument("--query_file", help="Path to the JSON file containing the Elasticsearch query", required=True)
parser.add_argument("--action", help="Action to invoke for query, i.e. 'search' or 'delete'", required=True)
args = parser.parse_args()

# Set up logging to a rolling file, within a new logs sub-folder
logging_handler = TimedRotatingFileHandler(logging_file, when='midnight', backupCount=logging_num_backup_files)
logging_handler.setFormatter(logging_formatter)
logger = logging.getLogger()
logger.addHandler(logging_handler)
logger.setLevel(logging_level)

# Connect to Elasticsearch
es = Elasticsearch([args.host])

# Load the query from the JSON file
with open(args.query_file, "r") as f:
    query = json.load(f)

# Execute the query on Elasticsearch with specified index per action requested
logging.info("Executing [" + args.action + "] with query file [" + args.query_file + "]")
if (args.action == "search"):
    res = es.search(index=args.index, body=query)

    # Log the detailed results
    logging.debug(res)

    # Log and print the confirmation info
    confirm_message = "Found documents: " + str(res['hits']['total']['value'])
    print(confirm_message)
    logging.info(confirm_message)
elif (args.action == "delete"):
    res = es.delete_by_query(index=args.index, body=query)

    # Log the detailed results
    logging.debug(res)

    # Log and print the confirmation info
    confirm_message = "Affected documents: " + str(res['deleted'])
    print(confirm_message)
    logging.info(confirm_message)
else:
    print("Invalid --action value [" + args.action + "]. Choose from 'search' or 'delete'")
    logging.critical("Invalid --action value [" + args.action + "]. Choose from 'search' or 'delete'")
    sys.exit(1)




