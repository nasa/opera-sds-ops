<!-- Header block for project -->
<hr>

<div align="center">

<h1 align="center">Elasticsearch Query Executor</h1>

</div>

<pre align="center">A script for executing version-controlled Elasticsearch queries.</pre>

<!-- Header block for project -->

This is a simple script meant to execute an Elasticsearch query specified in an external config file, log the query transaction, and print how many docs were affected by the query.

[![SLIM](https://img.shields.io/badge/Best%20Practices%20from-SLIM-blue)](https://nasa-ammos.github.io/slim/)

## Features

* Contains static set of pre-configured Elasticsearch queries for the HySDS system
* Executes static queries against a specified Elasticsearch instance, either in "search" mode (find docs) or "delete" mode (delete docs)
* Logs the transaction to a log file (daily)
* Prints the number of documents affected by the query
  
<!-- ☝️ Replace with a bullet-point list of your features ☝️ -->

## Contents

- [Features](#features)
- [Contents](#contents)
- [Quick Start](#quick-start)
  - [Requirements](#requirements)
  - [Setup Instructions](#setup-instructions)
  - [Run Instructions](#run-instructions)
  - [Usage Examples](#usage-examples)
- [Frequently Asked Questions (FAQ)](#frequently-asked-questions-faq)
- [License](#license)
- [Support](#support)

## Quick Start

This guide provides a quick way to get started with our project.

### Requirements

* Python 2.7+
* Elasticsearch Python SDK 
* Elasticsearch 7+
  
### Setup Instructions

1. Ensure you have Python 2.7+ installed on your system
2. Install the Elasticsearch Python SDK. See: https://elasticsearch-py.readthedocs.io/en/7.x/ 
3. Ensure your machine has network access to a given Elasticsearch instance you want to execute queries against, without authentication
   
<!-- ☝️ Replace with a numbered list of how to set up your software prior to running ☝️ -->

### Run Instructions

```
python es_query_executor.py --host [HOST] --index [ES_INDEX] --query_file [PATH_TO_QUERY_FILE] --action [search|delete]
```

<!-- ☝️ Replace with a numbered list of your run instructions, including expected results ☝️ -->

### Usage Examples

* Search for old Mozart jobs considered nominal, print the number of docs found and log the results to a log file
  ```
  python es_query_executor.py --host http://localhost:9200 --index job_status-current --query_file queries/mozart_jobs_nominal_old.json --action search
  ```
* Delete / clean-up old Mozart jobs considered nominal, print the number of docs deleted and log the results of the transaction to a log file
  ```
  python es_query_executor.py --host http://localhost:9200 --index job_status-current --query_file queries/mozart_jobs_nominal_old.json --action delete
  ```  

## Frequently Asked Questions (FAQ)

No questions yet. Propose a question to be added here by reaching out to our contributors! See support section below.

## License

See our: [LICENSE](LICENSE)

## Support

Key points of contact are: [@riverma](https://github.com/riverma) and [@niarenaw](https://github.com/niarenaw)