from cmr import GranuleQuery
import logging
import requests
import json
import datetime

logging.basicConfig(level=logging.INFO)

collection = "SENTINEL-1A_SLC"
#collection = "SENTINEL-1B_SLC"
CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json?provider_id=ASF&native_id="

# START_DATETIME = datetime.datetime(2023, 10, 16, 0, 0, 0)
START_DATETIME = datetime.datetime(2023, 11, 1, 0, 0, 0)

# STOP_DATETIME = START_DATETIME + datetime.timedelta(days=1)
NUMBER_OF_DAYS = 10

# loop over days to break the query
granules_dict = {}
start_datetime = START_DATETIME
for i in range(NUMBER_OF_DAYS):
    stop_datetime = start_datetime + datetime.timedelta(days=1)
    logging.debug("Querying from: %s to %s" % (start_datetime, stop_datetime))

    api = GranuleQuery()
    api.short_name(collection)
    api.temporal(start_datetime, stop_datetime)
    logging.info("\n# of granules in CMR: %i" % api.hits())
    granules = api.get_all()
    # granules = api.get(100)

    for granule in granules:
        granule_id = granule['producer_granule_id']
        logging.info("Retrieved granule: %s" % granule_id)
        logging.debug(granule)

        # execute separate request to capture the full metadata
        try:
            url = CMR_URL + granule_id + "-SLC"
            response = requests.get(url)
            response_json = response.json()
            logging.debug(response_json)
            revision = response_json['items'][0]['meta']['revision-id']
            logging.debug("revision=%s" % revision)
            revision_date = response_json['items'][0]['meta']['revision-date']
            logging.debug("revision date=%s" % revision_date)
            granules_dict[granule_id] = {}
            granules_dict[granule_id]['revision'] = revision
            granules_dict[granule_id]['revision_date'] = revision_date

            # json_object = json.loads(json.dumps(response_json))
            # json_formatted_str = json.dumps(json_object, indent=2)

            # retrieve the acquisition date
            for mydict in response_json['items'][0]['umm']['AdditionalAttributes']:
                if mydict['Name'] == 'ACQUISITION_DATE':
                    acquisition_date = mydict['Values'][0]
                    granules_dict[granule_id]['acquisition_date'] = acquisition_date
        except requests.exceptions.RequestException as e:
            logging.error(e)

    # do the next day
    start_datetime = stop_datetime

# write out
with open(collection + "_granules.txt", 'w') as output_file:
    for gid in granules_dict.keys():
        output_file.write(",".join([gid,
                                    granules_dict[gid]['acquisition_date'],
                                    granules_dict[gid]['revision_date'],
                                    str(granules_dict[gid]['revision'])])
                          + "\n")







