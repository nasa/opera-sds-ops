"""
Script to check the OPERA static layers archived in CMR versus the list of
Static Layers that should have been produced (available on GitHub).
The missing static layers and frames are written to separate files in the working directory.

Pre-Requisites:
A Python virtual environment that contains the necessary inputs:
python -m virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt

Example usage:

python StaticLayersValidator.py -h
python StaticLayersValidator.py CSLC-S1 [--debug]
python StaticLayersValidator.py RTC-S1 [--debug]

"""
import requests
import logging
from cmr import GranuleQuery
import re
import argparse

CONFIG_DATA = {
        "CSLC-S1": {
            'collection': 'OPERA_L2_CSLC-S1-STATIC_V1',
            'bursts_file_name': 'cslc_query_bursts_2016-05-01_to_2023-09.csv',
            'frames_file_name': 'cslc_query_frames_2016-05-01_to_2023-09.csv'
        },
        "RTC-S1": {
            'collection': 'OPERA_L2_RTC-S1-STATIC_V1',
            'bursts_file_name': 'rtc_query_bursts_2016-05-01_to_2023-09.csv',
            'frames_file_name': 'rtc_query_frames_2016-05-01_to_2023-09.csv'
        }
}
GITHUB_URL = "https://raw.githubusercontent.com/nasa/opera-sds/main/processing_request_datasets/static_layers/"

class StaticLayersValidator:

    def __init__(self, product):

        self._config = CONFIG_DATA[product]

    def main(self):
        """
        Driver method to execute the validation
        """

        burst_id_to_slc_frame_from_file = self._get_burst_ids_from_file()

        burst_id_to_static_layer = self._get_burst_ids_from_cmr()

        self._identify_duplicate_static_layers(burst_id_to_static_layer)

        self._identify_missing_bursts_and_frames(burst_id_to_slc_frame_from_file, burst_id_to_static_layer)

    def _get_burst_ids_from_file(self) -> dict:
        """
        Method to read the list of static bursts IDs that should have been generated.
        The burst list is retrieved from GitHub.
        """

        # Read the granules from the file
        burst_id_to_slc_frame = {}
        count = 0
        response = requests.get(GITHUB_URL + self._config['bursts_file_name'])
        data = response.text
        for line in data.split('\n'):
            # t175_374393_iw2,"2019-11-14 16:51:07.117769",
            # S1B_IW_SLC__1SDV_20191114T165057_20191114T165116_018926_023B2C_6A5F
            if line:  # avoid blank line at the end
                count += 1
                (burst_id_lc, timestamp, frame) = line.split(",")
                # convert t174_372337_iw1 to T174-372337-IW1
                burst_id = burst_id_lc.upper().replace("_", "-")
                logging.debug([burst_id, timestamp, frame])
                burst_id_to_slc_frame[burst_id] = frame
        logging.info("# of bursts in file: %i" % count)
        logging.info("# of unique bursts in file: %i" % len(burst_id_to_slc_frame))

        return burst_id_to_slc_frame

    def _get_burst_ids_from_cmr(self) -> dict:
        """
        Method to query CMR for all the static layer granules in the given collection,
        and parse the burst ids from them.
        """

        # Count the number of granules in the CMR collection
        api = GranuleQuery()
        api.short_name(self._config['collection'])
        logging.info("# of static layers in CMR: %i" % api.hits())
        granules = api.get_all()
        # granules = api.get(10)
        burst_id_to_static_layer = {}
        for granule in granules:
            # OPERA_L2_CSLC-S1-STATIC_T004-006642-IW3_20140403_S1A_v1.0
            title = granule['title']
            m = re.search(r'OPERA_L2_.+-S1-STATIC_(\w\d{3}-\d{6}-\w\w\d)_\d+.+', title)
            # T004-006642-IW3
            # T159-340132-IW1
            burst_id = m.group(1)
            logging.debug("\t granule: %s burst id: %s" % (title, burst_id))
            if burst_id in burst_id_to_static_layer:
                burst_id_to_static_layer[burst_id].append(title)
            else:
                burst_id_to_static_layer[burst_id] = [title]

        return burst_id_to_static_layer

    @staticmethod
    def _identify_duplicate_static_layers(burst_id_to_static_layer):
        """
        Method to count and identify how many static layers in CMR correspond to the same burst ID.
        These may be due to computing the static layer with input SLC granule from S1A and S1B.
        """

        # Count the number of unique burst ids:
        logging.info("# of unique burst ids in CMR: %d" % len(burst_id_to_static_layer))

        # Identify burst ids in CMR that were produced by more than 1 static layer
        num_duplicates = 0
        for burst_id in burst_id_to_static_layer.keys():
            if len(burst_id_to_static_layer[burst_id]) > 1:
                num_duplicates += 1
                # print some examples
                if num_duplicates <= 5:
                    logging.info("Example of Duplicate Burst ID: %s" % burst_id)
                    for g in burst_id_to_static_layer[burst_id]:
                        logging.info("\tStatic Layer Granule: %s" % g)
        logging.info("# of bursts IDs with duplicate static layers: %d" % num_duplicates)

    def _identify_missing_bursts_and_frames(self, burst_id_to_slc_frame_from_file, burst_id_to_static_layer):
        """
        Method to compare the list of static layers that should have been generated
        to the static layers in CMR and identify any missing bursts and frames.
        The missing bursts and frames are written to local files.
        """

        # Find the static layers we have not produced
        missing_frames = set()
        missing_bursts = set()
        for burst_id in burst_id_to_slc_frame_from_file.keys():
            if burst_id not in burst_id_to_static_layer:
                frame = burst_id_to_slc_frame_from_file[burst_id]
                logging.debug("Missing from CMR: burst id: %s frame: %s:" % (burst_id, frame))
                missing_frames.add(frame)
                missing_bursts.add(burst_id)

        # Print out the missing frames
        logging.info("\nNumber of bursts missing from CMR: %s" % len(missing_bursts))
        logging.info("\nNumber of frames missing from CMR: %s" % len(missing_frames))

        # Write out the missing frames
        with open(self._config['frames_file_name'] + "_missing_frames.txt", 'w') as output_file:
            for frame in missing_frames:
                output_file.write(frame + "\n")

        # Write out the missing bursts
        with open(self._config['bursts_file_name'] + "_missing_bursts.txt", 'w') as output_file:
            for burst in missing_bursts:
                output_file.write(burst + "\n")


if __name__ == "__main__":

    # Initialize parser
    parser = argparse.ArgumentParser()

    # Define arguments
    parser.add_argument("product", type=str,
                        help="OPERA Product", choices=['CSLC-S1', 'RTC-S1'])
    parser.add_argument("-d", "--debug", default=False,
                        action=argparse.BooleanOptionalAction,
                        help='Optional debug flag for extended verbosity')

    # Read arguments from command line
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    validator = StaticLayersValidator(args.product)
    validator.main()
