import csv
from collections import defaultdict

# Parse csv file


def map_slc_granules_to_bursts(csv_file):
    '''Parse csv that looks like this and map using default dict with value stored as sets:
    t001_000010_iw1,"2017-02-03 18:00:33.938144",S1A_IW_SLC__1SDV_20170203T180033_20170203T180101_015123_018BA2_8CB7
    t001_000010_iw2,"2017-02-03 18:00:34.879588",S1A_IW_SLC__1SDV_20170203T180033_20170203T180101_015123_018BA2_8CB7'''

    # the file is located:
    # https://github.com/nasa/opera-sds/blob/main/processing_request_datasets/static_layers/rtc_query_bursts_2016-05-01_to_2023-09.csv
    csv_file = open(csv_file, 'r')
    csv_reader = csv.reader(csv_file, delimiter=',')

    slc_granules_to_bursts = defaultdict(set)
    for row in csv_reader:
        slc_granules_to_bursts[row[2]].add(row[0])

    return slc_granules_to_bursts

m = map_slc_granules_to_bursts("rtc_query_bursts_2016-05-01_to_2023-09.csv")

count = 0

# Iterate through the dictionary and print the key and value pairs
for key, value in m.items():
    if (len(value) != 27):
        print(key, len(value))
        count += 1
    if key == "S1B_IW_SLC__1SDV_20180920T164800_20180920T164829_012801_017A1E_06B8":
        print(value)

print("There are", count, "SLC granules that do not have 27 bursts.")
