import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# 2023-11-01T00:18:56.943748Z
dateparser = lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ')

# Read data from file
COLLECTION = "SENTINEL-1A_SLC"
FILENAME = COLLECTION + "_granules.txt"
headers = ['Granule ID', 'Acquisition Date', 'Revision Date', 'Revision Number']
df = pd.read_csv(FILENAME, names=headers,
                 parse_dates=['Acquisition Date', 'Revision Date'], date_parser=dateparser)
delta_time_secs = [delta.total_seconds() for delta in df['Revision Date'] - df['Acquisition Date']]
delta_time_hours = [x/3600 for x in delta_time_secs]

# for index, row in df.iterrows():
#    print('Granule=%s Revision Date=%s Acquisition Date=%s Delta=%s' % (
#        row['Granule ID'], row['Revision Date'], row['Acquisition Date'], delta_time[index]))

# top plot
plt.subplot(2, 1, 1)
plt.hist(df['Revision Number'], label='Revision Number')
plt.xlim(0, 10)
plt.legend(loc="upper right")
# plt.xlabel("Granule Revision Number")
plt.ylabel("Number of Granules")

# bottom plot
plt.subplot(2, 1, 2)
plt.hist(delta_time_hours, label='(Update Time - Acquisition Time) in hours')
plt.legend(loc="upper right")
# plt.xlabel("(Update DateTime - Acquisition DateTime) in hours")
plt.ylabel("Number of Granules")

plt.suptitle("S1A Granules From: %s To: %s" % (
    min(df['Acquisition Date']).strftime("%Y/%m/%d"),
    max(df['Acquisition Date']).strftime("%Y/%m/%d")))

plt.show()
