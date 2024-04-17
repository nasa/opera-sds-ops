#python3 ~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c SENTINEL-1A_SLC --release-version=2.0.0 --job-queue=opera-job_worker-slc_data_download --chunk-size=1 --processing-mode=reprocessing --native-id=S1A_IW_SLC__1SDV_20230619T162414_20230619T162444_049057_05E637_17BE*

#!/bin/bash

# HLS.S30.T15DWD.2023275T145231.v2.0


while read line
do
   file=${line:0:67}
   file_type=${file:4:3}
   data_year=${file:15:4}
   data_doy=${file:19:3}

   data_date=$(date -d "${data_year}-01-01 +${data_doy} days -1 day" +%F)
   data_date_yesterday=$(date -d "${data_year}-01-01 +${data_doy} days -2 day" +%F)
   data_date_tomorrow=$(date -d "${data_year}-01-01 +${data_doy} days -0 day" +%F)

   cmd="python /export/home/hysdsops/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c SENTINEL-1A_SLC --release-version=2.1.1 --job-queue=opera-job_worker-slc_data_download  --chunk-size=1 --native-id="$file"*"


##   echo $file_type
##   echo $data_year
##   echo $data_doy
##   echo $data_date
   echo $cmd

done < $1

