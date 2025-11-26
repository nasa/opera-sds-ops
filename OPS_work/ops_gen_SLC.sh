#!/bin/bash

while read line
do
   file=${line:0:67}
   file_type=${file:1:2}
   cmd="python3 /export/home/hysdsops/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c SENTINEL-"$file_type"_SLC --release-version=3.2.0 --job-queue=opera-job_worker-slc_data_download --chunk-size=1 --provider=ASF --processing-mode=reprocessing --native-id="$file"*"
   echo $cmd
done < $1
