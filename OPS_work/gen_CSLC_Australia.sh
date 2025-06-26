#!/bin/bash

while read line
   do
	   file=${line:0:67}
           file_type=${file:1:2}
	   cmd="python3 /export/home/hysdsops/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c SENTINEL-"$file_type"_SLC -p ASF --release-version=3.1.3 --job-queue=opera-job_worker-slc_data_download --chunk-size=1 --processing-mode=historical --native-id="$file"*  --include-regions=australia_5cities_dissolved"
           echo $cmd
  done < $1
