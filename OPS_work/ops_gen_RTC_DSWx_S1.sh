#!/bin/bash
#python3 ~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c OPERA_L2_RTC-S1_V1   --job-queue=opera-job_worker-rtc_data_download --chunk-size 1 --native-id=OPERA_L2_RTC-S1_T025-052658-IW2_20240312T102134Z_20240313T121214Z_S1A_30_v1.0
while read line
do
   file=${line}
   cmd="python3 ~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c OPERA_L2_RTC-S1_V1   --job-queue=opera-job_worker-rtc_data_download --chunk-size 1 --native-id="$file""
   echo $cmd
done < $1
