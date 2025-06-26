#python3 ~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c SENTINEL-1A_SLC --release-version=2.0.0 --job-queue=opera-job_worker-slc_data_download --chunk-size=1 --processing-mode=reprocessing --native-id=S1A_IW_SLC__1SDV_20230619T162414_20230619T162444_049057_05E637_17BE*

# S1A_IW_SLC__1SDV_20211222T035608_20211222T035635_041116_04E29A_38B0

#!/bin/bash


while read line
do
   file=${line:0:67}
   file_type=${file:1:2}

   cmd="python /export/home/hysdsops/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c SENTINEL-"$file_type"_SLC --job-queue=opera-job_worker-slc_data_download  --chunk-size=1 --native-id="$file"*"


   echo $cmd

done < $1

