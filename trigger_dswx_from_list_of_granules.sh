#! /bin/bash

# loop through HLS granules and submit download jobs for them 
# 
# usage: ./trigger_dswx_from_list_of_granules.sh <hls list> [--dryrun]
# - hls_list is a list of HLS granules ids                     
# - use --dryrun or -d to only print the command that will be run 
#																
# ex: ./trigger_dswx_from_list_of_granules.sh f	
#   - where f is a file containing:	
#										
#		HLS.S30.T11SNT.2022335T182731.v2.0
#		HLS.S30.T11SPS.2022335T182731.v2.0
#		HLS.S30.T11SNS.2022335T182731.v2.0

if [[ $# < 1 || $# > 2 ]]; then
	sed -n '3,14p' $0
	exit 1
fi

HLS_LIST=$1
DRYRUN=false
if [[ $2 == "--dryrun" || $2 == "-d" ]]; then
	DRYRUN=true
fi

RELEASE_VERSION="1.0.0-rc.8.0"

for granule in $( cat $HLS_LIST ); do 

	if  [[ ${granule:4:1} == "S" ]]; then
		collection="HLSS30"
	elif [[ ${granule:4:1} == "L" ]]; then
		collection="HLSL30"
	else
		echo "invalid granule format: $granule"
		exit 1
	fi

	if $DRYRUN; then
		echo "~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c $collection -p LPCLOUD --release-version=$RELEASE_VERSION --job-queue=opera-job_worker-hls_data_download --chunk-size=1 --native-id=$granule"
	else
		~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c $collection -p LPCLOUD --release-version=$RELEASE_VERSION --job-queue=opera-job_worker-hls_data_download --chunk-size=1 --native-id=$granule
	fi

done
