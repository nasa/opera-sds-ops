#! /bin/bash

# loop through HLS granules and submit download jobs for them 
# 
# usage: ./trigger_dswx_from_list_of_granules.sh <list> [--dryrun]
# - hls_list is a list of HLS, S1A, and/or S1B SLC granule ids            
# - use --dryrun or -d to only print the command that will be run 
#																
# ex: ./trigger_dswx_from_list_of_granules.sh f	
#   - where f is a file containing:	
#										
#		HLS.S30.T11SNT.2022335T182731.v2.0
#		HLS.S30.T11SPS.2022335T182731.v2.0
#		S1A_IW_SLC__1SDV_20170213T122240_20170213T122309_015265_019030_32C3

if [[ $# < 1 || $# > 2 ]]; then
	sed -n '3,14p' $0
	exit 1
fi

bad_granule () {
     	echo "invalid granule format: $1"
	exit 1
}


# required to run data subscriber
source /export/home/hysdsops/.bash_profile

LIST=$1
DRYRUN=false
if [[ $2 == "--dryrun" || $2 == "-d" ]]; then
	DRYRUN=true
fi

RELEASE_VERSION="2.0.0-rc.3.0"

for granule in $( cat $LIST ); do 

	## HLS PROCESSING
	if [[ ${granule:0:3} == "HLS" ]] ; then

		if  [[ ${granule:4:1} == "S" ]]; then
			collection="HLSS30"
		elif [[ ${granule:4:1} == "L" ]]; then
			collection="HLSL30"
		else
			bad_granule $granule
		fi

	## SLC PROCESSING
	elif [[ ${granule:0:3} == "S1A" ]]; then
		collection="SENTINEL-1A_SLC"
	
	elif [[ ${granule:0:3} == "S1B" ]]; then
                collection="SENTINEL-1B_SLC"

	else
		bad_granule $granule
	fi


	if $DRYRUN; then
		echo "~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c $collection --release-version=$RELEASE_VERSION --job-queue=opera-job_worker-hls_data_download --chunk-size=1 --native-id=$granule"
	else
		~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c $collection  --release-version=$RELEASE_VERSION --job-queue=opera-job_worker-hls_data_download --chunk-size=1 --native-id=$granule
	fi

done
