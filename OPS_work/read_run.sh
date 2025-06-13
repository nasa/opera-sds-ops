#!/bin/bash

# Usage: ./read_run.sh filename.txt

FILENAME="$1"

if [[ ! -f "$FILENAME" ]]; then
	  echo "File not found!"
	    exit 1
fi

LINE_COUNT=0

while IFS= read -r LINE; do
      echo "$LINE"
      ((LINE_COUNT++))

       if (( LINE_COUNT % 500 == 0 )); then
	       echo "sleep (2700)"
           #sleep $((45 * 60))  # 45 minutes in seconds
       fi
       done < "$FILENAME"

echo "Done reading file!"
