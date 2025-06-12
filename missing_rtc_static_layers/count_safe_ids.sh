#!/bin/bash

# Loop through all files starting with "safe_file_ids" and ending with ".txt"
for file in safe_file_ids*.txt; do
    if [[ -f "$file" ]]; then
        count=$(wc -l < "$file")
        echo "$file: $count lines"
    fi
done
