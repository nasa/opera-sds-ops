#!/bin/bash

# Temporary file to collect all second column values
temp_file=$(mktemp)

# Loop through all matching files
for file in safe_file_ids*.txt; do
    if [[ -f "$file" ]]; then
        echo "Processing $file"
        # Skip the first line and extract the second column
        tail -n +2 "$file" | while IFS=',' read -r col1 col2 _; do
            echo "$col2" >> "$temp_file"
        done
    fi
done

# Extract unique values and write to output file
sort "$temp_file" | uniq > unique_safe_ids.txt

# Clean up temporary file
rm "$temp_file"

echo "Unique second column values written to unique_safe_ids.txt"