import glob
import csv

# Set to hold unique second-column values
unique_ids = set()

# Find all matching files
for filename in glob.glob('safe_file_ids*.txt'):
    print(f"Processing {filename}")
    with open(filename, 'r', newline='') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip the header
        for row in reader:
            if len(row) > 1:
                unique_ids.add(row[1])

# Write unique values to output file
with open('unique_safe_ids.txt', 'w') as out_file:
    for uid in sorted(unique_ids):  # Sort for consistency
        out_file.write(f"{uid}\n")

print("Unique second column values written to unique_safe_ids.txt")