import subprocess
import time
from datetime import datetime, date

def get_and_validate_dates():
    """
    Prompts the user for a start and end date, validates them,
    and returns them as date objects.
    """
    today = datetime.now().date()
    while True:
        start_date_str = input("Enter the start date (YYYY-MM-DD): ")
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            print("❌ Error: Invalid date format. Please use YYYY-MM-DD.")
            continue

        end_date_str = input("Enter the end date (YYYY-MM-DD): ")
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            print("❌ Error: Invalid date format. Please use YYYY-MM-DD.")
            continue

        if start_date > today or end_date > today:
            print("❌ Error: Dates cannot be in the future. Please try again.")
        elif end_date <= start_date:
            print("❌ Error: End date must be after the start date. Please try again.")
        else:
            print("✅ Success! The date range is valid.")
            return start_date, end_date

def chunk_date_range(start_date, end_date):
    """
    Splits a given date range into smaller chunks based on the 1st and 16th
    of each month.
    """
    chunks = []
    current_start = start_date

    while current_start < end_date:
        if current_start.day < 16:
            potential_end = date(current_start.year, current_start.month, 16)
        else:
            next_month = current_start.month + 1
            next_year = current_start.year
            if next_month > 12:
                next_month = 1
                next_year += 1
            potential_end = date(next_year, next_month, 1)

        chunk_end = min(potential_end, end_date)
        chunks.append((current_start, chunk_end))
        current_start = chunk_end
        
    return chunks

# This block runs when the script is executed directly
if __name__ == "__main__":
    print("--- Date Range Command Executor with Retries ---")
    
    # --- Configuration for Retries ---
    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 3 # The delay between each retry
    
    valid_start_date, valid_end_date = get_and_validate_dates()
    date_chunks = chunk_date_range(valid_start_date, valid_end_date)
    
    print(f"\n--- Found {len(date_chunks)} chunks. Preparing to execute commands. ---")

    for i, (start, end) in enumerate(date_chunks):
        start_str = f"{start.strftime('%Y-%m-%d')}T00:00:00Z"
        end_str = f"{end.strftime('%Y-%m-%d')}T00:00:00Z"
        
        command = (
            f"python ~/mozart/ops/opera-pcm/tools/ops/cmr_audit/cmr_audit_dswx_s1.py "
            f"--start-datetime {start_str} "
            f"--end-datetime {end_str}"
        )
        
        print(f"\n▶️ Processing Chunk {i+1} of {len(date_chunks)}: From {start} to {end}")
        
        # --- Retry Loop ---
        for attempt in range(MAX_RETRIES):
            print(f"   Attempt {attempt + 1} of {MAX_RETRIES}...")
            try:
                # Execute the command. The 'capture_output' and 'text' arguments
                # help us see the command's output if it fails.
                subprocess.run(
                    command,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True
                )
                print(f"   ✅ Success! Command for Chunk {i+1} completed.")
                break # If successful, break out of the retry loop
            
            except subprocess.CalledProcessError as e:
                print(f"   ⚠️ Command failed with exit code {e.returncode}.")
                # Optional: Print the error output from the failed command
                # print(f"   Stderr: {e.stderr.strip()}")
                
                if attempt < MAX_RETRIES - 1: # Check if more retries are left
                    print(f"      Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS) # Wait before the next attempt
        else:
            # This 'else' block runs ONLY if the 'for' loop completes without a 'break'.
            # This means all retry attempts failed.
            print(f"   ❌ FINAL FAILURE: Command for Chunk {i+1} failed after {MAX_RETRIES} attempts. Moving on.")
            
    print("\n--- All commands have been processed. ---")

