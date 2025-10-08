#!/bin/bash
# run_cmr_audit.sh
# Purpose: Run CMR audit scripts from opera-sds-pcm/tools/ops/cmr_audit
# Date created: June 24, 2025
# Usage: source cmr_audit.env && ./run_cmr_audit.sh -f <script_name> [additional options]
#        Where script_name is one of:
#        - hls (for cmr_audit_hls.py)
#        - rtc_s1 (for cmr_audit_slc.py with RTC audit)
#        - cslc_s1 (for cmr_audit_slc.py with CSLC audit)
#        - disp_s1 (for cmr_audit_disp_s1.py)
#        - dswx_s1 (for cmr_audit_dswx_s1.py)

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cmdname=$(basename $0)

######################################################################
# Default values and constants
######################################################################

# Default values
processing_mode="forward"  # Default processing mode for DISP-S1
output_format="txt"        # Default output format for DSWX-S1
k_value=15                 # Default K value for DISP-S1
log_level="INFO"           # Default log level
dry_run=false              # Default to actually run the command
start_days=28              # Default start point in days (28 days ago)
end_days=7                 # Default end point in days (7 days ago)
max_gap_days=1             # Maximum allowed gap between start and end dates in days
push_to_git=false          # Default to not push results to git

# Repository paths - use environment variables with fallback defaults
PCM_REPO_PATH="${PCM_REPO_PATH:-/export/home/hysdsops/scheduled_tasks/opera-sds-pcm}"
OPS_REPO_PATH="${OPS_REPO_PATH:-/export/home/hysdsops/scheduled_tasks/opera-sds-ops}"

# Virtual environment path - use environment variable with fallback default
CMR_AUDIT_VENV_PATH="${CMR_AUDIT_VENV_PATH:-$PCM_REPO_PATH/venv_cmr_audit/bin/activate}"

# GeoJSON file path - use environment variable with fallback default
GEOJSON_FILE_PATH="${GEOJSON_FILE_PATH}"

######################################################################
# Functions
######################################################################

# Display usage
usage() {
  cat << USAGE >&2
Usage:
  $cmdname [options]

Required options:
  -f, --filename <n>  Script to run (required)
                         Valid values: hls, rtc_s1, cslc_s1, disp_s1, dswx_s1

Optional parameters:
  -m, --mode <mode>      Processing mode for DISP-S1 (forward, reprocessing, historical)
                         Default: $processing_mode
  -o, --output <file>    Output filepath for audit results (for DSWX-S1)
  -k, --k-value <num>    K-value for DISP-S1 (default: $k_value)
  -s, --start <days>     Starting point in days ago (default: $start_days)
                         Audit will run from <days> ago to end days ago
  -e, --end <days>       Ending point in days ago (default: $end_days)
  --max-gap-days <n>     Maximum days per audit call when splitting long ranges (default: $max_gap_days)
  --format <format>      Output format for DSWX-S1 (txt, json) (default: $output_format)
  --frames-only <list>   Restrict validation to specific frame numbers (comma-separated)
  --validate-with-grq    Use GRQ database instead of CMR for DISP-S1
  --log-level <level>    Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) (default: $log_level)
  -n, --dry-run          Show the command that would be executed without running it
  --push-to-git          Push generated files to opera-sds-ops git repository (default: false)
  -h, --help             Show this help message

Examples:
  source cmr_audit.env && $cmdname --filename hls
  source cmr_audit.env && $cmdname -f rtc_s1
  source cmr_audit.env && $cmdname -f cslc_s1
  source cmr_audit.env && $cmdname -f disp_s1 -m historical -k 15
  source cmr_audit.env && $cmdname -f dswx_s1 --format json -o results.json
  source cmr_audit.env && $cmdname -f hls --dry-run
  source cmr_audit.env && $cmdname -f rtc_s1 -s 35 -e 7  # Run from 35 days ago to 7 days ago
USAGE
}

# Log messages
echoerr() { echo "$@" 1>&2; }
log_info() { echo "[INFO] $@"; }
log_error() { echoerr "[ERROR] $@"; }

# Exit with error
exit_with_error() {
  log_error "$1"
  exit 1
}

# Map shorthand name to full script name
get_full_script_name() {
  local shorthand=$1
  case $shorthand in
    hls)
      echo "cmr_audit_hls"
      ;;
    rtc_s1)
      echo "cmr_audit_slc"
      ;;
    cslc_s1)
      echo "cmr_audit_slc"
      ;;
    disp_s1)
      echo "cmr_audit_disp_s1"
      ;;
    dswx_s1)
      echo "cmr_audit_dswx_s1"
      ;;
    *)
      return 1
      ;;
  esac
}

# Execute a single audit command
execute_audit_command() {
  local start_date=$1
  local end_date=$2
  local cmd_base=$3

  # Extract product type from command base (look for the script name pattern)
  local product_type=""
  if [[ "$cmd_base" == *"cmr_audit_hls"* ]]; then
    product_type="hls"
  elif [[ "$cmd_base" == *"cmr_audit_slc"* ]]; then
    # Determine if it's RTC or CSLC based on the script_shorthand
    if [[ "$script_shorthand" == "rtc_s1" ]]; then
      product_type="rtc_s1"
    elif [[ "$script_shorthand" == "cslc_s1" ]]; then
      product_type="cslc_s1"
    else
      log_error "Unable to determine SLC product type from script_shorthand: $script_shorthand"
      return 1
    fi
  elif [[ "$cmd_base" == *"cmr_audit_disp_s1"* ]]; then
    product_type="disp_s1"
  elif [[ "$cmd_base" == *"cmr_audit_dswx_s1"* ]]; then
    product_type="dswx_s1"
  else
    log_error "Unable to determine product type from command: $cmd_base"
    return 1
  fi

  # Format dates for directory name (only keep YYYYMMDD)
  local start_dir=$(echo "$start_date" | cut -d'T' -f1 | sed 's/-//g')
  local end_dir=$(echo "$end_date" | cut -d'T' -f1 | sed 's/-//g')
  
  # Create directory structure: product_type/start_date-end_date
  local output_dir=""
  # Create directory structure
  if [ "$max_gap_days" -eq 1 ]; then
  # Special case: if max_gap_days is 1, only use start day
    output_dir="${product_type}/${start_dir}"
  else
    # Default: use both start and end
    output_dir="${product_type}/${start_dir}-${end_dir}"
  fi


  if [ "$dry_run" = true ]; then
    log_info "DRY RUN: Would create directory: $output_dir"
  else
    log_info "Creating output directory: $output_dir"
    mkdir -p "$output_dir"
    if [ $? -ne 0 ]; then
      log_error "Failed to create directory: $output_dir"
      return 1
    fi
  fi

  # Create symlink for geojson file if running SLC audit (both RTC and CSLC)
  if [ "$product_type" = "rtc_s1" ] || [ "$product_type" = "cslc_s1" ]; then
    local geojson_source="$GEOJSON_FILE_PATH"
    local geojson_target="$output_dir/north_america_opera.geojson"
    
    if [ "$dry_run" = true ]; then
      log_info "DRY RUN: Would create symlink: $geojson_target -> $geojson_source"
    else
      if [ -f "$geojson_source" ]; then
        log_info "Creating symlink for geojson file: $geojson_target -> $geojson_source"
        ln -sf "$geojson_source" "$geojson_target"
        if [ $? -ne 0 ]; then
          log_error "Failed to create symlink for geojson file"
          return 1
        fi
      else
        log_error "Source geojson file not found: $geojson_source"
        return 1
      fi
    fi
  fi

  local cmd="${cmd_base} --start-datetime=${start_date} --end-datetime=${end_date}"

  # Execute or display command based on dry run flag
  if [ "$dry_run" = true ]; then
    log_info "DRY RUN: Command that would be executed in directory $output_dir:"
    echo "cd $output_dir && $cmd"
  else
    log_info "Executing command in directory: $output_dir"
    log_info "Command: $cmd"
    
    # Change to output directory and execute command
    (cd "$output_dir" && eval "$cmd")
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
      log_info "Command completed successfully in $output_dir"
    else
      log_error "Command failed with exit code $exit_code in $output_dir"
      return $exit_code
    fi
  fi
}

# Push generated files to git repository
push_to_git_repo() {
  local product_type=$1
  local start_dir=$2
  local end_dir=$3
  
  if [ "$push_to_git" = false ]; then
    return 0
  fi

  log_info "Preparing to push results to git repository..."

  # Get git token from ~/.github_token file, environment variable, or SDS config
  if [[ -f ~/.github_token ]]; then
    GIT_TOKEN=$(cat ~/.github_token | tr -d '\n\r')
    log_info "Using GitHub token from ~/.github_token file"
  else
    GIT_TOKEN=${GIT_OAUTH_TOKEN:-$(grep "^GIT_OAUTH_TOKEN:" ~/.sds/config | awk '{print $2}' 2>/dev/null)}
    if [[ -n "$GIT_TOKEN" ]]; then
      log_info "Using GitHub token from environment variable or SDS config"
    fi
  fi
  
  if [[ -z "$GIT_TOKEN" ]]; then
    log_error "Git token not found. Cannot push to repository."
    log_error "Please ensure one of the following:"
    log_error "  1. Create ~/.github_token file with your GitHub personal access token"
    log_error "  2. Set GIT_OAUTH_TOKEN environment variable"
    log_error "  3. Add GIT_OAUTH_TOKEN to ~/.sds/config file"
    return 1
  fi

  # Configure git to use the token for authentication
  git config --global user.name "CMR Audit Script"
  git config --global user.email "hysdsops@jpl.nasa.gov"

  # Check if we're in a git repository or if opera-sds-ops exists
  local cloned_repo=false
  if [ ! -d "$OPS_REPO_PATH" ]; then
    log_info "Cloning opera-sds-ops repository..."
    if [ "$dry_run" = true ]; then
      log_info "DRY RUN: Would clone https://github.com/nasa/opera-sds-ops.git to $OPS_REPO_PATH"
    else
      # Use token directly in the URL for cloning
      git clone "https://${GIT_TOKEN}:@github.com/nasa/opera-sds-ops.git" "$OPS_REPO_PATH"
              if [ $? -ne 0 ]; then
          log_error "Failed to clone opera-sds-ops repository"
          return 1
        fi
      cloned_repo=true
    fi
  fi

  # Copy generated files to the ops repo
  local current_dir=$(pwd)
  local today_date=$(date +"%Y-%m-%d")
  # local branch_name="cmr_audit_results_${product_type}_${today_date}"
  local branch_name="feature/schedule_tasks"

  if [ "$dry_run" = true ]; then
    log_info "DRY RUN: Would copy .txt files from $current_dir to $OPS_REPO_PATH/scheduled_tasks/"
    log_info "DRY RUN: Would clean up product folders from working directory:"
    for product_folder in hls rtc_s1 cslc_s1 disp_s1 dswx_s1; do
      if [ -d "$current_dir/$product_folder" ]; then
        log_info "DRY RUN: Would remove folder: $current_dir/$product_folder"
      fi
    done
    log_info "DRY RUN: Would create branch $branch_name and push changes"
    if [ "$cloned_repo" = true ]; then
      log_info "DRY RUN: Would clean up cloned repository at $OPS_REPO_PATH"
    fi
    return 0
  fi

  # Navigate to ops repo
  cd "$OPS_REPO_PATH" || {
    log_error "Failed to navigate to $OPS_REPO_PATH"
    return 1
  }

  # Update repository
  log_info "Updating repository..."
  git fetch origin
  git checkout main
  git pull origin main

  # Create new branch
  log_info "Creating branch: $branch_name"
  git checkout -b "$branch_name"

# Ensure remote is up to date
git fetch --prune

# If branch has no upstream set, set it to origin/branch_name
if ! git rev-parse --abbrev-ref --symbolic-full-name "@{u}" >/dev/null 2>&1; then
  git branch --set-upstream-to="origin/$branch_name" "$branch_name" || true
fi

# Fast-forward only (fails if local has diverged)
git pull --ff-only || {
  echo "Local branch has diverged from origin/$branch_name. Resetting to match remote..."
  git reset --hard "origin/$branch_name"
  git clean -fd
}


  # Copy files (only .txt files for the specific product type)
  log_info "Copying generated .txt files for product type: $product_type..."
  
  # Check if the product folder exists in current directory
  if [ -d "$current_dir/$product_type" ]; then
    # Create target directory structure in opera-sds-ops repo
    target_product_dir="scheduled_tasks/$product_type"
    mkdir -p "$target_product_dir"
    
    # Find and copy only .txt files from the specific product folder
    find "$current_dir/$product_type" -name "*.txt" -type f | while read txt_file; do
      # Get relative path from the product folder
      rel_path="${txt_file#$current_dir/$product_type/}"
      target_subdir="$target_product_dir/$(dirname "$rel_path")"
      
      # Create subdirectory if it doesn't exist
      if [ "$(dirname "$rel_path")" != "." ]; then
        mkdir -p "$target_subdir"
        target_file="$target_subdir/$(basename "$txt_file")"
      else
        target_file="$target_product_dir/$(basename "$txt_file")"
      fi
      
      log_info "Copying: $txt_file -> $target_file"
      cp "$txt_file" "$target_file"
    done
  else
    log_info "No product folder found for $product_type, skipping file copy"
  fi

  # Clean up the specific product folder from the working directory after copying
  log_info "Cleaning up $product_type folder from working directory..."
  cd "$current_dir"
  if [ -d "$product_type" ]; then
    log_info "Removing product folder: $product_type"
    rm -rf "$product_type"
    if [ $? -eq 0 ]; then
      log_info "Successfully removed $product_type folder"
    else
      log_error "Warning: Failed to remove $product_type folder"
    fi
  fi

  # Return to ops repo directory
  cd "$OPS_REPO_PATH"

  # Add only .txt files from the specific product type folder
  if [ -d "scheduled_tasks/$product_type" ]; then
    find "scheduled_tasks/$product_type" -name "*.txt" -type f -exec git add {} + 2>/dev/null || true
    log_info "Added .txt files from scheduled_tasks/$product_type to git staging"
  else
    log_info "No scheduled_tasks/$product_type directory found, nothing to add"
  fi
  
  # Check if there are changes to commit
  if git diff --staged --quiet; then
    log_info "No changes to commit"
    git checkout main
    git branch -D "$branch_name"
    cd "$current_dir"
    return 0
  fi

  # Commit changes
  local commit_message="Add CMR audit results for $product_type ($(date +"%Y-%m-%d %H:%M:%S"))"
  git commit -m "$commit_message"

  # Push branch
  log_info "Pushing branch $branch_name to remote repository..."
  git push "https://${GIT_TOKEN}:@github.com/nasa/opera-sds-ops.git" "$branch_name"

  if [ $? -eq 0 ]; then
    log_info "Successfully pushed results to git repository in branch: $branch_name"
    log_info "You can create a pull request to merge these changes to main"
      else
      log_error "Failed to push to git repository"
      cd "$current_dir"
      return 1
    fi

  # Return to original directory
  cd "$current_dir"
  
  # Clean up the cloned repository (only if we cloned it)
  if [ "$cloned_repo" = true ]; then
    log_info "Cleaning up cloned repository at $OPS_REPO_PATH..."
    # Double-check that the path contains 'opera-sds-ops' for safety
    if [[ "$OPS_REPO_PATH" == *"opera-sds-ops"* ]]; then
      rm -rf "$OPS_REPO_PATH"
      if [ $? -eq 0 ]; then
        log_info "Successfully cleaned up cloned repository"
      else
        log_error "Warning: Failed to clean up cloned repository at $OPS_REPO_PATH"
      fi
    else
      log_error "Safety check failed: Repository path doesn't contain 'opera-sds-ops'. Skipping cleanup."
    fi
  else
    log_info "Repository was not cloned by this script, skipping cleanup"
  fi
  
  return 0
}

######################################################################
# Parse arguments
######################################################################

# If no arguments provided, show usage
if [ $# -eq 0 ]; then
  usage
  exit 1
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -f|--filename)
      script_shorthand="$2"
      shift 2
      ;;
    # Legacy support for old-style flags
    --hls)
      script_shorthand="hls"
      shift
      ;;
    --rtc_s1)
      script_shorthand="rtc_s1"
      shift
      ;;
    --cslc_s1)
      script_shorthand="cslc_s1"
      shift
      ;;
    --disp_s1)
      script_shorthand="disp_s1"
      shift
      ;;
    --dswx_s1)
      script_shorthand="dswx_s1"
      shift
      ;;
    -m|--mode)
      processing_mode="$2"
      shift 2
      ;;
    -o|--output)
      output_file="$2"
      shift 2
      ;;
    -k|--k-value)
      k_value="$2"
      shift 2
      ;;
    -s|--start|-p|--period)  # Support both -s/--start and -p/--period for backward compatibility
      start_days="$2"
      shift 2
      ;;
    -e|--end)
      end_days="$2"
      shift 2
      ;;
    --max-gap-days)
      max_gap_days="$2"
      shift 2
      ;;
    --format)
      output_format="$2"
      shift 2
      ;;
    --frames-only)
      frames_only="$2"
      shift 2
      ;;
    --validate-with-grq)
      validate_with_grq="--validate-with-grq"
      shift
      ;;
    --log-level)
      log_level="$2"
      shift 2
      ;;
    -n|--dry-run)
      dry_run=true
      shift
      ;;
    --push-to-git)
      push_to_git=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echoerr "Unsupported argument $1. Exiting."
      usage
      exit 1
      ;;
  esac
done


######################################################################
# Argument validation
######################################################################

if [[ ! -v script_shorthand ]]; then
  log_error "Script name is required"
  usage
  exit 1
fi

# Convert shorthand to full script name
cmr_audit_filename=$(get_full_script_name "$script_shorthand")
if [[ $? -ne 0 ]]; then
  exit_with_error "Invalid script shorthand: $script_shorthand. Must be one of: hls, rtc_s1, cslc_s1, disp_s1, dswx_s1"
fi

# For DISP-S1, ensure processing-mode is valid
if [[ "$script_shorthand" == "disp_s1" ]]; then
  valid_modes=("forward" "reprocessing" "historical")
  if [[ ! " ${valid_modes[*]} " =~ " ${processing_mode} " ]]; then
    exit_with_error "Invalid processing mode: $processing_mode. Must be one of: ${valid_modes[*]}"
  fi
fi

# Validate that start_days is a positive integer
if ! [[ "$start_days" =~ ^[0-9]+$ ]]; then
  exit_with_error "Start days must be a positive integer: $start_days"
fi

# Enforce minimum for start_days to be at least 7 days
if [ "$start_days" -lt 7 ]; then
  log_info "Start days must be at least 7, setting to 7"
  start_days=7
fi

# Validate that end_days is a positive integer
if ! [[ "$end_days" =~ ^[0-9]+$ ]]; then
  exit_with_error "End days must be a positive integer: $end_days"
fi

# Enforce minimum for end_days to be at least 0 days (can be today)
if [ "$end_days" -lt 0 ]; then
  log_info "End days cannot be negative, setting to 0"
  end_days=0
fi

# Validate that start_days is greater than end_days
if [ "$start_days" -le "$end_days" ]; then
  exit_with_error "Start days ($start_days) must be greater than end days ($end_days)"
fi


# Validate max_gap_days
if ! [[ "$max_gap_days" =~ ^[0-9]+$ ]]; then
  exit_with_error "Max gap days must be a positive integer: $max_gap_days"
fi
if [ "$max_gap_days" -lt 1 ]; then
  log_info "Max gap days must be at least 1, setting to 1"
  max_gap_days=1
fi

######################################################################
# Main script body
######################################################################

# Only activate Python environment if not in dry run mode
if [ "$dry_run" = false ]; then
  # deactivate any existing python virtual environments (typically "metrics")
  set +e
  deactivate 2>/dev/null || true
  set -e

  # Set up Python environment
  source "$CMR_AUDIT_VENV_PATH"

  # Make sure the opera-sds-pcm modules can be found by adding to PYTHONPATH
  export PYTHONPATH="$PCM_REPO_PATH:$PCM_REPO_PATH/opera_commons:$PYTHONPATH"
fi

# Build base command (without start and end dates)
# Prefer running as a module so intra-repo imports (e.g., rtc_utils) resolve via PYTHONPATH
cmd_base="PYTHONPATH=$PCM_REPO_PATH:$PCM_REPO_PATH/tools/ops/cmr_audit:$PYTHONPATH python -m tools.ops.cmr_audit.${cmr_audit_filename} --log-level=$log_level"


# Add script-specific arguments
case $script_shorthand in
  "disp_s1")
    cmd_base="$cmd_base --processing-mode=$processing_mode"
    # Add optional DISP-S1 specific arguments
    [[ -v k_value ]] && cmd_base="$cmd_base --k=$k_value"
    [[ -v frames_only ]] && cmd_base="$cmd_base --frames-only=$frames_only"
    [[ -v validate_with_grq ]] && cmd_base="$cmd_base $validate_with_grq"
    ;;

  "dswx_s1")
    # Add optional DSWX-S1 specific arguments
    [[ -v output_file ]] && cmd_base="$cmd_base --output=$output_file"
    [[ -v output_format ]] && cmd_base="$cmd_base --format=$output_format"
    ;;

  "rtc_s1")
    # Enable RTC audit and disable CSLC audit
    cmd_base="$cmd_base --do_rtc=true --do_cslc=false"
    ;;

  "cslc_s1")
    # Enable CSLC audit and disable RTC audit
    cmd_base="$cmd_base --do_cslc=true --do_rtc=false"
    ;;
esac

# Calculate date ranges
now=$(date --iso-8601=d)
end_date=$(date --iso-8601=s -d "$now - ${end_days} days")
start_date=$(date --iso-8601=s -d "$now - ${start_days} days")

# Calculate the total days between start and end
total_days=$((start_days - end_days))

log_info "Running CMR audit for: $cmr_audit_filename"
log_info "Total time range: $start_date to $end_date"

# Break down into chunks if needed
if [ $total_days -gt $max_gap_days ]; then
  log_info "Time range exceeds maximum allowed gap of $max_gap_days days. Breaking down into multiple calls."

  # Calculate how many chunks we need
  num_chunks=$(( (total_days + max_gap_days - 1) / max_gap_days ))
  log_info "Will execute $num_chunks separate audit calls."

  # Process each chunk
  for (( i=0; i<$num_chunks; i++ )); do
    if [ $i -eq $((num_chunks-1)) ]; then
      # Last chunk ends at the fixed end date
      chunk_end_date=$end_date
    else
      # Earlier chunks have their own end dates
      chunk_end_days=$((start_days - i*max_gap_days - max_gap_days))
      if [ $chunk_end_days -lt $end_days ]; then
        chunk_end_days=$end_days
      fi
      chunk_end_date=$(date --iso-8601=s -d "$now - ${chunk_end_days} days")
    fi

    # Calculate start date for this chunk
    if [ $i -eq 0 ]; then
      # First chunk starts at the original start date
      chunk_start_date=$start_date
    else
      # Other chunks start where the previous chunk ended
      chunk_start_days=$((start_days - (i-1)*max_gap_days - max_gap_days))
      chunk_start_date=$(date --iso-8601=s -d "$now - ${chunk_start_days} days")
    fi

    log_info "Processing chunk $((i+1)) of $num_chunks: $chunk_start_date to $chunk_end_date"
    execute_audit_command "$chunk_start_date" "$chunk_end_date" "$cmd_base"
  done
else
  # Single execution for small ranges
  execute_audit_command "$start_date" "$end_date" "$cmd_base"
fi

# Push results to git if requested
if [ "$push_to_git" = true ]; then
  # Use the overall date range for branch naming
  overall_start_dir=$(echo "$start_date" | cut -d'T' -f1 | sed 's/-//g')
  overall_end_dir=$(echo "$end_date" | cut -d'T' -f1 | sed 's/-//g')
  
  push_to_git_repo "$script_shorthand" "$overall_start_dir" "$overall_end_dir"
  if [ $? -ne 0 ]; then
    log_error "Git push failed, but audit completed successfully"
  fi
else
  # Keep the generated files when not pushing to git
  log_info "Generated files are available in the $script_shorthand folder in the current working directory"
  if [ -d "$script_shorthand" ]; then
    log_info "Results saved locally in: $(pwd)/$script_shorthand"
  fi
fi

# Return success
log_info "CMR audit completed successfully"
exit 0

