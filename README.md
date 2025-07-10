# opera-sds-ops

Operational (OPS) related scripts, configuration, and issues for the OPERA Science Data System (SDS)
CMR Audit Script Runner
A utility script for running Common Metadata Repository (CMR) audit tools from the opera-sds-pcm repository.

Overview
run_cmr_audit.sh is a bash script that simplifies running CMR audit scripts for various OPERA products:

HLS (Harmonized Landsat Sentinel)
SLC (Single Look Complex)
DISP-S1 (Displacement Sentinel-1)
DSWX-S1 (Dynamic Surface Water Extent Sentinel-1)
The script handles common tasks such as:

Setting up the correct Python environment
Managing date ranges and time periods
Breaking down large time ranges into manageable chunks
Providing a consistent interface for different audit scripts
Requirements
opera-sds-pcm repository installed at /export/home/hysdsops/scheduled_tasks/opera-sds-pcm
Python virtual environment set up at $PCM_REPO_PATH/venv_cmr_audit
Usage
bash
./run_cmr_audit.sh -f <script_name> [additional options]
Where script_name is one of:

hls (for cmr_audit_hls.py)
slc (for cmr_audit_slc.py)
disp_s1 (for cmr_audit_disp_s1.py)
dswx_s1 (for cmr_audit_dswx_s1.py)
Options
Required Options
-f, --filename <name>: Script to run (required)
Valid values: hls, slc, disp_s1, dswx_s1
Optional Parameters
-m, --mode <mode>: Processing mode for DISP-S1 (forward, reprocessing, historical)
Default: forward
-o, --output <file>: Output filepath for audit results (for DSWX-S1)
-k, --k-value <num>: K-value for DISP-S1 (default: 15)
-s, --start <weeks>: Starting point in weeks ago (default: 5)
Audit will run from <weeks> ago to 1 week ago
--format <format>: Output format for DSWX-S1 (txt, json) (default: txt)
--frames-only <list>: Restrict validation to specific frame numbers (comma-separated)
--validate-with-grq: Use GRQ database instead of CMR for DISP-S1
--log-level <level>: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) (default: INFO)
-n, --dry-run: Show the command that would be executed without running it
-h, --help: Show help message
Examples
bash
# Run HLS audit for the default period (5 weeks ago to 1 week ago)
./run_cmr_audit.sh --filename hls

# Run SLC audit with a shorter syntax
./run_cmr_audit.sh -f slc

# Run DISP-S1 audit with specific parameters
./run_cmr_audit.sh -f disp_s1 -m historical -k 15

# Run DSWX-S1 audit with JSON output
./run_cmr_audit.sh -f dswx_s1 --format json -o results.json

# See the command that would be run without executing it
./run_cmr_audit.sh -f hls --dry-run

# Run SLC audit for a custom time period (8 weeks ago to 1 week ago)
./run_cmr_audit.sh -f slc -s 8
Features
Time Range Management
The script automatically breaks down large time ranges into smaller chunks (maximum 1 week per chunk) to prevent timeouts and improve reliability
End date is fixed at 1 week ago from current date
Start date is configurable (default: 5 weeks ago)
Environment Setup
Automatically deactivates any existing Python environment
Activates the CMR audit virtual environment
Sets up the correct PYTHONPATH for accessing opera-sds-pcm modules
Note
This script is part of the OPERA SDS PCM toolkit and is designed to run on systems with the correct environment setup.





side note
 cat ~/.github_token   git hub token is here



 