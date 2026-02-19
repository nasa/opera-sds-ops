#!/bin/bash
################################################################################
# Run OPERA DSWx-S1 audit for last 1 year with 5-minute threshold
# Automatically calculates current date and 1-year lookback
################################################################################

set -e

# Get current date in UTC
END_DATE=$(date -u +"%Y-%m-%dT23:59:59Z")

# Calculate 1 year ago (365 days)
START_DATE=$(date -u -d "365 days ago" +"%Y-%m-%dT00:00:00Z" 2>/dev/null || \
             date -u -v-365d +"%Y-%m-%dT00:00:00Z" 2>/dev/null)

echo "=================================="
echo "OPERA DSWx-S1 1-Year Audit"
echo "=================================="
echo "Max time span: 5.0 minutes"
echo "Start date: $START_DATE"
echo "End date: $END_DATE"
echo ""

echo "Running 1-year audit..."
python audit_dswx_inputs.py \
  --temporal "$START_DATE,$END_DATE" \
  --max-time-span-minutes 5.0 \
  --max-pages 200 \
  --sleep 0.1 \
  --out failures_1year_5min.csv

echo "✓ Complete: failures_1year_5min.csv"
echo ""
