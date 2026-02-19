#!/bin/bash
################################################################################
# Run OPERA DSWx-S1 audit for last 2 years with 5-minute threshold
# Automatically calculates current date and 2-year lookback
################################################################################

set -e

# Get current date in UTC
END_DATE=$(date -u +"%Y-%m-%dT23:59:59Z")

# Calculate 2 years ago (730 days)
START_DATE=$(date -u -d "730 days ago" +"%Y-%m-%dT00:00:00Z" 2>/dev/null || \
             date -u -v-730d +"%Y-%m-%dT00:00:00Z" 2>/dev/null)

echo "=================================="
echo "OPERA DSWx-S1 2-Year Audit"
echo "=================================="
echo "Max time span: 5.0 minutes"
echo "Start date: $START_DATE"
echo "End date: $END_DATE"
echo ""

echo "Running 2-year audit..."
python audit_dswx_inputs.py \
  --temporal "$START_DATE,$END_DATE" \
  --max-time-span-minutes 5.0 \
  --max-pages 400 \
  --sleep 0.1 \
  --out failures_2years_5min.csv

echo "✓ Complete: failures_2years_5min.csv"
echo ""
