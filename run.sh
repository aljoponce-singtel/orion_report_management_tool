#!/bin/bash

# Production
# source '/app/o2p/ossadmin/python3-env/bin/activate'
# cd /app/o2p/ossadmin/orion_report_management_tool

# DEV
source 'c:/Users/p1319639/Development/pythonenv/Scripts/activate'

echo "Parsed arguments: $@"
python run.py $@
