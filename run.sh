#!/bin/bash

# Get the OS information
os=$(uname -a)

# Windows OS
if echo "$os" | grep -q "MINGW64"; then
    # DEV
    source 'c:/Users/p1319639/Development/pythonenv/Scripts/activate'
# Linux OS
else
    # Production
    source '/app/o2p/ossadmin/python3-env/bin/activate'
    cd /app/o2p/ossadmin/orion_report_management_tool
fi

python run.py $@
