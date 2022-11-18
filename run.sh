#!/bin/bash

# Get the OS information
os=$(uname -a)

# Windows OS
if echo "$os" | grep -q "MINGW64"; then
    # DEV
    source 'c:/Users/p1319639/Development/pythonenv/Scripts/activate'
    cd 'c:/Users/p1319639/Development/orion_report_management_tool'
# Linux OS
else
    # Production
    source '/app/o2p/ossadmin/python368-env/bin/activate'
    cd /app/o2p/ossadmin/ormt
fi

python run.py $@
