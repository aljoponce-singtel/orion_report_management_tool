#!/bin/bash

source 'c:/Users/p1319639/Development/pythonenv/Scripts/activate'
cd /app/o2p/ossadmin/orion_report_management_tool

echo "Parsed arguments: $@"
python manage.py $@
