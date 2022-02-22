#!/bin/bash
PATH=/home/mysql/perl5/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/home/mysql/.local/bin:/home/mysql/bin
export PATH

source /app/o2p/ossadmin/python3-env/bin/activate
cd /app/o2p/ossadmin/gsp_reports
python reports.py
