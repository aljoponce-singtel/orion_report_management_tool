"""GSP Reports

Author: P1319639

This script will generate SDWAN reports:

Add this crontab command for report scheduling:
15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh gsp_sdwan main
"""

import os
import sys
import importlib
import logging
import calendar
import configparser
from datetime import datetime, timedelta
import log
import utils

# getting the name of the directory
# where this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the current and parent directory to
# the sys.path.
sys.path.append(parent)
sys.path.append(current)

reports = importlib.import_module('reports')
config = configparser.ConfigParser()
config.read('gsp_sdwan/config_sdwan.ini')
defaultConfig = config['DEFAULT']


def main():
    log.initialize('logs/gsp_sdwan.log')
    logger = logging.getLogger(__name__)

    today_date = datetime.now().date()

    logger.info("==========================================")
    logger.info("START of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    logger.info("Running script in " + utils.getPlatform())

    try:
        startDate = None
        endDate = None

        if defaultConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
        else:
            startDate = str(today_date - timedelta(days=7))
            endDate = str(today_date - timedelta(days=1))

        logger.info("start date: " + str(startDate))
        logger.info("end date: " + str(endDate))

        reports.generateSDWANReport('sdwan_weekly_report', startDate,
                                    endDate, "SDWAN Weekly Report", '')

    except Exception as err:
        logger.error(err)

    logger.info("END of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
