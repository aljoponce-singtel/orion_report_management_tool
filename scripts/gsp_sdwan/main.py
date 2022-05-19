"""GSP Reports

Author: P1319639

This script will generate SDWAN reports:

Add this crontab command for report scheduling:
15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh gsp_sdwan main
"""

import logging
import configparser
from datetime import datetime, timedelta
from scripts import log
from scripts import utils
from scripts.gsp_sdwan import reports

config = configparser.ConfigParser()
config.read('scripts/gsp_sdwan/config.ini')
defaultConfig = config['DEFAULT']


def main():
    log.initialize('scripts/gsp_sdwan/config.ini')
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
