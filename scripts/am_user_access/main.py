"""GSP Reports

Author: P1319639

This script will generate the following SDO reports:
- Singnet (GSDT7x)
- MegaPop (GSDT8x)

Add this crontab command for report scheduling:
15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh am_user_access main
"""

import logging
import configparser
from datetime import datetime, timedelta
from scripts import log
from scripts import utils
from scripts.am_user_access import reports

config = configparser.ConfigParser()
configFile = 'scripts/am_user_access/config.ini'
config.read(configFile)
log.initialize(configFile)
defaultConfig = config['DEFAULT']
logger = logging.getLogger(__name__)


def main():
    logger.info("==========================================")
    logger.info("START of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    logger.info("Running script in " + utils.getPlatform())

    today_date = datetime.now().date()

    try:
        reports.loadConfig(config)
        reportDate = None

        if defaultConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            reports.generateAmUserReport(
                startDate, endDate, "AM User Weekly Access Report")
        else:
            startDate = str(today_date - timedelta(days=7))
            endDate = str(today_date - timedelta(days=1))
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            reports.generateAmUserReport(
                startDate, endDate, "AM User Weekly Access Report")

    except Exception as err:
        logger.exception(err)
        raise Exception(err)

    finally:
        logger.info("END of script - " +
                    datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
