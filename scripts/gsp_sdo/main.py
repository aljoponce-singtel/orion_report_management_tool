"""GSP Reports

Author: P1319639

This script will generate the following SDO reports:
- Singnet (GSDT7x)
- MegaPop (GSDT8x)

Add this crontab command for report scheduling:
15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh gsp_sdo main
"""

import logging
import configparser
from datetime import datetime, timedelta
from scripts import log
from scripts import utils
from scripts.gsp_sdo import reports

config = configparser.ConfigParser()
configFile = 'scripts/gsp_sdo/config.ini'
config.read(configFile)
log.initialize(configFile)
defaultConfig = config['DEFAULT']
logger = logging.getLogger(__name__)


def main():
    logger.info("==========================================")
    logger.info("START of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    logger.info("Running script in " + utils.getPlatform())

    reports.loadConfig(config)
    today_date = datetime.now().date()

    try:
        reportDate = None

        if defaultConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            reportDate = defaultConfig['ReportDate']
            logger.info("report date: " + str(reportDate))

            # reports.generateSdoSingnetReport(
            #     'sdo_singnet_report', reportDate, "SDO Singnet Report")
            reports.generateSdoMegaPopReport(
                'sdo_megapop_report', reportDate, "SDO MegaPop Report")
        else:
            reportDate = str(today_date.replace(day=1))
            logger.info("report date: " + str(reportDate))

            reports.generateSdoSingnetReport(
                'sdo_singnet_report', reportDate, "SDO Singnet Report")
            reports.generateSdoMegaPopReport(
                'sdo_megapop_report', reportDate, "SDO MegaPop Report")

    except Exception as err:
        logger.exception(err)

    logger.info("END of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
