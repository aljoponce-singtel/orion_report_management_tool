"""GSP Reports

Author: P1319639

This script will generate Singnet reports:

Add this crontab command for report scheduling:
15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh gsp main2
"""

import logging
import configparser
from datetime import datetime, timedelta
from scripts import log
from scripts import utils
from scripts.gsp import reports

config = configparser.ConfigParser()
configFile = 'scripts/gsp/config.ini'
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
        if defaultConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            defaultConfig['UpdateTableauDB'] = 'false'
            logger.info('UpdateTableauDB = ' +
                        str(defaultConfig.getboolean('UpdateTableauDB')))

            reports.generateSingnetReport(
                'singnet_report_apnic', startDate, endDate, 'gsdt7', "Singnet Report - GSP APNIC", 'teckchye@singtel.com;tao.taskrequest@singtel.com')
            # reports.generateSingnetReport(
            #     'singnet_report_connectportal', startDate, endDate, 'gsdt7', "Singnet Report - Connect Portal updating", 'kirti.vaish@singtel.com;sandeep.kumarrajendran@singtel.com')

        else:
            #-- START --#
            # If the day falls on a Monday
            # start date = date of the previous Monday (T-7)
            # end date = date of the previous Sunday (T-1)

            # TEST DATA
            # today_date = datetime.now().date().replace(day=7)
            # print("date today: " + str(today_date))

            if today_date.isoweekday() == 1:  # Monday
                startDate = str(today_date - timedelta(days=7))
                endDate = str(today_date - timedelta(days=1))
                logger.info("start date: " + str(startDate))
                logger.info("end date: " + str(endDate))

                defaultConfig['UpdateTableauDB'] = 'false'
                logger.info('UpdateTableauDB = ' +
                            str(defaultConfig.getboolean('UpdateTableauDB')))

                reports.generateSingnetReport(
                    'singnet_report_apnic', startDate, endDate, 'gsdt7', "Singnet Report - GSP APNIC", 'teckchye@singtel.com;tao.taskrequest@singtel.com')
                reports.generateSingnetReport(
                    'singnet_report_connectportal', startDate, endDate, 'gsdt7', "Singnet Report - Connect Portal updating", 'kirti.vaish@singtel.com;sandeep.kumarrajendran@singtel.com')

            #-- END --#

    except Exception as err:
        logger.exception(err)

    logger.info("END of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
