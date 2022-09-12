# import sys
import os
import logging
import configparser
import calendar
from datetime import datetime, timedelta
from scripts import log
from scripts import utils
from scripts.gsp_transport import reports

# # Getting the name of the directory where this file is present.
# current = os.path.dirname(os.path.realpath(__file__))
# # Getting the parent directory name where the current directory is present.
# parent = os.path.dirname(current)
# # Adding the parent directory to the sys.path.
# sys.path.append(parent)
# # Import modules as recognized from sys.path.

# import log
# import utils

config = configparser.ConfigParser()
configFile = 'scripts/gsp_transport/config.ini'
# configFile = os.path.join(os.path.dirname(
#     os.path.realpath(__file__)), 'config.ini')
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
        reports.initialize(config)

        if defaultConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            defaultConfig['UpdateTableauDB'] = 'false'
            logger.info('UpdateTableauDB = ' +
                        str(defaultConfig.getboolean('UpdateTableauDB')))

            reports.generateTransportReport(
                'transport_report', startDate,  endDate, "Transport Report")
        else:
            previousMonth = (today_date.replace(day=1) -
                             timedelta(days=1)).replace(day=today_date.day)
            startDate = str(previousMonth)
            lastDay = calendar.monthrange(
                previousMonth.year, previousMonth.month)[1]
            endDate = str(previousMonth.replace(day=lastDay))
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            logger.info('UpdateTableauDB = ' +
                        str(defaultConfig.getboolean('UpdateTableauDB')))

            reports.generateTransportReport(
                'transport_report', startDate,  endDate, "Transport Report")

    except Exception as err:
        logger.exception(err)
        raise Exception(err)

    finally:
        logger.info("END of script - " +
                    datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
