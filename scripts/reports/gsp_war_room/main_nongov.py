# import sys
import os
import logging
import configparser
import calendar
from datetime import datetime, timedelta
from scripts import log
from scripts import utils
from scripts.gsp_war_room import reports

config = configparser.ConfigParser()
configFile = 'scripts/gsp_war_room/config.ini'
config.read(configFile)
log.initialize(configFile)
defaultConfig = config['DEFAULT']
debugConfig = config['DEBUG']
logger = logging.getLogger(__name__)


def main():
    logger.info("==========================================")
    logger.info("START of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    logger.info("Running script in " + utils.getPlatform())

    today_date = datetime.now().date()

    try:
        reports.initialize(config)

        if debugConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = debugConfig['ReportStartDate']
            endDate = debugConfig['ReportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            reports.generateWarRoomNonGovReport(
                'gsp_warroom_nongov_report', startDate,  endDate, "GSP War Room (NonGov) Report")
        else:
            previousMonth = (today_date.replace(day=1) -
                             timedelta(days=1)).replace(day=today_date.day)
            startDate = str(previousMonth)
            lastDay = calendar.monthrange(
                previousMonth.year, previousMonth.month)[1]
            endDate = str(previousMonth.replace(day=lastDay))
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            reports.generateWarRoomNonGovReport(
                'gsp_warroom_nongov_report', startDate,  endDate, "GSP War Room (NonGov) Report")

    except Exception as err:
        logger.exception(err)
        raise Exception(err)

    finally:
        logger.info("END of script - " +
                    datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
