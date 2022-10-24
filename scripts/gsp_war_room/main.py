import os
import logging
from datetime import datetime
from scripts import utils
from scripts.gsp_war_room import reports

logger = logging.getLogger(__name__)

configFile = os.path.join(os.path.dirname(__file__), 'config.ini')
orionReport = reports.initialize(configFile)


def main():
    logger.info("==========================================")
    logger.info("START of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    logger.info("Running script in " + utils.getPlatform())

    try:

        if orionReport.debugConfig.getboolean('genReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = orionReport.debugConfig['reportStartDate']
            endDate = orionReport.debugConfig['reportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            reports.generateWarRoomReport(
                'gsp_warroom_report', startDate,  endDate, "GSP War Room Report")
        else:
            startDate = str(utils.getPrevMonthFirstDayDate(
                datetime.now().date()))
            endDate = str(utils.getPrevMonthLastDayDate(datetime.now().date()))
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            reports.generateWarRoomReport(
                'gsp_warroom_report', startDate,  endDate, "GSP War Room Report")

    except Exception as err:
        logger.exception(err)
        raise Exception(err)

    finally:
        logger.info("END of script - " +
                    datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
