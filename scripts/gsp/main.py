"""GSP Reports

Author: P1319639

This script will generate the following reports:
- CPluseIp
- MegaPop
- Singnet
- Stix
- Internet
- SDWAN

Add this crontab command for report scheduling:
15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh gsp main
"""

import logging
import calendar
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

    today_date = datetime.now().date()

    try:
        reports.loadConfig(config)

        if defaultConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            defaultConfig['UpdateTableauDB'] = 'false'
            logger.info('UpdateTableauDB = ' +
                        str(defaultConfig.getboolean('UpdateTableauDB')))

            reports.generateCPluseIpReport(
                'cplusip_report', startDate,  endDate, '', "CPlusIP Report")
            # reports.generateMegaPopReport(
            #     'megapop_report', startDate, endDate, '', "MegaPop Report")
            # reports.generateSingnetReport(
            #     'singnet_report', startDate, endDate, '', "Singnet Report")
            # reports.generateStixReport(
            #     'stix_report', startDate, endDate, "STIX Report")
            # reports.generateInternetReport(
            #     'internet_report', startDate, endDate, "Internet Report")
            # reports.generateSDWANReport(
            #     'sdwan_report', startDate, endDate, "SDWAN Report")
            # reports.generateCPluseIpReportGrp(
            #     'cplusip_report_grp', startDate, endDate, '', "CPlusIP Report")
            # reports.generateMegaPopReportGrp(
            #     'megapop_report_grp', startDate, endDate, '', "MegaPop Report")

        else:
            #-- START --#
            # If the day falls on the 1st day of the month
            # start date = 1st day of the month
            # end date = last day of the month

            # TEST DATA
            # today_date = datetime.now().date().replace(day=1)
            # print("date today: " + str(today_date))

            if today_date.day == 1:
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

                reports.generateSingnetReport(
                    'singnet_report', startDate, endDate, '', "Singnet Report")
                reports.generateStixReport(
                    'stix_report', startDate, endDate, "STIX Report")
                reports.generateInternetReport(
                    'internet_report', startDate, endDate, "Internet Report")
                reports.generateSDWANReport(
                    'sdwan_report', startDate, endDate, "SDWAN Report")

                # add email recepients to config file
                emailReceiver = config['Email']['receiverTo']
                emailReceiverToAdd = ';karthik.manjunath@singtel.com'
                config['Email']['receiverTo'] = emailReceiver + \
                    emailReceiverToAdd

                reports.generateCPluseIpReport(
                    'cplusip_report', startDate, endDate, '', "CPlusIP Report")
                reports.generateMegaPopReport(
                    'megapop_report', startDate, endDate, '', "MegaPop Report")

            #-- END --#

            #-- START --#
            # If the day falls on the 26th of the month
            # start date = 26th day of the previous month
            # end date = 25th day of the current month

            # TEST DATA
            # today_date = datetime.now().date().replace(day=26)
            # print("date today: " + str(today_date))

            if today_date.day == 26:
                previousMonth = (today_date.replace(day=1) -
                                 timedelta(days=1)).replace(day=today_date.day)
                startDate = str(previousMonth)
                endDate = str(today_date - timedelta(days=1))
                logger.info("start date: " + str(startDate))
                logger.info("end date: " + str(endDate))

                defaultConfig['UpdateTableauDB'] = 'false'
                logger.info('UpdateTableauDB = ' +
                            str(defaultConfig.getboolean('UpdateTableauDB')))

                # add additional email recepients to config file
                emailReceiver = config['Email']['receiverTo']
                emailReceiverToAdd = ';karthik.manjunath@singtel.com'
                config['Email']['receiverTo'] = emailReceiver + \
                    emailReceiverToAdd

                reports.generateCPluseIpReportGrp(
                    'cplusip_report_grp', startDate, endDate, '', "CPlusIP Report")
                reports.generateMegaPopReportGrp(
                    'megapop_report_grp', startDate, endDate, '', "MegaPop Report")

            #-- END --#

    except Exception as err:
        logger.exception(err)
        raise Exception(err)

    finally:
        logger.info("END of script - " +
                    datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
