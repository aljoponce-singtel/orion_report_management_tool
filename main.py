from reports import *
import sys
from utils import *
import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
import calendar
import configparser
from datetime import datetime, timedelta

logger = logging.getLogger()
config = configparser.ConfigParser()
config.read('config.ini')
defaultConfig = config['DEFAULT']


def main():
    global logger
    logger = logging.getLogger()
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleFormat = logging.Formatter(
        fmt="%(asctime)s - %(module)s - %(levelname)s - %(message)s", datefmt="%T")
    consoleHandler.setFormatter(consoleFormat)
    fileHandler = TimedRotatingFileHandler("logs/gsp.log")
    fileFormat = logging.Formatter(
        fmt="%(asctime)s - %(module)s - %(levelname)s - %(message)s", datefmt="%F %a %T")
    fileHandler.setFormatter(fileFormat)
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(logging.DEBUG)

    today_date = datetime.now().date()

    try:
        if defaultConfig.getboolean('GenReportManually'):
            logger.info("==========================================")
            logger.info("START of script - " +
                        datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
            logger.info("Running script in " + utils.getPlatform())

            logger.info('\\* TEST RUN *\\')

            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            # defaultConfig['UpdateTableauDB'] = 'false'
            logger.info('UpdateTableauDB = ' +
                        str(defaultConfig.getboolean('UpdateTableauDB')))

            generateCPluseIpReport('cplusip_report', startDate,
                                   endDate, '', "CPlusIP Report", '')
            # generateMegaPopReport('megapop_report', startDate,
            #                       endDate, '', "MegaPop Report", '')]
            # generateSingnetReport('singnet_report', startDate,
            #                       endDate, '', "Singnet Report", '')
            # generateStixReport('stix_report', startDate,
            #                    endDate, "STIX Report", '')
            # generateInternetReport('internet_report', startDate,
            #                        endDate, "Internet Report", '')
            # generateSDWANReport('sdwan_report', startDate,
            #                     endDate, "SDWAN Report", '')
            # generateCPluseIpReportGrp(
            #     'cplusip_report_grp', startDate, endDate, '', "CPlusIP Report", '')
            # generateMegaPopReportGrp(
            #     'megapop_report_grp', startDate, endDate, '', "MegaPop Report", '')
            # generateSingnetReport(
            #     'singnet_report_apnic', startDate, endDate, 'gsdt7', "Singnet Report - GSP APNIC", 'teckchye@singtel.com;tao.taskrequest@singtel.com')
            # generateSingnetReport(
            #     'singnet_report_connectportal', startDate, endDate, 'gsdt7', "Singnet Report – Connect Portal updating", 'kirti.vaish@singtel.com;sandeep.kumarrajendran@singtel.com')

            logger.info("END of script - " +
                        datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))

        else:

            #-- START --#
            # If the day falls on the 1st day of the month
            # start date = 1st day of the month
            # end date = last day of the month

            # TEST DATA
            # today_date = datetime.now().date().replace(day=1)
            # print("date today: " + str(today_date))

            if today_date.day == 1:
                logger.info(
                    "==========================================")
                logger.info("START of script - " +
                            datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
                logger.info("Running script in " + utils.getPlatform())

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

                generateCPluseIpReport('cplusip_report', startDate,
                                       endDate, '', "CPlusIP Report", 'karthik.manjunath@singtel.com')
                generateMegaPopReport('megapop_report', startDate,
                                      endDate, '', "MegaPop Report", 'karthik.manjunath@singtel.com')
                generateSingnetReport('singnet_report', startDate,
                                      endDate, '', "Singnet Report", '')
                generateStixReport('stix_report', startDate,
                                   endDate, "STIX Report", '')
                generateInternetReport('internet_report', startDate,
                                       endDate, "Internet Report", '')
                generateSDWANReport('sdwan_report', startDate,
                                    endDate, "SDWAN Report", '')

                logger.info(
                    "END of script - " + datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))

            #-- END --#

            #-- START --#
            # If the day falls on the 26th of the month
            # start date = 26th day of the previous month
            # end date = 25th day of the current month

            # TEST DATA
            # today_date = datetime.now().date().replace(day=26)
            # print("date today: " + str(today_date))

            if today_date.day == 26:
                logger.info(
                    "==========================================")
                logger.info("START of script - " +
                            datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
                logger.info("Running script in " + getPlatform())

                previousMonth = (today_date.replace(day=1) -
                                 timedelta(days=1)).replace(day=today_date.day)
                startDate = str(previousMonth)
                endDate = str(today_date - timedelta(days=1))
                logger.info("start date: " + str(startDate))
                logger.info("end date: " + str(endDate))

                defaultConfig['UpdateTableauDB'] = 'false'
                logger.info('UpdateTableauDB = ' +
                            str(defaultConfig.getboolean('UpdateTableauDB')))

                generateCPluseIpReportGrp(
                    'cplusip_report_grp', startDate, endDate, '', "CPlusIP Report", 'karthik.manjunath@singtel.com')
                generateMegaPopReportGrp(
                    'megapop_report_grp', startDate, endDate, '', "MegaPop Report", 'karthik.manjunath@singtel.com')

                logger.info(
                    "END of script - " + datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))

            #-- END --#

            #-- START --#
            # If the day falls on a Monday
            # start date = date of the previous Monday (T-7)
            # end date = date of the previous Sunday (T-1)

            # TEST DATA
            # today_date = datetime.now().date().replace(day=7)
            # print("date today: " + str(today_date))

            if today_date.isoweekday() == 1:  # Monday
                logger.info(
                    "==========================================")
                logger.info("START of script - " +
                            datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
                logger.info("Running script in " + getPlatform())

                startDate = str(today_date - timedelta(days=7))
                endDate = str(today_date - timedelta(days=1))
                logger.info("start date: " + str(startDate))
                logger.info("end date: " + str(endDate))

                defaultConfig['UpdateTableauDB'] = 'false'
                logger.info('UpdateTableauDB = ' +
                            str(defaultConfig.getboolean('UpdateTableauDB')))

                generateSingnetReport(
                    'singnet_report_apnic', startDate, endDate, 'gsdt7', "Singnet Report - GSP APNIC", 'teckchye@singtel.com;tao.taskrequest@singtel.com')
                generateSingnetReport(
                    'singnet_report_connectportal', startDate, endDate, 'gsdt7', "Singnet Report – Connect Portal updating", 'kirti.vaish@singtel.com;tao.taskrequest@singtel.com')

                logger.info(
                    "END of script - " + datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))

            #-- END --#

    except Exception as err:
        logger.error(err)
        logger.info("END of script - " +
                    datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
