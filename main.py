from utils import *
from reports import *
import logging
import os
import calendar
import configparser
from datetime import datetime, timedelta

config = configparser.ConfigParser()
config.read('config.ini')
defaultConfig = config['DEFAULT']
logsFolderPath = os.path.join(os.getcwd(), "logs")

logging.basicConfig(handlers=[logging.FileHandler(filename=os.path.join(logsFolderPath, "reports.log"),
                                                  encoding='utf-8', mode='a+')],
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt="%F %a %T",
                    level=logging.INFO)


def main():
    today_date = datetime.now().date()

    try:
        if defaultConfig.getboolean('GenReportManually'):
            printAndLogMessage("==========================================")
            printAndLogMessage("START of script - " +
                               datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
            printAndLogMessage("Running script in " + getPlatform())

            printAndLogMessage('\\* TEST RUN *\\')

            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
            printAndLogMessage("start date: " + str(startDate))
            printAndLogMessage("end date: " + str(endDate))

            # defaultConfig['UpdateTableauDB'] = 'false'
            printAndLogMessage('UpdateTableauDB = ' +
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

            printAndLogMessage("END of script - " +
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
                printAndLogMessage(
                    "==========================================")
                printAndLogMessage("START of script - " +
                                   datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
                printAndLogMessage("Running script in " + getPlatform())

                previousMonth = (today_date.replace(day=1) -
                                 timedelta(days=1)).replace(day=today_date.day)
                startDate = str(previousMonth)
                lastDay = calendar.monthrange(
                    previousMonth.year, previousMonth.month)[1]
                endDate = str(previousMonth.replace(day=lastDay))
                printAndLogMessage("start date: " + str(startDate))
                printAndLogMessage("end date: " + str(endDate))

                printAndLogMessage('UpdateTableauDB = ' +
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

                printAndLogMessage(
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
                printAndLogMessage(
                    "==========================================")
                printAndLogMessage("START of script - " +
                                   datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
                printAndLogMessage("Running script in " + getPlatform())

                previousMonth = (today_date.replace(day=1) -
                                 timedelta(days=1)).replace(day=today_date.day)
                startDate = str(previousMonth)
                endDate = str(today_date - timedelta(days=1))
                printAndLogMessage("start date: " + str(startDate))
                printAndLogMessage("end date: " + str(endDate))

                defaultConfig['UpdateTableauDB'] = 'false'
                printAndLogMessage('UpdateTableauDB = ' +
                                   str(defaultConfig.getboolean('UpdateTableauDB')))

                generateCPluseIpReportGrp(
                    'cplusip_report_grp', startDate, endDate, '', "CPlusIP Report", 'karthik.manjunath@singtel.com')
                generateMegaPopReportGrp(
                    'megapop_report_grp', startDate, endDate, '', "MegaPop Report", 'karthik.manjunath@singtel.com')

                printAndLogMessage(
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
                printAndLogMessage(
                    "==========================================")
                printAndLogMessage("START of script - " +
                                   datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
                printAndLogMessage("Running script in " + getPlatform())

                startDate = str(today_date - timedelta(days=7))
                endDate = str(today_date - timedelta(days=1))
                printAndLogMessage("start date: " + str(startDate))
                printAndLogMessage("end date: " + str(endDate))

                defaultConfig['UpdateTableauDB'] = 'false'
                printAndLogMessage('UpdateTableauDB = ' +
                                   str(defaultConfig.getboolean('UpdateTableauDB')))

                generateSingnetReport(
                    'singnet_report_apnic', startDate, endDate, 'gsdt7', "Singnet Report - GSP APNIC", 'teckchye@singtel.com;tao.taskrequest@singtel.com')
                generateSingnetReport(
                    'singnet_report_connectportal', startDate, endDate, 'gsdt7', "Singnet Report – Connect Portal updating", 'kirti.vaish@singtel.com;tao.taskrequest@singtel.com')

                printAndLogMessage(
                    "END of script - " + datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))

            #-- END --#

    except Exception as err:
        printAndLogError(err)
        printAndLogMessage("END of script - " +
                           datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
