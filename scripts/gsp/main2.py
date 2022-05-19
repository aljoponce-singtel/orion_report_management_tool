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
config.read('scripts/gsp/config.ini')
defaultConfig = config['DEFAULT']


def main():
    log.initialize('scripts/gsp/config.ini')
    logger = logging.getLogger(__name__)

    today_date = datetime.now().date()

    try:
        if defaultConfig.getboolean('GenReportManually'):
            logger.info("==========================================")
            logger.info("START of script - " +
                        datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
            logger.info("Running script in " + utils.getPlatform())

            logger.info('\\* MANUAL RUN *\\')

            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
            logger.info("start date: " + str(startDate))
            logger.info("end date: " + str(endDate))

            # defaultConfig['UpdateTableauDB'] = 'false'
            logger.info('UpdateTableauDB = ' +
                        str(defaultConfig.getboolean('UpdateTableauDB')))

            reports.generateCPluseIpReport('cplusip_report', startDate,
                                           endDate, '', "CPlusIP Report", '')
            # reports.generateMegaPopReport('megapop_report', startDate,
            #                       endDate, '', "MegaPop Report", '')]
            # reports.generateSingnetReport('singnet_report', startDate,
            #                       endDate, '', "Singnet Report", '')
            # reports.generateStixReport('stix_report', startDate,
            #                    endDate, "STIX Report", '')
            # reports.generateInternetReport('internet_report', startDate,
            #                        endDate, "Internet Report", '')
            # reports.generateSDWANReport('sdwan_report', startDate,
            #                     endDate, "SDWAN Report", '')
            # reports.generateCPluseIpReportGrp(
            #     'cplusip_report_grp', startDate, endDate, '', "CPlusIP Report", '')
            # reports.generateMegaPopReportGrp(
            #     'megapop_report_grp', startDate, endDate, '', "MegaPop Report", '')
            # reports.generateSingnetReport(
            #     'singnet_report_apnic', startDate, endDate, 'gsdt7', "Singnet Report - GSP APNIC", 'teckchye@singtel.com;tao.taskrequest@singtel.com')
            # reports.generateSingnetReport(
            #     'singnet_report_connectportal', startDate, endDate, 'gsdt7', "Singnet Report – Connect Portal updating", 'kirti.vaish@singtel.com;sandeep.kumarrajendran@singtel.com')

            logger.info("END of script - " +
                        datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))

        else:
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
                logger.info("Running script in " + utils.getPlatform())

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
