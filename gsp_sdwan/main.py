"""GSP Reports

Author: P1319639

This script will generate SDWAN reports:

Add this crontab command for report scheduling:
15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh gsp_sdwan main
"""

import configparser
from logging.handlers import TimedRotatingFileHandler
import logging.config
import logging
import sys
import os
import smtplib
import csv
from datetime import datetime, timedelta
from zipfile import ZipFile
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pymysql

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to
# the sys.path.
sys.path.append(parent)

import utils
import reports


logger = logging.getLogger()
config = configparser.ConfigParser()
config.read('gsp_sdwan/config_sdwan.ini')
defaultConfig = config['DEFAULT']
emailConfig = config[defaultConfig['EmailInfo']]
dbConfig = config[defaultConfig['DatabaseEnv']]
engine = None
conn = None
csvFiles = []
reportsFolderPath = os.path.join(os.getcwd(), "reports")
pymysql.install_as_MySQLdb()

def main():
    # applies to all modules using this variable
    global logger
    logger = logging.getLogger()
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleFormat = logging.Formatter(
        fmt="%(asctime)s - %(module)s - %(levelname)s - %(message)s", datefmt="%T")
    consoleHandler.setFormatter(consoleFormat)
    fileHandler = TimedRotatingFileHandler('logs/gsp_sdwan.log', 'D', 1)
    fileFormat = logging.Formatter(
        fmt="%(asctime)s - %(module)s - %(levelname)s - %(message)s", datefmt="%F %a %T")
    fileHandler.setFormatter(fileFormat)
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(logging.DEBUG)

    today_date = datetime.now().date()

    logger.info("==========================================")
    logger.info("START of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    logger.info("Running script in " + utils.getPlatform())

    try:
        startDate = None
        endDate = None

        if defaultConfig.getboolean('GenReportManually'):
            logger.info('\\* MANUAL RUN *\\')
            startDate = defaultConfig['ReportStartDate']
            endDate = defaultConfig['ReportEndDate']
        else:
            startDate = str(today_date - timedelta(days=7))
            endDate = str(today_date - timedelta(days=1))

        logger.info("start date: " + str(startDate))
        logger.info("end date: " + str(endDate))

        reports.generateSDWANReport('sdwan_weekly_report', startDate,
                            endDate, "SDWAN Weekly Report", '')

    except Exception as err:
        logger.error(err)

    logger.info("END of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()
