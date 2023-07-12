# Import built-in packages
import os
from datetime import datetime, timedelta
import logging

# Import third-party packages
import pandas as pd
import openpyxl

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(configFile)

    email_subject = 'GSP OFData OFBiz'
    filename = 'gsp_ofdata_ofbiz'
    report_date = None

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')
        report_date = report.debug_config['report_date']

    else:
        report_date = datetime.now().date()

    logger.info("report date: " + str(report_date))
    logger.info("Generating OFData OFBiz report ...")

    logger.info("Querying db ...")
    # result = report.orion_db.query_to_list(query)

    logger.info("Creating SGO report ...")

    # Set a password to the excel file
    excel_file = ("{}.xlsx").format(filename)
    excel_file_path = os.path.join(report.reports_folder_path, excel_file)
    password = report.debug_config['password']
    utils.set_excel_password(
        excel_file_path, password, replace=False)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.attach_file_to_email(excel_file_path)
    report.send_email()

    return
