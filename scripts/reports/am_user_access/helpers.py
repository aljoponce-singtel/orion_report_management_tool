# Import built-in packages
import os
from datetime import datetime
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(configFile)

    email_subject = 'AM User Weekly Access Report'

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')
        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

    else:
        # Monday and Sunday date of previous week
        start_date, end_date = utils.get_prev_week_monday_sunday_date(
            datetime.now().date())

    logger.info("report start date: " + str(start_date))
    logger.info("report end date: " + str(end_date))
    logger.info("Generating AM User Access report ...")

    query = ("""
                SELECT
                    DISTINCT USR.username
                FROM
                    RestInterface_user USR
                WHERE
                    NOT (
                        USR.username = 'admin'
                        OR USR.username = 'gspuser@singtel.com'
                        OR USR.username = 'projectmanager@singtel.com'
                        OR USR.username = 'executivemanager@singtel.com'
                        OR USR.username = 'productmanager@singtel.com'
                        OR USR.username = 'accountmanager@singtel.com'
                        OR USR.username = 'queueowner@singtel.com'
                        OR USR.username = 'opsmanager'
                        OR USR.username = 'mluser@singtel.com'
                        OR USR.username = 'aljo.ponce@singtel.com'
                        OR USR.username = 'jiangxu@ncs.com.sg'
                        OR USR.username = 'adelinethk@singtel.com'
                        OR USR.username = 'jacob.toh@singtel.com'
                        OR USR.username = 'yuchen.liu@singtel.com'
                        OR USR.username = 'weiwang.thang@singtel.com'
                    )
                    AND USR.team = 'Account Manager'
                    AND DATE(USR.last_login) BETWEEN '{}'
                    AND '{}'
                ORDER BY
                    USR.username;
            """).format(start_date, end_date)

    logger.info("Querying db ...")
    result = report.orion_db.query_to_list(query)
    logger.info("Creating report ...")
    df = None

    if result:
        df = pd.DataFrame(result)
        # Add column name
        df.columns = ['username']
    else:
        # Create empty dataframe with 1 column
        df = pd.DataFrame(columns=['username'])

    # Send Email
    # Change starting index from 0 to 1 for proper table presentation
    df.index += 1

    email_body_text = """
        Hello,

        Please see attached ORION report.


        Thanks you and best regards,
        Orion Team
    """

    email_body_html = ("""\
        <html>
        <p>Hello,</p>
        <p>Please see AM user weekly access report from {} to {}.</p>
        <p>{}</p>
        <p>&nbsp;</p>
        <p>Thank you and best regards,</p>
        <p>Orion Team</p>
        </html>
        """).format(start_date, end_date, df.to_html())

    report.set_email_body_text(email_body_text)
    report.set_email_body_html(email_body_html)
    report.set_email_subject(report.add_timestamp(email_subject))
    report.send_email()

    return
