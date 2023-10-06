# Import built-in packages
import os
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(configFile, 'AM User Weekly Access Report')
    report.set_prev_week_monday_sunday_date()

    query = f"""
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
                        OR USR.username = 'weiwang.thang@singtel.com'
                    )
                    AND USR.team = 'Account Manager'
                    AND DATE(USR.last_login) BETWEEN '{report.start_date}'
                    AND '{report.end_date}'
                ORDER BY
                    USR.username;
            """

    df = report.query_to_dataframe(query)

    if not df.empty:
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

    email_body_html = f"""\
        <html>
        <p>Hello,</p>
        <p>Please see AM user weekly access report from {report.start_date} to {report.end_date}.</p>
        <p>{df.to_html()}</p>
        <p>&nbsp;</p>
        <p>Thank you and best regards,</p>
        <p>Orion Team</p>
        </html>
        """

    report.set_email_body_text(email_body_text)
    report.set_email_body_html(email_body_html)
    report.send_email()

    return
