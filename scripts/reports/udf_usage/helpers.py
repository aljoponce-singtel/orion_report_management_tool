# Import built-in packages
import os
import logging
from datetime import datetime, timedelta
from typing import List

# Import third-party packages
import pandas as pd

# Import local packages
import constants as const
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(config_file)
    report.set_report_name('UDF Usage Report')
    report.set_filename('udf_usage_report')
    report.set_prev_month_first_last_day_date()

    mondays = get_all_mondays(report.start_date, report.end_date)

    logger.info("Monday dates:")

    for monday in mondays:
        logger.info("  " + str(monday))

    query = f"""
                SELECT
                    DISTINCT DATE(created_at) AS login_date,
                    DAYNAME(created_at) AS day_name,
                    username
                FROM
                    RestInterface_authlog
                WHERE
                    NOT (
                        username = 'admin'
                        OR username = 'gspuser@singtel.com'
                        OR username = 'projectmanager@singtel.com'
                        OR username = 'executivemanager@singtel.com'
                        OR username = 'productmanager@singtel.com'
                        OR username = 'accountmanager@singtel.com'
                        OR username = 'queueowner@singtel.com'
                        OR username = 'opsmanager'
                        OR username = 'mluser@singtel.com'
                        OR username = 'aljo.ponce@singtel.com'
                        OR username = 'jiangxu@ncs.com.sg'
                        OR username = 'adelinethk@singtel.com'
                        OR username = 'jacob.toh@singtel.com'
                        OR username = 'yuchen.liu@singtel.com'
                        OR username = 'weiwang.thang@singtel.com'
                    )
                    AND user_type = 'Account Manager'
                    AND created_at BETWEEN '{report.start_date}'
                    AND '{report.end_date}'
                    AND status = 1
                ORDER BY
                    login_date,
                    username;
            """

    df_raw = report.query_to_dataframe(query)
    # Sort records in ascending order by login_date, and username
    df_raw = df_raw.sort_values(
        by=['login_date', 'username'], ascending=[True, True])
    # Main dataframe
    df_main = pd.DataFrame(columns=['Monday', 'Week of', 'Month', 'Name'])
    # Get the email address
    df_main['Name'] = df_raw['username']
    # output: e.g., "May '22"
    df_main['Month'] = pd.to_datetime(df_raw['login_date']).dt.strftime(
        "%b '%y")

    # update the 'Week of' column for the rows within the specified 'login_date' date range
    for monday in mondays:
        # # output: e.g., "22 May"
        # remove leading zero from day
        formatted_date = monday.strftime('%d' + ' %B').lstrip('0')

        day_suffix = ''

        # Split the date string into day and month
        day, month = formatted_date.split()
        day = int(day)

        # Check which suffix to append to the day value
        if 1 == day or 21 == day or 31 == day:
            day_suffix = 'st'
        elif 2 == day or 22 == day:
            day_suffix = 'nd'
        elif 3 == day or 23 == day:
            day_suffix = 'rd'
        else:
            day_suffix = 'th'

        # Append day_suffix to the day string
        day = str(day) + day_suffix

        # Join the day and month strings with a space
        formatted_date = ' '.join([day, month])

        # Assign a week range to 'Week of' column that belongs to the week of the Monday's date
        df_main.loc[(df_raw['login_date'] >= monday) & (
            df_raw['login_date'] <= monday + timedelta(days=6)), 'Week of'] = formatted_date

        # Assign a week range to 'Week of' column that belongs to the week of the Monday's date
        df_main.loc[(df_raw['login_date'] >= monday) & (
            df_raw['login_date'] <= monday + timedelta(days=6)), 'Monday'] = monday

    # Remove duplicate records
    df_main = df_main.drop_duplicates()
    # Sort records in ascending order by 'Week of', and 'Name'
    df_main = df_main.sort_values(
        by=['Monday', 'Name'], ascending=[True, True])
    # Reset index
    df_main = df_main.reset_index(drop=True)
    # Start index at 1
    df_main.index = df_main.index + 1
    # Reset the index again to copy the index value to '#' column
    df_main.reset_index(inplace=True)
    df_main.rename(columns={'index': '#'}, inplace=True)

    # Write to CSV for Warroom Report
    csv_file = report.create_csv_from_df(df_main[const.FINAL_COLUMNS])

    # Send Email
    # Change starting index from 0 to 1 for proper table presentation
    df_main.index += 1
    # Show the dataframe as a table in the email body
    email_body_html = f"""\
        <html>
        <p>Hello,</p>
        <p>Please see the attached ORION report.</p>
        <p>{df_main[['Week of', 'Month', 'Name']].to_html()}</p>
        <p>&nbsp;</p>
        <p>Best regards,</p>
        <p>The Orion Team</p>
        </html>
        """

    report.set_email_body_html(email_body_html)
    report.attach_file_to_email(csv_file)
    report.send_email()


def get_all_mondays(start_date, end_date) -> List[datetime]:
    """
    Returns a list of all the Mondays between the start_date and end_date (inclusive).
    """
    # Monday has weekday number 0
    all_mondays = []

    start_date_obj = datetime.strptime(str(start_date), '%Y-%m-%d').date()
    end_date_obj = datetime.strptime(str(end_date), '%Y-%m-%d').date()
    current_date_obj = start_date_obj

    # If the start date does not fall on a Monday,
    # get the Monday's date of the previous month
    if start_date_obj.weekday() != 0:
        all_mondays.append(get_previous_monday_date(start_date))

    while current_date_obj <= end_date_obj:
        if current_date_obj.weekday() == 0:
            all_mondays.append(current_date_obj)
        current_date_obj += timedelta(days=1)
    return all_mondays


def get_previous_monday_date(date):
    date_obj = datetime.strptime(str(date), '%Y-%m-%d').date()
    days_since_monday = (date_obj.weekday()) % 7
    previous_monday_date = date_obj - timedelta(days=days_since_monday)

    return previous_monday_date
