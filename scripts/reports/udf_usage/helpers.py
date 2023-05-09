# Import built-in packages
import os
from datetime import datetime, timedelta
import logging

# Import third-party packages
import numpy as np
import pandas as pd
from sqlalchemy import select, case, and_, or_, null, func

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(configFile)

    email_subject = 'UDF Usage Report'
    filename = 'udf_usage_report'
    start_date = None
    end_date = None

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

    else:
        start_date = utils.get_first_day_from_prev_month(
            datetime.now().date())
        end_date = utils.get_last_day_from_prev_month(datetime.now().date())

    logger.info("report start date: " + str(start_date))
    logger.info("report end date: " + str(end_date))

    logger.info("Generating report ...")

    mondays = get_all_mondays(start_date, end_date)

    logger.info("Monday dates:")

    for monday in mondays:
        logger.info("  " + str(monday))

    query = ("""
                SELECT
                    DISTINCT CAST(created_at AS DATE) AS login_date,
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
                    AND created_at BETWEEN '{}'
                    AND '{}'
                    AND status = 1
                ORDER BY
                    login_date,
                    username;
            """).format(start_date, end_date)

    result = report.orion_db.query_to_list(query)

    df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # convert to datetime
    df_raw['login_date'] = pd.to_datetime(df_raw['login_date'])

    # Sort records in ascending order by login_date, and username
    df_raw = df_raw.sort_values(
        by=['login_date', 'username'], ascending=[True, True])

    df_main = pd.DataFrame(columns=const.MAIN_COLUMNS)

    # Get the email address
    df_main['Name'] = df_raw['username']
    # output: e.g., "May '22"
    df_main['Month'] = df_raw['login_date'].dt.strftime(
        "%b '%y")

    # convert datetime to date (remove time)
    df_raw['login_date'] = pd.to_datetime(
        df_raw['login_date']).dt.date

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
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(df_main[const.FINAL_COLUMNS], csv_main_file_path)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.attach_file_to_email(csv_main_file_path)
    report.send_email()


def get_all_mondays(start_date_str, end_date_str):
    """
    Returns a list of all the Mondays between the start_date and end_date (inclusive).
    """
    # Monday has weekday number 0
    all_mondays = []

    start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    current_date_obj = start_date_obj

    # If the start date does not fall on a Monday,
    # get the Monday's date of the previous month
    if start_date_obj.weekday() != 0:
        all_mondays.append(get_previous_monday_date(start_date_str))

    while current_date_obj <= end_date_obj:
        if current_date_obj.weekday() == 0:
            all_mondays.append(current_date_obj)
        current_date_obj += timedelta(days=1)
    return all_mondays


def get_previous_monday_date(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
    days_since_monday = (date_obj.weekday()) % 7
    previous_monday_date = date_obj - timedelta(days=days_since_monday)

    return previous_monday_date
