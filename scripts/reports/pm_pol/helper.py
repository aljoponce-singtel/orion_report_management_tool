# Import built-in packages
import logging
from datetime import datetime

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_pol_report():

    report = OrionReport('[ORION POL Report]')
    report.set_filename('POL_PM_Managed')
    query = report.get_query_from_file("query.sql")
    csv_file = report.query_to_csv(
        query, filename=f"{report.filename}_{get_file_timestamp()}.csv")
    zip_file = report.add_to_zip_file(
        csv_file, zip_filename=f"{report.filename}_{get_file_timestamp()}.zip")
    report.attach_file_to_email(zip_file)
    report.set_email_subject(f"{report.report_name} {get_email_timestamp()}")
    report.set_email_body_html(get_email_body_html())
    report.send_email()


def generate_pol_non_pm_report():

    report = OrionReport('[ORION POL (Non-PM) Report]')
    report.set_filename('POL_Non_PM_Managed')
    query = report.get_query_from_file("query_non_pm.sql")
    csv_file = report.query_to_csv(
        query, filename=f"{report.filename}_{get_file_timestamp()}.csv")
    zip_file = report.add_to_zip_file(
        csv_file, zip_filename=f"{report.filename}_{get_file_timestamp()}.zip")
    report.attach_file_to_email(zip_file)
    report.set_email_subject(f"{report.report_name} {get_email_timestamp()}")
    report.set_email_body_html(get_email_body_html())
    report.send_email()


def generate_pol_monthly_report():

    report = OrionReport('[ORION Monthly POL Report]')
    report.set_filename('POL_Monthly_PM_Managed')
    report.set_prev_month_first_last_day_date()
    query = report.get_query_from_file("query_monthly.sql")
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    csv_file = report.query_to_csv(
        formatted_query, filename=f"{report.filename}_{get_file_timestamp()}.csv")
    zip_file = report.add_to_zip_file(
        csv_file, zip_filename=f"{report.filename}_{get_file_timestamp()}.zip")
    report.attach_file_to_email(zip_file)
    report.set_email_subject(f"{report.report_name} {get_email_timestamp()}")
    report.set_email_body_html(get_email_body_html())
    report.add_email_receiver_to("nadianurul@singtel.com")
    report.send_email()


# Legacy/deprecated function
# sample format - '311023_1355'
def get_file_timestamp():

    return datetime.now().strftime("%d%m%y_%H%M")


# Legacy/deprecated function
# sample format - '2023oct31 9am'
def get_email_timestamp():

    today_datetime = datetime.now()
    day = today_datetime.strftime('%d').lstrip('0')
    hour = today_datetime.strftime('%I').lstrip('0')
    ampm = today_datetime.strftime('%p').lower()
    year = today_datetime.strftime('%Y')
    month = today_datetime.strftime('%b').lower()

    return f"{year}{month}{day} {hour}{ampm}"


def get_email_body_html():

    email_body_html = """\
        <html>
        <p>Hi All,</p>
        <p>Please have the attached ORION report.</p>
        <p>&nbsp;</p>
        <p>Thanks and Regards</p>
        <p>Muhammad Siddique</p>
        </html>
        """

    return email_body_html
