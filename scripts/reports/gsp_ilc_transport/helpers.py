# Import built-in packages
import logging

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_ilc_transport_report():

    report = OrionReport('ILC Transport Report')
    report.set_filename('ilc_transport_report')
    report.set_prev_month_first_last_day_date()
    generate_report(report)


def generate_ilc_transport_billing_report():

    report = OrionReport('ILC Transport (Billing) Report')
    report.add_email_receiver_to('xv.hema.pawar@singtel.com')
    report.add_email_receiver_to('xv.abhijeet.navale@singtel.com')
    report.add_email_receiver_to('xv.chetna.deshmukh@singtel.com')
    report.add_email_receiver_to('maladzim@singtel.com')
    report.set_filename('ilc_transport_billing_report')
    report.set_gsp_billing_month_start_end_date()
    generate_report(report)


def generate_report(report: OrionReport):

    query = report.get_query_from_file("query.sql")
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    # Query to CSV
    csv_file = report.query_to_csv(
        formatted_query, add_timestamp=True, query_description="ILC records")
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.send_email()
