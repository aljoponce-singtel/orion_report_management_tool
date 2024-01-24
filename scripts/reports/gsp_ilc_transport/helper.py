# Import built-in packages
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.orion_report import OrionReport
from models import IlcTransport

logger = logging.getLogger(__name__)


def generate_ilc_transport_report():

    report = OrionReport('ILC Transport Report')
    report.set_filename('ilc_transport_report')
    report.set_prev_month_first_last_day_date()
    # Create dataframe using a query from file
    df = query_to_dataframe(report, "query.sql")
    # Insert records to tableau db
    update_tableau_table(report, df)
    # Create and zip CSV file, and send to email
    send_report(report, df)


def generate_ilc_transport_billing_report():

    report = OrionReport('ILC Transport (Billing) Report')
    report.add_email_receiver_to('xv.hema.pawar@singtel.com')
    report.add_email_receiver_to('xv.abhijeet.navale@singtel.com')
    report.add_email_receiver_to('xv.chetna.deshmukh@singtel.com')
    report.add_email_receiver_to('maladzim@singtel.com')
    report.set_filename('ilc_transport_billing_report')
    report.set_gsp_billing_month_start_end_date()
    # Create dataframe using a query from file
    df = query_to_dataframe(report, "query.sql")
    # Create and zip CSV file, and send to email
    send_report(report, df)


def query_to_dataframe(report: OrionReport, query_file=None):
    query = report.get_query_from_file(query_file)
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    df = report.query_to_dataframe(
        formatted_query, query_description="ILC records", datetime_to_date=True)
    return df


def update_tableau_table(report: OrionReport, df: pd.DataFrame):
    # add new column
    df["update_time"] = pd.Timestamp.now()
    # set empty values to null
    df.replace('', None)
    # insert records to DB
    report.insert_df_to_tableau_table(
        df, table_name=IlcTransport.__tablename__, table_model=IlcTransport)


def send_report(report: OrionReport, df: pd.DataFrame):
    # Write to CSV
    csv_file = report.create_csv_from_df(df, add_timestamp=True)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.send_email()
