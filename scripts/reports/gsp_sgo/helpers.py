# Import built-in packages
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.orion_report import OrionReport
from models import Sgo

logger = logging.getLogger(__name__)


def generate_sgo_report():

    report = OrionReport('SGO Report')
    report.set_filename('sgo_report')
    report.set_prev_month_first_last_day_date()
    # Create dataframe using a query from file
    df = query_to_dataframe(report, "query.sql")
    # Insert records to tableau db
    update_tableau_table(report, df)
    # Create and zip CSV file, and send to email
    send_report(report, df)


def generate_sgo_billing_report():

    report = OrionReport('SGO (Billing) Report')
    report.set_filename('sgo_billing_report')
    report.set_gsp_billing_month_start_end_date()
    # Create dataframe using a query from file
    df = query_to_dataframe(report, "query.sql")
    # Create and zip CSV file, and send to email
    send_report(report, df)


def query_to_dataframe(report: OrionReport, query_file=None):
    # get query string from file
    query = report.get_query_from_file(query_file)
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    df = report.query_to_dataframe(
        formatted_query, query_description="SGO records", datetime_to_date=True)

    # /* START */
    # There is no value in 'PO No' for 'NPP Level' == 'Mainline'.
    # It is only available when 'NPP Level' == 'VAS'
    # Since 'PONo' is required in the report, the value from VAS will be copied to the Mainline record,
    # even if it doesn't belong to this record.

    # Find the rows where NPPLevel is "VAS"
    df_vas = df[df['NPPLevel'] == 'VAS']

    # Iterate over the rows
    for index, row in df_vas.iterrows():
        # Get the WorkorderNo and PoNo values
        workorder = row['Workorder']
        po_number = row['PONo']

        # Update the corresponding row where NPP Level is "Mainline" and Workorder is the same
        df.loc[(df['NPPLevel'] == 'Mainline') & (
            df['Workorder'] == workorder), 'PONo'] = po_number
    # /* END */

    # Get the unique orders with VAS product
    vas_orders = df_vas['Workorder'].unique().tolist()
    # Get the unique orders with Mainline product
    mainline_orders = df[df['NPPLevel']
                         == 'Mainline']['Workorder'].unique().tolist()
    # Compare vas_orders and mainline_orders to get the orders without a Mainline product
    orders_wo_mainline = [
        element for element in vas_orders if element not in mainline_orders]

    # /* START */
    # Remove the row where 'NPPLevel' is 'VAS' but only for workorders than have a Mainline product
    # Check for VAS records
    condition1 = df['NPPLevel'] != 'VAS'
    # Check if the orders in df do not have a Mainline product
    condition2 = df['Workorder'].isin(orders_wo_mainline)
    # Use 'loc' to filter and extract records that satisfy both conditions
    df = df.loc[condition1 | condition2]
    # Remove the 'NPPLevel' column
    df = df.drop('NPPLevel', axis=1)
    # /* END */

    return df


def update_tableau_table(report: OrionReport, df: pd.DataFrame):
    # set empty values to null
    df.replace('', None)
    # insert records to DB
    report.insert_df_to_tableau_table(
        df, table_name=Sgo.__tablename__, table_model=Sgo)


def send_report(report: OrionReport, df: pd.DataFrame):
    # Write to CSV
    csv_file = report.create_csv_from_df(df, add_timestamp=True)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.send_email()
