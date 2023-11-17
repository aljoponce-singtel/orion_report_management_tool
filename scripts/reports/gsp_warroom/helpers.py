# Import built-in packages
from dateutil.relativedelta import relativedelta
import logging

# Import third-party packages
import pandas as pd

# Import local packages
import constants as const
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_warroom_report():

    report = OrionReport('GSP (NEW) War Room Report')
    report.add_email_receiver_to('teokokwee@singtel.com')
    report.add_email_receiver_to('kinex.yeoh@singtel.com')
    report.add_email_receiver_to('ml-cssosdpe@singtel.com')
    report.add_email_receiver_to('ml-cssosmpeteam@singtel.com')
    report.add_email_receiver_to('ml-cpetm@singtel.com')
    report.set_filename('gsp_warroom_report')
    report.set_reporting_date()

    query = report.get_query_from_file("query.sql")
    formatted_query = query.format(
        report_date=report.report_date)
    df_raw = report.query_to_dataframe(
        formatted_query, query_description="warroom records", datetime_to_date=True)

    # Create new dataframe based from CRD_AMENDMENT_COLUMNS
    df_crd_amendment = df_raw[const.CRD_AMENDMENT_COLUMNS].drop_duplicates().dropna(
        subset=['crd_amendment_date'])

    # Sort records in ascending order by order_code and note_code
    df_crd_amendment = df_crd_amendment.sort_values(
        by=['order_code', 'note_code'], ascending=[True, False])

    # Keep the latest CRD amendment reason
    df_crd_amendment = df_crd_amendment.drop_duplicates(
        ['order_code'], keep='last')

    df_main = df_raw[const.MAIN_COLUMNS].drop_duplicates()
    # Sort records in ascending order by current_crd, order_code and step_no
    df_main = df_main.sort_values(
        by=['current_crd', 'order_code', 'step_no'], ascending=[False, True, True])

    # Rename the 'parameter_value' column to 'ed_pd_diversity'
    df_main = df_main.rename(columns={'parameter_value': 'ed_pd_diversity'})

    # (left) Join the df_main and df_crd_amendment dataframe through their order_code
    df_merged = pd.merge(df_main, df_crd_amendment.drop(columns=['note_code']),
                         how='left', on=['order_code'])

    # Write to CSV
    csv_file = report.create_csv_from_df(df_merged, add_timestamp=True)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.send_email()

    return


def generate_warroom_npp_report():

    report = OrionReport('GSP War Room NPP Report')
    report.add_email_receiver_cc('sulo@singtel.com')
    report.add_email_receiver_cc('ksha@singtel.com')
    report.add_email_receiver_cc('annesha@singtel.com')
    report.add_email_receiver_cc('kkchan@singtel.com')
    report.add_email_receiver_cc('sheila@singtel.com')
    report.add_email_receiver_cc('xv.abhijeet.navale@singtel.com')
    report.add_email_receiver_cc('xv.santoshbiradar.biradar@singtel.com')
    report.add_email_receiver_to('aileentan@singtel.com')
    report.add_email_receiver_to('kskuang@singtel.com')
    report.add_email_receiver_to('jamesgoh@singtel.com')
    report.add_email_receiver_to('xv.deepika.shukla@singtel.com')
    report.add_email_receiver_to('xv.ayan.nandy@singtel.com')
    report.add_email_receiver_to('xv.nogeranchitty.ramanchitty@singtel.com')
    report.add_email_receiver_to('poonam.mane@singtel.com')
    report.add_email_receiver_to('xv.varaddeshpande.deshpande@singtel.com')

    report.set_filename('gsp_warroom_npp_report')
    report.set_reporting_date()
    # Subtract 3 months
    report.set_start_date(report.report_date - relativedelta(months=3))
    # Add 3 months
    report.set_end_date(report.report_date + relativedelta(months=3))

    logger.info("report start date: " + str(report.start_date))
    logger.info("report end date: " + str(report.end_date))

    query = report.get_query_from_file("query_npp.sql")
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    df_raw = report.query_to_dataframe(
        formatted_query, query_description="warroom npp records")

    # Sort records in ascending order by order_code, parameter_name and step_no
    df_raw = df_raw.sort_values(
        by=['order_code', 'npp_level'], ascending=[True, True])

    # Write to CSV
    csv_file = report.create_csv_from_df(df_raw, add_timestamp=True)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.send_email()

    return
