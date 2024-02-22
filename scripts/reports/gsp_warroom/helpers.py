# Import built-in packages
from dateutil.relativedelta import relativedelta
import logging

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_warroom_report():

    report = OrionReport('GSP (NEW) War Room Report')
    report.add_email_receiver_to('teokokwee@singtel.com')
    report.add_email_receiver_to('petchiok@singtel.com')
    report.add_email_receiver_to('baoling@singtel.com')
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
    # Write to CSV
    csv_file = report.create_csv_from_df(df_raw, add_timestamp=True)
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
    # Write to CSV
    csv_file = report.create_csv_from_df(df_raw, add_timestamp=True)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.send_email()

    return
