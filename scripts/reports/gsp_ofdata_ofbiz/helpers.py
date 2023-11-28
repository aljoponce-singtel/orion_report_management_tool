# Import built-in packages
from datetime import datetime
import logging

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_report():

    report = OrionReport('GSP OFData OFBiz')
    report.set_filename('gsp_ofdata_ofbiz')
    report.set_prev_week_monday_sunday_date()
    query = report.get_query_from_file("query.sql")
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    df = report.query_to_dataframe(formatted_query)

    if not df.empty:
        # Export the DataFrame to an Excel file
        # Keep the first row for duplicate records with the same 'OrderNo'
        df = df.drop_duplicates(subset=['OrderNo'], keep='first')

        # /* START - BizSeg */
        df_bizseg = df[df['Sector'].str.contains('BizSeg')]
        df_bizseg = df_bizseg[df_bizseg['ServiceType'].isin(
            ['CN - Meg@pop Suite Of IP Services', 'SingNet', 'ISDN'])]
        # Remove the 'Assignee' column
        df_bizseg = df_bizseg.drop('Assignee', axis=1)
        excel_bizseg_file = report.create_excel_from_df(
            df_bizseg, filename=("BSOFB_{}_TEST.xlsx").format(report.get_current_datetime(format="%d%m%y")))
        zip_bizseg_file = report.add_to_zip_file(excel_bizseg_file, zip_file_path=report.replace_ext_type(
            excel_bizseg_file, 'zip'), password=("BSOFB{}").format(get_date_password()))
        report.attach_file_to_email(zip_bizseg_file)
        # /* END - BizSeg */

        # /* START - EAG_GB */
        df_eag_gb_assignee = df[df['Sector'].str.contains(
            '|'.join(['SGO', 'Ent and Govt', 'Global Business']))]
        df_eag_gb_assignee = df_eag_gb_assignee[df_eag_gb_assignee['ServiceType'].isin(
            ['CN - Meg@pop Suite Of IP Services', 'SingNet', 'DigiNet', 'ISDN',
                'CN - ConnectPlus IP VPN', 'ConnectPlus E-Line', 'ILC', 'Software Defined Networking'])]

        # With 'Assignee' column
        excel_eag_gb_assignee_file = report.create_excel_from_df(
            df_eag_gb_assignee, filename=("GLEOFD_{}_PM_TEST.xlsx").format(report.get_current_datetime(format="%d%m%y")))
        zip_eag_gb_assignee_file = report.add_to_zip_file(excel_eag_gb_assignee_file, zip_file_path=report.replace_ext_type(
            excel_eag_gb_assignee_file, 'zip'), password=("GLEOFD{}").format(get_date_password()))
        report.attach_file_to_email(zip_eag_gb_assignee_file)

        # Without 'Assignee' column
        # Remove the 'Assignee' column
        df_eag_gb = df_eag_gb_assignee.drop('Assignee', axis=1)
        excel_eag_gb_file = report.create_excel_from_df(
            df_eag_gb, filename=("GLEOFD_{}_TEST.xlsx").format(report.get_current_datetime(format="%d%m%y")))
        zip_eag_gb_file = report.add_to_zip_file(excel_eag_gb_file, zip_file_path=report.replace_ext_type(
            excel_eag_gb_file, 'zip'), password=("GLEOFD{}").format(get_date_password()))
        report.attach_file_to_email(zip_eag_gb_file)
        # /* END - EAG_GB */

        # Option to generate raw file
        if report.debug_config.getboolean('include_raw_report'):
            excel_raw_file = generate_report_raw(report)
            report.attach_file_to_email(excel_raw_file)

    # Send Email
    report.send_email()

    return


def generate_report_raw(report: OrionReport):

    logger.info("Including raw report ...")
    query = report.get_query_from_file("query.sql")
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    excel_file = report.query_to_excel(
        formatted_query, query_description='raw records')

    return excel_file


def get_date_password():
    # Get the current date
    current_date = datetime.now()
    # Format the current date as "MMYYYY"
    formatted_date = current_date.strftime('%m%Y')
    # Replace the 4th character with '@'
    modified_date = formatted_date[:3] + '\@' + formatted_date[4:]

    return modified_date
