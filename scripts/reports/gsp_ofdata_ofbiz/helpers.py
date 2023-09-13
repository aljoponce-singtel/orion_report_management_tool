# Import built-in packages
import os
from datetime import datetime
import logging

# Import third-party packages
import pandas as pd

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(configFile)
    report.set_report_name('GSP OFData OFBiz')
    report.set_filename('gsp_ofdata_ofbiz')
    report.set_prev_week_monday_sunday_date()

    query = f"""
                SELECT
                    DISTINCT ORD.order_code AS 'OrderNo',
                    CUS.name AS 'CustomerName',
                    BRN.brn AS 'BRN',
                    PRD.network_product_desc AS 'ProductDescription',
                    ORD.ord_action_type AS 'OrderType',
                    ORD.order_status AS 'OrderStatus',
                    ORD.service_action_type AS 'ServiceActionType',
                    ORD.arbor_disp AS 'ServiceType',
                    ORD.business_sector AS 'Sector',
                    ORD.initial_crd AS 'InitialCRD',
                    ORD.close_date AS 'CloseDate',
                    ORD.current_crd AS 'CommissionDate',
                    ORD.taken_date AS 'OrdCreationDate',
                    PRJ.project_code AS 'ProjectID',
                    CON.family_name AS 'FirstName',
                    CON.given_name AS 'LastName',
                    CON.work_phone_no AS 'WorkPhoneNo',
                    CON.mobile_no AS 'MobileNo',
                    SITE.location AS 'Address',
                    ORD.assignee AS 'Assignee'
                FROM
                    (
                        SELECT
                            *
                        FROM
                            RestInterface_order
                        WHERE
                            id NOT IN (
                                SELECT
                                    DISTINCT order_id
                                FROM
                                    RestInterface_contactdetails
                                WHERE
                                    contact_type IN (
                                        'A-end-Cust',
                                        'Clarification-Cust',
                                        'Maintenance-Cust',
                                        'Technical-Cust'
                                    )
                                    AND email_address REGEXP '.*@singtel.com$'
                            )
                    ) ORD
                    JOIN RestInterface_contactdetails CON ON CON.order_id = ORD.id
                    AND CON.contact_type IN (
                        'A-end-Cust',
                        'Clarification-Cust',
                        'Maintenance-Cust',
                        'Technical-Cust'
                    )
                    AND (
                        (
                            CON.work_phone_no REGEXP '^[0-9]{{8}}$'
                            OR CON.work_phone_no REGEXP '^65[0-9]{{8}}$'
                            OR CON.work_phone_no REGEXP '^\\\\+65[0-9]{{8}}$'
                        )
                        OR (
                            CON.mobile_no REGEXP '^[0-9]{{8}}$'
                            OR CON.mobile_no REGEXP '^65[0-9]{{8}}$'
                            OR CON.mobile_no REGEXP '^\\\\+65[0-9]{{8}}$'
                        )
                    )
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = 'Mainline'
                    AND NPP.status != 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
                    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
                WHERE
                    ORD.service_action_type != 'Transfer SI'
                    AND ORD.ord_action_type != 'Contract Renewal '
                    AND (
                        ORD.business_sector LIKE '%bizseg%'
                        OR ORD.business_sector LIKE '%sgo%'
                        OR ORD.business_sector LIKE '%ent and govt%'
                        OR ORD.business_sector LIKE '%global business%'
                    )
                    AND ORD.arbor_disp IN (
                        'CN - Meg@pop Suite Of IP Services',
                        'SingNet',
                        'DigiNet',
                        'ISDN',
                        'CN - ConnectPlus IP VPN',
                        'ConnectPlus E-Line',
                        'ILC',
                        'Software Defined Networking'
                    )
                    AND ORD.order_status = 'Closed'
                    AND ORD.current_crd > DATE_SUB(ORD.close_date, INTERVAL 30 day)
                    AND ORD.current_crd < ORD.close_date
                    AND ORD.close_date BETWEEN '{report.start_date}'
                    AND '{report.end_date}'
                ORDER BY
                    ORD.order_code;
            """

    result = report.orion_db.query_to_list(query)
    df = pd.DataFrame(data=result, columns=const.MAIN_COLUMNS)
    # Convert columns to date
    for column in const.DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column]).dt.date
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
        df_bizseg, filename=("BSOFB_{}_TEST.xlsx").format(utils.get_current_datetime(format="%d%m%y")))
    zip_bizseg_file = report.add_to_zip_file(excel_bizseg_file, zip_file_path=utils.replace_extension(
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
        df_eag_gb_assignee, filename=("GLEOFD_{}_PM_TEST.xlsx").format(utils.get_current_datetime(format="%d%m%y")))
    zip_eag_gb_assignee_file = report.add_to_zip_file(excel_eag_gb_assignee_file, zip_file_path=utils.replace_extension(
        excel_eag_gb_assignee_file, 'zip'), password=("GLEOFD{}").format(get_date_password()))
    report.attach_file_to_email(zip_eag_gb_assignee_file)

    # Without 'Assignee' column
    # Remove the 'Assignee' column
    df_eag_gb = df_eag_gb_assignee.drop('Assignee', axis=1)
    excel_eag_gb_file = report.create_excel_from_df(
        df_eag_gb, filename=("GLEOFD_{}_TEST.xlsx").format(utils.get_current_datetime(format="%d%m%y")))
    zip_eag_gb_file = report.add_to_zip_file(excel_eag_gb_file, zip_file_path=utils.replace_extension(
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

    query = f"""
                SELECT
                    DISTINCT ORD.order_code AS 'OrderNo',
                    CUS.name AS 'CustomerName',
                    BRN.brn AS 'BRN',
                    PRD.network_product_desc AS 'ProductDescription',
                    ORD.ord_action_type AS 'OrderType',
                    ORD.order_status AS 'OrderStatus',
                    ORD.service_action_type AS 'ServiceActionType',
                    ORD.arbor_disp AS 'ServiceType',
                    ORD.business_sector AS 'Sector',
                    ORD.initial_crd AS 'InitialCRD',
                    ORD.close_date AS 'CloseDate',
                    ORD.current_crd AS 'CommissionDate',
                    ORD.taken_date AS 'OrdCreationDate',
                    PRJ.project_code AS 'ProjectID',
                    CON.contact_type AS 'ContactType',
                    CON.family_name AS 'FirstName',
                    CON.given_name AS 'LastName',
                    CON.work_phone_no AS 'WorkPhoneNo',
                    CON.mobile_no AS 'MobileNo',
                    CON.email_address AS 'Email',
                    SITE.location AS 'Address',
                    ORD.assignee AS 'Assignee'
                FROM
                    RestInterface_order ORD
                    LEFT JOIN RestInterface_contactdetails CON ON CON.order_id = ORD.id
                    AND CON.contact_type IN (
                        'A-end-Cust',
                        'Clarification-Cust',
                        'Maintenance-Cust',
                        'Technical-Cust'
                    )
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = 'Mainline'
                    AND NPP.status != 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
                    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
                WHERE
                    ORD.service_action_type != 'Transfer SI'
                    AND ORD.ord_action_type != 'Contract Renewal '
                    AND (
                        ORD.business_sector LIKE '%bizseg%'
                        OR ORD.business_sector LIKE '%sgo%'
                        OR ORD.business_sector LIKE '%ent and govt%'
                        OR ORD.business_sector LIKE '%global business%'
                    )
                    AND ORD.arbor_disp IN (
                        'CN - Meg@pop Suite Of IP Services',
                        'SingNet',
                        'DigiNet',
                        'ISDN',
                        'CN - ConnectPlus IP VPN',
                        'ConnectPlus E-Line',
                        'ILC',
                        'Software Defined Networking'
                    )
                    AND ORD.order_status = 'Closed'
                    AND ORD.current_crd > DATE_SUB(ORD.close_date, INTERVAL 30 day)
                    AND ORD.current_crd < ORD.close_date
                    AND ORD.close_date BETWEEN '{report.start_date}'
                    AND '{report.end_date}'
                ORDER BY
                    ORD.order_code,
                    CON.contact_type;
            """

    result = report.orion_db.query_to_list(
        query, query_description='raw records')
    df = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)
    # Convert columns to date
    for column in const.DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column]).dt.date
    # Export the DataFrame to an Excel file
    excel_file = report.create_excel_from_df(df)

    return excel_file


def get_date_password():
    # Get the current date
    current_date = datetime.now()
    # Format the current date as "MMYYYY"
    formatted_date = current_date.strftime('%m%Y')
    # Replace the 4th character with '@'
    modified_date = formatted_date[:3] + '\@' + formatted_date[4:]

    return modified_date
