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

    email_subject = 'GSP OFData OFBiz'
    filename = 'gsp_ofdata_ofbiz'

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')
        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

    else:
        # Monday and Sunday date of previous week
        start_date, end_date = utils.get_prev_week_start_end_date(
            datetime.now().date())

    logger.info("report start date: " + str(start_date))
    logger.info("report end date: " + str(end_date))
    logger.info("Generating OFData OFBiz report ...")

    query = ("""
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
                    AND ORD.close_date BETWEEN '{}'
                    AND '{}'
                ORDER BY
                    ORD.order_code;
            """).format(start_date, end_date)

    logger.info("Querying db ...")
    result = report.orion_db.query_to_list(query)

    logger.info("Creating report ...")
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
    # Export to excel file
    excel_bizseg_filename = ("BSOFB_{}_TEST.xlsx").format(
        utils.get_current_datetime(format="%d%m%y"))
    excel_bizseg_file_path = os.path.join(
        report.reports_folder_path, excel_bizseg_filename)
    # Set index=False if you don't want to export the index column
    df_bizseg.to_excel(excel_bizseg_file_path, index=False)
    # Add excel file to zip file and attach to email
    zip_bizseg_file_path = os.path.join(
        report.reports_folder_path, utils.replace_extension(excel_bizseg_filename, 'zip'))
    report.add_to_zip_file(excel_bizseg_file_path, zip_bizseg_file_path,
                           ("BSOFB{}").format(get_date_password()))
    report.attach_file_to_email(zip_bizseg_file_path)
    # /* END - BizSeg */

    # /* START - EAG_GB */
    df_eag_gb_assignee = df[df['Sector'].str.contains(
        '|'.join(['SGO', 'Ent and Govt', 'Global Business']))]
    df_eag_gb_assignee = df_eag_gb_assignee[df_eag_gb_assignee['ServiceType'].isin(
        ['CN - Meg@pop Suite Of IP Services', 'SingNet', 'DigiNet', 'ISDN',
         'CN - ConnectPlus IP VPN', 'ConnectPlus E-Line', 'ILC', 'Software Defined Networking'])]

    # With 'Assignee' column
    # Export to excel file
    excel_eag_gb_assignee_filename = ("GLEOFD_{}_PM_TEST.xlsx").format(
        utils.get_current_datetime(format="%d%m%y"))
    excel_eag_gb_assignee_file_path = os.path.join(
        report.reports_folder_path, excel_eag_gb_assignee_filename)
    # Set index=False if you don't want to export the index column
    df_eag_gb_assignee.to_excel(excel_eag_gb_assignee_file_path, index=False)
    # Add excel file to zip file and attach to email
    zip_eag_gb_assignee_file_path = os.path.join(
        report.reports_folder_path, utils.replace_extension(excel_eag_gb_assignee_filename, 'zip'))
    report.add_to_zip_file(excel_eag_gb_assignee_file_path,
                           zip_eag_gb_assignee_file_path, ("GLEOFD{}").format(get_date_password()))
    report.attach_file_to_email(zip_eag_gb_assignee_file_path)

    # Without 'Assignee' column
    # Remove the 'Assignee' column
    df_eag_gb = df_eag_gb_assignee.drop('Assignee', axis=1)
    # Export to excel file
    excel_eag_gb_filename = ("GLEOFD_{}_TEST.xlsx").format(
        utils.get_current_datetime(format="%d%m%y"))
    excel_eag_gb_file_path = os.path.join(
        report.reports_folder_path, excel_eag_gb_filename)
    # Set index=False if you don't want to export the index column
    df_eag_gb.to_excel(excel_eag_gb_file_path, index=False)
    # Add excel file to zip file and attach to email
    zip_eag_gb_file_path = os.path.join(
        report.reports_folder_path, utils.replace_extension(excel_eag_gb_filename, 'zip'))
    report.add_to_zip_file(excel_eag_gb_file_path, zip_eag_gb_file_path,
                           ("GLEOFD{}").format(get_date_password()))
    report.attach_file_to_email(zip_eag_gb_file_path)
    # /* END - EAG_GB */

    # Option to generate raw file
    if report.debug_config.getboolean('include_raw_report'):
        excel_raw_file_path = generate_report_raw(
            report, filename, start_date, end_date)
        report.attach_file_to_email(excel_raw_file_path)

        # /* START */
        # Export to excel file
        excel_filename = ("{}_{}.xlsx").format(
            filename, utils.get_current_datetime())
        excel_main_file_path = os.path.join(
            report.reports_folder_path, excel_filename)
        # Set index=False if you don't want to export the index column
        # df.to_excel(excel_main_file_path, index=False)
        # report.attach_file_to_email(excel_main_file_path)
        # /* END */

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.send_email()

    # Set a password to the excel file
    # excel_file = ("{}.xlsx").format(filename)
    # excel_file_path = os.path.join(report.reports_folder_path, excel_file)
    # password = report.debug_config['password']
    # utils.set_excel_password(
    #     excel_file_path, password, replace=False)

    return


def generate_report_raw(report, filename, start_date, end_date):

    query = ("""
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
                    AND ORD.close_date BETWEEN '{}'
                    AND '{}'
                ORDER BY
                    ORD.order_code,
                    CON.contact_type;
            """).format(start_date, end_date)

    logger.info("Querying db for raw report ...")
    result = report.orion_db.query_to_list(query)

    logger.info("Creating raw report ...")
    df = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # Convert columns to date
    for column in const.DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column]).dt.date

    # Export the DataFrame to an Excel file
    excel_filename = ("{}_raw_{}.xlsx").format(
        filename, utils.get_current_datetime())
    excel_main_file_path = os.path.join(
        report.reports_folder_path, excel_filename)
    # Set index=False if you don't want to export the index column
    df.to_excel(excel_main_file_path, index=False)

    return excel_main_file_path


def get_date_password():
    # Get the current date
    current_date = datetime.now()

    # Format the current date as "MMYYYY"
    formatted_date = current_date.strftime('%m%Y')

    # Replace the 4th character with '@'
    modified_date = formatted_date[:3] + '\@' + formatted_date[4:]

    return modified_date
