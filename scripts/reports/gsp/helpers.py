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


def generate_cplus_ip_report():

    report = OrionReport(configFile)

    email_subject = 'CPlusIP Report'
    filename = 'cplusip_report'
    start_date = None
    end_date = None

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

    else:
        # 1st of the month
        start_date, end_date = utils.get_prev_month_first_last_day_date(
            datetime.now().date())

    logger.info("Generating CPlusIP Report ...")

    logger.info("report start date: " + str(start_date))
    logger.info("report end date: " + str(end_date))

    cnp_act_list = [
        'Change C+ IP',
        'De-Activate C+ IP',
        'DeActivate Video Exch Svc',
        'LLC Received from Partner',
        'LLC Accepted by Singtel',
        'Activate C+ IP',
        'Cease Resale SGO',
        'OLLC Site Survey',
        'De-Activate TOPSAA on PE',
        'De-Activate RAS',
        'De-Activate RLAN on PE',
        'Pre-Configuration on PE',
        'De-Activate RMS on PE',
        'GSDT Co-ordination Work',
        'Change Resale SGO',
        'Pre-Configuration',
        'Cease MSS VPN',
        'Recovery - PNOC Work',
        'De-Activate RMS for IP/EV',
        'GSDT Co-ordination OS LLC',
        'Change RAS',
        'Extranet Config',
        'Cease Resale SGO JP',
        'm2m EVC Provisioning',
        'Activate RMS/TOPS IP/EV',
        'Config MSS VPN',
        'De-Activate RMS on CE-BQ',
        'OLLC Order Ack',
        'Cease Resale SGO CHN'
    ]

    gsdt6_act_list = [
        'GSDT Co-ordination Work',
        'De-Activate C+ IP',
        'Cease Monitoring of IPPBX',
        'GSDT Co-ordination OS LLC',
        'GSDT Partner Cloud Access',
        'Cease In-Ctry Data Pro',
        'Change Resale SGO',
        'Ch/Modify In-Country Data',
        'De-Activate RMS on PE',
        'Disconnect RMS for FR',
        'Change C+ IP',
        'Activate C+ IP',
        'LLC Accepted by Singtel',
        'GSDT Co-ordination - BQ',
        'LLC Received from Partner',
        'In-Country Data Product',
        'OLLC Site Survey',
        'GSDT Co-ordination-RMS',
        'Pre-Configuration on PE',
        'Cease Resale SGO',
        'Disconnect RMS for ATM'
    ]

    query = f"""
                SELECT
                    DISTINCT ORD.order_code,
                    ORD.service_number,
                    PRD.network_product_code,
                    PRD.network_product_desc,
                    CUS.name AS customer_name,
                    ORD.order_type,
                    ORD.current_crd,
                    ORD.taken_date,
                    PER.role AS group_id,
                    CAST(ACT.activity_code AS SIGNED INTEGER) AS act_step_no,
                    ACT.name AS act_name,
                    ACT.due_date AS act_due_date,
                    ACT.ready_date AS act_rdy_date,
                    DATE(ACT.exe_date) AS act_exe_date,
                    DATE(ACT.dly_date) AS act_dly_date,
                    ACT.completed_date AS act_com_date
                FROM
                    RestInterface_order ORD
                    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                    JOIN RestInterface_person PER ON PER.id = ACT.person_id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                    AND NPP.level = 'MainLine'
                    AND NPP.status != 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                WHERE
                    ORD.id IN (
                        SELECT
                            DISTINCT ORD.id
                        FROM
                            RestInterface_order ORD
                            JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                            JOIN RestInterface_person PER ON PER.id = ACT.person_id
                        WHERE
                            PER.role LIKE 'CNP%'
                            AND ACT.completed_date BETWEEN '{start_date}'
                            AND '{end_date}'
                            AND ACT.name IN (
                                {utils.list_to_string(cnp_act_list)}
                            )
                    )
                    AND (
                        (
                            PER.role LIKE 'CNP%'
                            AND ACT.completed_date BETWEEN '{start_date}'
                            AND '{end_date}'
                            AND ACT.name IN (
                                {utils.list_to_string(cnp_act_list)}
                            )
                        )
                        OR (
                            PER.role LIKE 'GSDT6%'
                            AND ACT.name IN (
                                {utils.list_to_string(gsdt6_act_list)}
                            )
                        )
                    )
                ORDER BY
                    ORD.order_code,
                    act_step_no;
            """

    logger.info("Querying db ...")
    logger.info(report.orion_db.log_full_query(query))
    result = report.orion_db.query_to_list(query)

    logger.info("Creating report ...")
    df = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # Convert columns to date
    for column in const.DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column]).dt.date

    # Write to CSV for Warroom Report
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(df, csv_main_file_path)

    # Add CSV to zip file
    zip_file = ("{}_{}.zip").format(filename, utils.get_current_datetime())
    zip_file_path = os.path.join(report.reports_folder_path, zip_file)

    # Attach files to email
    if report.debug_config.getboolean('compress_files'):
        report.add_to_zip_file(csv_main_file_path, zip_file_path)
        report.attach_file_to_email(zip_file_path)
    else:
        report.attach_file_to_email(csv_main_file_path)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.send_email()
