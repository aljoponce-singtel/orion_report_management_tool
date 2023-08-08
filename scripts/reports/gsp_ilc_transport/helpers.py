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


def generate_ilc_transport_report():

    report = OrionReport(configFile)

    email_subject = 'ILC Transport Report'
    filename = 'ilc_transport_report'
    start_date = None
    end_date = None

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

    else:
        # 1st of the month
        start_date = utils.get_first_day_from_prev_month(
            datetime.now().date())
        end_date = utils.get_last_day_from_prev_month(datetime.now().date())

    logger.info("Generating ILC Transport report ...")
    generate_report(report, email_subject, filename, start_date, end_date)


def generate_ilc_transport_billing_report():

    report = OrionReport(configFile)

    email_subject = 'ILC Transport (Billing) Report'
    filename = 'ilc_transport_billing_report'
    start_date = None
    end_date = None

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

    else:
        # 26th of the month
        start_date = utils.get_start_date_from_prev_month(
            datetime.now().date())
        end_date = utils.get_end_date_from_prev_month(
            datetime.now().date())

    logger.info("Generating ILC Transport (billing) report ...")
    generate_report(report, email_subject, filename, start_date, end_date)


def generate_report(report, email_subject, filename, start_date, end_date):

    logger.info("report start date: " + str(start_date))
    logger.info("report end date: " + str(end_date))

    query = ("""
                SELECT
                    DISTINCT (
                        CASE
                            WHEN ORD.service_number REGEXP '.*LLC$' THEN 'OLLC'
                            ELSE 'ILC'
                        END
                    ) AS 'Service',
                    ORD.order_code AS 'OrderCode',
                    CUS.name AS 'CustomerName',
                    ORD.current_crd AS 'CRD',
                    ORD.service_number AS 'ServiceNumber',
                    ORD.order_status AS 'OrderStatus',
                    ORD.order_type AS 'OrderType',
                    PRD.network_product_code AS 'ProductCode',
                    PER_PRECONFIG.role AS 'PreConfig_GroupID',
                    (
                        CASE
                            WHEN PER_PRECONFIG.role REGEXP '^ODC_.*' THEN 'ODC'
                            WHEN PER_PRECONFIG.role REGEXP '^RDC_.*' THEN 'RDC'
                            WHEN PER_PRECONFIG.role IS NULL THEN NULL
                            ELSE 'SGP'
                        END
                    ) AS 'PreConfig_Team',
                    ACT_PRECONFIG.name AS 'PreConfig_ActName',
                    ACT_PRECONFIG.status AS 'PreConfig_ActStatus',
                    ACT_PRECONFIG.due_date AS 'PreConfig_ActDueDate',
                    ACT_PRECONFIG.completed_date AS 'PreConfig_COM_Date',
                    PER_COORDINATION.role AS 'Coordination_Group_ID',
                    (
                        CASE
                            WHEN PER_COORDINATION.role REGEXP '^ODC_.*' THEN 'ODC'
                            WHEN PER_COORDINATION.role REGEXP '^RDC_.*' THEN 'RDC'
                            WHEN PER_COORDINATION.role IS NULL THEN NULL
                            ELSE 'SGP'
                        END
                    ) AS 'Coordination_Team',
                    ACT_COORDINATION.name AS 'Coordination_ActName',
                    ACT_COORDINATION.status AS 'Coordination_ActStatus',
                    ACT_COORDINATION.due_date AS 'Coordination_ActDueDate',
                    ACT_COORDINATION.completed_date AS 'Coordination_COM_Date'
                FROM
                    (
                        SELECT
                            ORD.id AS order_id
                        FROM
                            RestInterface_order ORD
                            JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                            JOIN RestInterface_person PER ON PER.id = ACT.person_id
                        WHERE
                            PER.role IN (
                                'RDC_ASSG',
                                'RDC_ASSG1',
                                'RDC_ASSG2',
                                'RDC_ASSG3',
                                'RDC_ASSG4',
                                'RDC_GSP',
                                'RDC_GSP1',
                                'RDC_GSP2',
                                'RDC_GSP3',
                                'RDC_GSP4',
                                'RDC_ILC',
                                'RDC_ILC1',
                                'RDC_ILC2',
                                'RDC_ILC3',
                                'RDC_ILC4'
                            )
                            AND ACT.name IN (
                                'GSDT Co-ordination Work',
                                'GSDT Co-ordination OS LLC'
                            )
                            AND ACT.completed_date BETWEEN '{}'
                            AND '{}'
                    ) ORD_COM
                    JOIN RestInterface_order ORD ON ORD.id = ORD_COM.order_id
                    JOIN (
                        SELECT
                            ACT.order_id,
                            MAX(
                                CASE
                                    WHEN ACT.name NOT IN (
                                        'GSDT Co-ordination Work',
                                        'GSDT Co-ordination OS LLC'
                                    ) THEN ACT.id
                                END
                            ) preconfig_id,
                            MAX(
                                CASE
                                    WHEN ACT.name IN (
                                        'GSDT Co-ordination Work',
                                        'GSDT Co-ordination OS LLC'
                                    ) THEN ACT.id
                                END
                            ) coordination_id
                        FROM
                            RestInterface_activity ACT
                            JOIN RestInterface_person PER ON PER.id = ACT.person_id
                        WHERE
                            PER.role IN (
                                'RDC_ASSG',
                                'RDC_ASSG1',
                                'RDC_ASSG2',
                                'RDC_ASSG3',
                                'RDC_ASSG4',
                                'RDC_GSP',
                                'RDC_GSP1',
                                'RDC_GSP2',
                                'RDC_GSP3',
                                'RDC-GSP4',
                                'RDC_ILC',
                                'RDC_ILC1',
                                'RDC_ILC2',
                                'RDC_ILC3',
                                'RDC_ILC4'
                            )
                        GROUP BY
                            order_id
                    ) ACT_MAX ON ACT_MAX.order_id = ORD.id
                    LEFT JOIN RestInterface_activity ACT_PRECONFIG ON ACT_PRECONFIG.id = ACT_MAX.preconfig_id
                    LEFT JOIN RestInterface_activity ACT_COORDINATION ON ACT_COORDINATION.id = ACT_MAX.coordination_id
                    LEFT JOIN RestInterface_person PER_PRECONFIG ON PER_PRECONFIG.id = ACT_PRECONFIG.person_id
                    LEFT JOIN RestInterface_person PER_COORDINATION ON PER_COORDINATION.id = ACT_COORDINATION.person_id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = 'Mainline'
                    AND NPP.status != 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                WHERE
                    PRD.network_product_code IN (
                        'BIC0003',
                        'CIC0001',
                        'CIC0006',
                        'CIC0010',
                        'ILC0008'
                    )
                ORDER BY
                    ORD.order_code;
            """).format(start_date, end_date)

    logger.info("Querying db ...")
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
    report.add_to_zip_file(csv_main_file_path, zip_file_path)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.attach_file_to_email(zip_file_path)
    report.send_email()
