# Import built-in packages
import os
from datetime import datetime
import logging

# Import third-party packages
import numpy as np
import pandas as pd
from sqlalchemy import select, case, and_, or_, null, func

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_warroom_report():

    report = OrionReport(configFile)

    email_subject = 'GSP (NEW) War Room Report'
    filename = 'gsp_warroom_report'
    report_date = datetime.now().date()

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')
        report_date = report.debug_config['report_date']

    else:
        report_date = datetime.now().date()

    logger.info("report date: " + str(report_date))

    logger.info("Generating report ...")

    query = ("""
                SELECT
                    DISTINCT ORD.order_code,
                    ORD.service_number,
                    CUS.name AS customer,
                    ORD.order_type,
                    ORD.order_status,
                    ORD.taken_date,
                    ORD.current_crd,
                    ORD.initial_crd,
                    SINOTE.note_code,
                    SINOTE.date_created AS crd_amendment_date,
                    SINOTE.details AS crd_amendment_details,
                    REGEXP_SUBSTR(SINOTE.details, '(?<=Old CRD:)(.*)(?= New CRD:)') AS old_crd,
                    REGEXP_SUBSTR(
                        SINOTE.details,
                        '(?<=New CRD:)(.*)(?= Category Code:)'
                    ) AS new_crd,
                    NOTEDLY.reason AS crd_amendment_reason,
                    NOTEDLY.reason_gsp AS crd_amendment_reason_gsp,
                    ORD.assignee,
                    PRJ.project_code,
                    CKT.circuit_code,
                    PRD.network_product_code AS product_code,
                    PRD.network_product_desc AS product_description,
                    ORD.business_sector,
                    SITE.site_code AS exchange_code_a,
                    SITE.site_code_second AS exchange_code_b,
                    BRN.brn,
                    ORD.sde_received_date,
                    ORD.arbor_disp AS arbor_service,
                    ORD.service_type,
                    ORD.order_priority,
                    PAR.parameter_value AS ed_pd_diversity,
                    GSP.department,
                    GSP.group_id,
                    CAST(ACT.activity_code AS SIGNED INTEGER) AS step_no,
                    ACT.name AS activity_name,
                    ACT.due_date,
                    ACT.status,
                    ACT.ready_date,
                    ACT.completed_date,
                    RMK.created_at AS act_dly_reason_date,
                    ACTDLY.reason AS act_delay_reason
                FROM
                    RestInterface_order ORD
                    JOIN (
                        SELECT
                            DISTINCT ORD2.id
                        FROM
                            RestInterface_order ORD2
                            JOIN RestInterface_activity ACT2 ON ACT2.order_id = ORD2.id
                            JOIN RestInterface_person PER2 ON PER2.id = ACT2.person_id
                            JOIN GSP_Q_ownership GSP2 ON GSP2.group_id = PER2.role
                        WHERE
                            GSP2.department LIKE "GD_%"
                            AND ORD2.order_status IN (
                                'Submitted',
                                'PONR',
                                'Pending Cancellation',
                                'Completed'
                            )
                            AND ORD2.current_crd <= DATE_ADD(NOW(), INTERVAL 3 MONTH)
                    ) ORDGD ON ORDGD.id = ORD.id
                    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                    JOIN RestInterface_person PER ON PER.id = ACT.person_id
                    JOIN GSP_Q_ownership GSP ON GSP.group_id = PER.role
                    LEFT JOIN auto_escalation_remarks RMK ON RMK.activity_id = ACT.id
                    LEFT JOIN auto_escalation_queueownerdelayreasons ACTDLY ON RMK.delay_reason_id = ACTDLY.id
                    LEFT JOIN RestInterface_ordersinote SINOTE ON SINOTE.order_id = ORD.id
                    AND SINOTE.categoty = 'CRD'
                    AND SINOTE.sub_categoty = 'CRD Change History'
                    AND SINOTE.reason_code IS NOT NULL
                    LEFT JOIN RestInterface_delayreason NOTEDLY ON NOTEDLY.code = SINOTE.reason_code
                    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
                    LEFT JOIN RestInterface_circuit CKT ON ORD.circuit_id = CKT.id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
                    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = 'Mainline'
                    AND NPP.status != 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
                    AND PAR.parameter_name = 'Type'
                    AND PAR.parameter_value IN ('1', '2', '010', '020')
                WHERE
                    ACT.tag_name = 'Pegasus';
            """).format(report_date)

    result = report.orion_db.query_to_list(query)
    df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # set columns to datetime type
    df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
        pd.to_datetime)

    # convert datetime to date (remove time)
    df_raw['act_dly_reason_date'] = pd.to_datetime(
        df_raw['act_dly_reason_date']).dt.date

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

    # (left) Join the df_main and df_crd_amendment dataframe through their order_code
    df_merged = pd.merge(df_main, df_crd_amendment.drop(columns=['note_code']),
                         how='left', on=['order_code'])

    # Write to CSV for Warroom Report
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(df_merged, csv_main_file_path)

    # Add CSV to zip file
    zip_file = ("{}_{}.zip").format(filename, utils.get_current_datetime())
    zip_file_path = os.path.join(report.reports_folder_path, zip_file)
    report.add_to_zip_file(csv_main_file_path, zip_file_path)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.attach_file_to_email(zip_file_path)
    report.send_email()

    return


def generate_warroom_nongov_report():

    return
