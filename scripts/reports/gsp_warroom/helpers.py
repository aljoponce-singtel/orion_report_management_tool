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
                    ORD.close_date,
                    SINOTE.note_code,
                    SINOTE.date_created AS crd_amendment_date,
                    SINOTE.details AS crd_amendment_details,
                    REGEXP_SUBSTR(SINOTE.details, BINARY '(?<=Old CRD:)(.*)(?= New CRD:[0-9]{{8}})') AS old_crd,
                    REGEXP_SUBSTR(
                        SINOTE.details,
                        BINARY '(?<=New CRD:)(.*)(?= Category Code:)'
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
                    ORD.am_id,
                    ORD.sde_received_date,
                    ORD.arbor_disp AS arbor_service,
                    ORD.service_type,
                    ORD.order_priority,
                    PAR.parameter_name,
                    PAR.parameter_value,
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
                            AND ORD2.current_crd <= DATE_ADD('{}', INTERVAL 3 MONTH)
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

    # Rename the 'parameter_value' column to 'ed_pd_diversity'
    df_main = df_main.rename(columns={'parameter_value': 'ed_pd_diversity'})

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
    report.add_email_receiver_to('teokokwee@singtel.com')
    report.attach_file_to_email(zip_file_path)
    report.send_email()

    return


def generate_warroom_npp_report():

    report = OrionReport(configFile)

    email_subject = 'GSP War Room NPP Report'
    filename = 'gsp_warroom_npp_report'
    start_date = None
    end_date = None

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

    else:
        start_date = utils.get_first_day_from_prev_month(
            datetime.now().date())
        end_date = utils.get_last_day_from_prev_month(datetime.now().date())

    logger.info("report start date: " + str(start_date))
    logger.info("report end date: " + str(end_date))

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
                    ORD.close_date,
                    ORD.assignee,
                    PRJ.project_code,
                    CKT.circuit_code,
                    NPP.level AS npp_level,
                    PRD.network_product_code AS product_code,
                    PRD.network_product_desc AS product_description,
                    PAR.PartnerNm,
                    PAR.OLLCAPartnerContractStartDt,
                    PAR.OLLCBPartnerContractStartDt,
                    PAR.OLLCAPartnerContractTerm,
                    PAR.OLLCATax,
                    PAR.LLC_Partner_Name,
                    PAR.LLC_Partner_Ref,
                    PAR.PartnerCctRef,
                    PAR.STPoNo,
                    PAR.STIntSvcNo,
                    PAR.IMPGcode,
                    PAR.Model,
                    ORD.business_sector,
                    SITE.site_code AS exchange_code_a,
                    SITE.site_code_second AS exchange_code_b,
                    BRN.brn,
                    ORD.am_id,
                    ORD.sde_received_date,
                    ORD.arbor_disp AS arbor_service,
                    ORD.service_type,
                    ORD.order_priority,
                    SINOTE.date_created AS crd_amendment_date,
                    REGEXP_SUBSTR(
                        SINOTE.details,
                        BINARY '(?<=Old CRD:)(.*)(?= New CRD:[0-9]{{8}})'
                    ) AS old_crd,
                    REGEXP_SUBSTR(
                        SINOTE.details,
                        BINARY '(?<=New CRD:)(.*)(?= Category Code:)'
                    ) AS new_crd,
                    NOTEDLY.reason AS crd_amendment_reason,
                    NOTEDLY.reason_gsp AS crd_amendment_reason_gsp,
                    ACT.ollc_order_ack AS 'OLLC Order Ack',
                    ACT.ollc_site_survey AS 'OLLC Site Survey',
                    ACT.foc_date_received AS 'FOC Date Received',
                    ACT.llc_accepted_by_singtel AS 'LLC Accepted by Singtel'
                FROM
                    RestInterface_order ORD
                    LEFT JOIN (
                        SELECT
                            order_id,
                            MAX(
                                CASE
                                    WHEN name = 'OLLC Order Ack' THEN completed_date
                                END
                            ) ollc_order_ack,
                            MAX(
                                CASE
                                    WHEN name = 'OLLC Site Survey' THEN completed_date
                                END
                            ) ollc_site_survey,
                            MAX(
                                CASE
                                    WHEN name = 'FOC Date Received' THEN completed_date
                                END
                            ) foc_date_received,
                            MAX(
                                CASE
                                    WHEN name = 'LLC Accepted by Singtel' THEN completed_date
                                END
                            ) llc_accepted_by_singtel
                        FROM
                            RestInterface_activity
                        GROUP BY
                            order_id
                    ) ACT ON ACT.order_id = ORD.id
                    LEFT JOIN (
                        SELECT
                            SINOTEINNER.*
                        FROM
                            o2pprod.RestInterface_ordersinote SINOTEINNER
                            JOIN (
                                SELECT
                                    order_id,
                                    MAX(note_code) AS note_code
                                FROM
                                    o2pprod.RestInterface_ordersinote
                                WHERE
                                    categoty = 'CRD'
                                    AND sub_categoty = 'CRD Change History'
                                    AND reason_code IS NOT NULL
                                GROUP BY
                                    order_id
                            ) SINOTEMAX ON SINOTEMAX.order_id = SINOTEINNER.order_id
                            AND SINOTEMAX.note_code = SINOTEINNER.note_code
                    ) SINOTE ON SINOTE.order_id = ORD.id
                    LEFT JOIN RestInterface_delayreason NOTEDLY ON NOTEDLY.code = SINOTE.reason_code
                    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
                    LEFT JOIN RestInterface_circuit CKT ON ORD.circuit_id = CKT.id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
                    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN (
                        SELECT
                            npp_id,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'IMPGcode' THEN parameter_value
                                END
                            ) IMPGcode,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'LLC_Partner_Name' THEN parameter_value
                                END
                            ) LLC_Partner_Name,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'LLC_Partner_Ref' THEN parameter_value
                                END
                            ) LLC_Partner_Ref,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'Model' THEN parameter_value
                                END
                            ) Model,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'OLLCAPartnerContractStartDt' THEN parameter_value
                                END
                            ) OLLCAPartnerContractStartDt,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'OLLCAPartnerContractTerm' THEN parameter_value
                                END
                            ) OLLCAPartnerContractTerm,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'OLLCATax' THEN parameter_value
                                END
                            ) OLLCATax,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'OLLCBPartnerContractStartDt' THEN parameter_value
                                END
                            ) OLLCBPartnerContractStartDt,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'PartnerCctRef' THEN parameter_value
                                END
                            ) PartnerCctRef,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'PartnerNm' THEN parameter_value
                                END
                            ) PartnerNm,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'STIntSvcNo' THEN parameter_value
                                END
                            ) STIntSvcNo,
                            MAX(
                                CASE
                                    WHEN parameter_name = 'STPoNo' THEN parameter_value
                                END
                            ) STPoNo
                        FROM
                            RestInterface_parameter
                        GROUP BY
                            npp_id
                    ) PAR ON PAR.npp_id = NPP.id
                WHERE
                    ORD.order_status IN ('Submitted', 'Closed')
                    AND ORD.current_crd BETWEEN '{}' AND '{}';
            """).format(start_date, end_date)

    result = report.orion_db.query_to_list(query)
    logger.info("Processing report ...")
    df_raw = pd.DataFrame(data=result, columns=const.MAIN_NPP_COLUMNS)

    # Convert columns to date
    for column in const.DATE_NPP_COLUMNS:
        df_raw[column] = pd.to_datetime(df_raw[column]).dt.date

    # Sort records in ascending order by order_code, parameter_name and step_no
    df_raw = df_raw.sort_values(
        by=['order_code', 'npp_level'], ascending=[True, True])

    # Write to CSV for Warroom Report
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(df_raw, csv_main_file_path)

    # Add CSV to zip file
    # zip_file = ("{}_{}.zip").format(filename, utils.get_current_datetime())
    # zip_file_path = os.path.join(report.reports_folder_path, zip_file)
    # report.add_to_zip_file(csv_main_file_path, zip_file_path)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.attach_file_to_email(csv_main_file_path)
    report.send_email()

    return
