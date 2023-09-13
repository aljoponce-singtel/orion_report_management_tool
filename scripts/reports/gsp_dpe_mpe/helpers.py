# Import built-in packages
import os
import logging

# Import third-party packages
import pandas as pd

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_main_report():

    report = OrionReport(config_file)
    report.set_report_name('DPE MPE Report')
    report.set_filename('dpe_mpe_report')
    report.set_prev_month_first_last_day_date()
    generate_report(report)


def generate_billing_report():

    report = OrionReport(config_file)
    report.set_report_name('DPE MPE (Billing) Report')
    report.set_filename('dpe_mpe_billing_report')
    report.set_gsp_billing_month_start_end_date()
    generate_report(report)


def generate_report(report: OrionReport):

    df_raw = get_raw_records(report)
    df_dpe = get_dpe_records(df_raw)
    df_mpe = get_mpe_records(df_raw)
    df_mse = get_mse_records(df_raw)

    # Create Excel writer object
    excel_file = ("{}_{}.xlsx").format(
        report.filename, utils.get_current_datetime())
    excel_file_path = os.path.join(report.reports_folder_path, excel_file)
    excel_writer = pd.ExcelWriter(excel_file_path, engine='xlsxwriter')

    # Write dataframes to separate sheets
    # Option to generate raw file
    if report.debug_config.getboolean('include_raw_report'):
        logger.info("Including raw records in the report ...")
        df_raw.to_excel(excel_writer, sheet_name='RAW', index=False)

    df_dpe.to_excel(excel_writer, sheet_name='DPE', index=False)
    df_mpe.to_excel(excel_writer, sheet_name='MPE', index=False)
    df_mse.to_excel(excel_writer, sheet_name='MSE', index=False)

    # Close the Excel file
    excel_writer.close()

    # Add Excel file to zip file
    zip_file = ("{}_{}.zip").format(
        report.filename, utils.get_current_datetime())
    zip_file_path = os.path.join(report.reports_folder_path, zip_file)

    # Attach files to email
    if report.debug_config.getboolean('compress_files'):
        report.add_to_zip_file(excel_file_path, zip_file_path)
        report.attach_file_to_email(zip_file_path)
    else:
        report.attach_file_to_email(excel_file_path)

    # Send Email
    report.send_email()


def get_dpe_records(df: pd.DataFrame):

    logger.info("Getting DPE records ...")
    # Define the conditions
    condition1 = (df['Group ID'] == 'SDE_TP') & (
        df['Activity name'] == 'Updating of TP Info')
    condition2 = (df['Group ID'].isin(['OLLC_AUS', 'OLLC_CHN', 'OLLC_HKG', 'OLLC_IND', 'OLLC_INS', 'OLLC_JPN', 'OLLC_KR', 'OLLC_MLA', 'OLLC_PHL',
                                       'OLLC_ROW', 'OLLC_SNG', 'OLLC_THA', 'OLLC_TWN', 'OLLC_UK', 'OLLC_USA', 'OLLC_VTM'])) & (df['Activity name'] == 'OLLC Order Ack')
    # Apply the conditions to filter the dataframe
    df_dpe = df[condition1 | condition2]
    df_dpe = df_dpe[const.DPE_COLUMNS]

    return df_dpe


def get_mpe_records(df: pd.DataFrame):

    logger.info("Getting MPE records ...")
    # Define the conditions
    condition1 = df['Group ID'] == 'IMPACT'
    # Apply the conditions to filter the dataframe
    df_mpe = df[condition1]
    df_mpe = df_mpe[const.MPE_COLUMNS]

    return df_mpe


def get_mse_records(df: pd.DataFrame):

    logger.info("Getting MSE records ...")
    # Define the conditions
    condition1 = df['Group ID'].isin(['CPE', 'CPE_CSE', 'EWO', 'EWO_CSE', 'IMPACT_CSE', 'LAN_CPE', 'LAN_CPE_TR', 'RLAN',
                                      'RLAN_CSE', 'SDLAN', 'SDWAN', 'TOPS', 'TOPS_CSE', 'TSGRS', 'TSGRS_CSE', 'WIFI', 'WIFI_CSE', 'WLAN', 'WLAN_CSE'])
    # Apply the conditions to filter the dataframe
    df_dpe = df[condition1]
    df_dpe = df_dpe[const.MSE_COLUMNS]
    return df_dpe


def get_raw_records(report: OrionReport):

    query = f"""
                SELECT
                    DISTINCT ORD.order_code AS 'Workorder',
                    ORD.service_number AS 'Service No',
                    CUS.name AS 'Customer Name',
                    ACT.name AS 'Activity name',
                    PER.role AS 'Group ID',
                    ORD.current_crd AS 'CRD',
                    ORD.order_type AS 'Order type',
                    PAR.STPoNo AS 'PO No',
                    'TBD' AS 'PO/SO No',
                    'TBD' AS 'GRS No',
                    NPP.level AS 'NPP Level',
                    PRD.network_product_code AS 'NPC',
                    ORD.taken_date AS 'Order creation date',
                    ACT.status AS 'Act Status',
                    ACT.completed_date AS 'Comm date',
                    PAR.OriginCtry AS 'Originating Country',
                    PAR.OriginCarr AS 'Originating Carrier',
                    PAR.MainSvcType AS 'Main Svc Type',
                    PAR.MainSvcNo AS 'Main Svc No',
                    PAR.LLC_Partner_Ref AS 'LLC Partner reference',
                    PAR.CrossConnReq AS 'Cross Connect Reference',
                    'TBD' AS 'Purchasing Group',
                    'TBD' AS 'Product Type',
                    PAR.IMPGcode AS 'IMPG Code',
                    PER.email AS 'Group Owner',
                    ACT.performer_id AS 'Performer ID'
                FROM
                    RestInterface_order ORD
                    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
                    JOIN RestInterface_person PER ON ACT.person_id = PER.id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN (
                        SELECT
                            NPP_INNER.id,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'STPoNo' THEN PAR_INNER.parameter_value
                                END
                            ) STPoNo,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'OriginCtry' THEN PAR_INNER.parameter_value
                                END
                            ) OriginCtry,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'OriginCarr' THEN PAR_INNER.parameter_value
                                END
                            ) OriginCarr,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'MainSvcType' THEN PAR_INNER.parameter_value
                                END
                            ) MainSvcType,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'MainSvcNo' THEN PAR_INNER.parameter_value
                                END
                            ) MainSvcNo,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'LLC_Partner_Ref' THEN PAR_INNER.parameter_value
                                END
                            ) LLC_Partner_Ref,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'CrossConnReq' THEN PAR_INNER.parameter_value
                                END
                            ) CrossConnReq,
                            MAX(
                                CASE
                                    WHEN PAR_INNER.parameter_name = 'IMPGcode' THEN PAR_INNER.parameter_value
                                END
                            ) IMPGcode
                        FROM
                            RestInterface_npp NPP_INNER
                            JOIN RestInterface_parameter PAR_INNER ON PAR_INNER.npp_id = NPP_INNER.id
                        WHERE
                            NPP_INNER.status != 'Cancel'
                        GROUP BY
                            NPP_INNER.id
                    ) PAR ON PAR.id = NPP.id
                WHERE
                    (
                        (
                            PER.role = 'SDE_TP'
                            AND ACT.name = 'Updating of TP Info'
                        )
                        OR (
                            PER.role IN (
                                'OLLC_AUS',
                                'OLLC_CHN',
                                'OLLC_HKG',
                                'OLLC_IND',
                                'OLLC_INS',
                                'OLLC_JPN',
                                'OLLC_KR',
                                'OLLC_MLA',
                                'OLLC_PHL',
                                'OLLC_ROW',
                                'OLLC_SNG',
                                'OLLC_THA',
                                'OLLC_TWN',
                                'OLLC_UK',
                                'OLLC_USA',
                                'OLLC_VTM'
                            )
                            AND ACT.name = 'OLLC Order Ack'
                        )
                        OR PER.role = 'IMPACT'
                        OR PER.role IN (
                            'CPE',
                            'CPE_CSE',
                            'EWO',
                            'EWO_CSE',
                            'IMPACT_CSE',
                            'LAN_CPE',
                            'LAN_CPE_TR',
                            'RLAN',
                            'RLAN_CSE',
                            'SDLAN',
                            'SDWAN',
                            'TOPS',
                            'TOPS_CSE',
                            'TSGRS',
                            'TSGRS_CSE',
                            'WIFI',
                            'WIFI_CSE',
                            'WLAN',
                            'WLAN_CSE'
                        )
                    )
                    AND NPP.status != 'Cancel'
                    AND ACT.completed_date BETWEEN '{report.start_date}'
                    AND '{report.end_date}'
                ORDER BY
                    ORD.order_code,
                    ACT.name;
            """

    result = report.orion_db.query_to_list(
        query, query_description='raw records')

    df = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # Convert columns to date
    for column in const.DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column]).dt.date

    return df
