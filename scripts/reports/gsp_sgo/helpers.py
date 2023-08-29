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
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_sgo_report():

    report = OrionReport(config_file)

    report.subject = 'SGO Report'
    report.filename = 'sgo_report'

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        report.start_date = report.debug_config['report_start_date']
        report.end_date = report.debug_config['report_end_date']

    else:
        # 1st of the month
        report.start_date, report.end_date = utils.get_prev_month_first_last_day_date(
            datetime.now().date())

    logger.info("Generating SGO report ...")
    generate_report(report)


def generate_sgo_billing_report():

    report = OrionReport(config_file)

    report.subject = 'SGO (Billing) Report'
    report.filename = 'sgo_billing_report'

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        report.start_date = report.debug_config['report_start_date']
        report.end_date = report.debug_config['report_end_date']

    else:
        # 26th of the month
        report.start_date, report.end_date = utils.get_gsp_billing_month_start_end_date(
            datetime.now().date())

    logger.info("Generating SGO (billing) report ...")
    generate_report(report)


def generate_report(report: OrionReport):

    logger.info("report start date: " + str(report.start_date))
    logger.info("report end date: " + str(report.end_date))

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
                            ) LLC_Partner_Ref
                        FROM
                            RestInterface_npp NPP_INNER
                            JOIN RestInterface_parameter PAR_INNER ON PAR_INNER.npp_id = NPP_INNER.id
                        WHERE
                            NPP_INNER.status != 'Cancel'
                        GROUP BY
                            NPP_INNER.id
                    ) PAR ON PAR.id = NPP.id
                WHERE
                    ACT.name IN (
                        'Cease Resale SGO',
                        'Cease Resale SGO CHN',
                        'Cease Resale SGO HK',
                        'Cease Resale SGO India',
                        'Cease Resale SGO JP',
                        'Cease Resale SGO KR',
                        'Cease Resale SGO TW',
                        'Cease Resale SGO UK',
                        'Cease Resale SGO USA',
                        'Change Resale SGO',
                        'Change Resale SGO CHN',
                        'Change Resale SGO HK',
                        'Change Resale SGO India',
                        'Change Resale SGO JP',
                        'Change Resale SGO KR',
                        'Change Resale SGO TW',
                        'Change Resale SGO UK',
                        'Change Resale SGO USA',
                        'Partner Coordination',
                        'LLC Accepted by Singtel'
                    )
                    AND PER.role IN (
                        'GIP_HK',
                        'GIP_IND',
                        'GIP_INS',
                        'GIP_MLA',
                        'GIP_PHL',
                        'GIP_THA',
                        'GIP_UK',
                        'GIP_USA',
                        'GIP_VTM',
                        'RESALE_IND',
                        'RESALE_INS',
                        'RESALE_MLA',
                        'RESALE_PHL',
                        'RESALE_THA',
                        'RESALE_USA',
                        'RESALE_VTM',
                        'Resale_HK',
                        'Resale_UK',
                        'SDWAN_INS',
                        'SDWAN_MLA',
                        'SDWAN_PHL',
                        'SDWAN_THA',
                        'SDWAN_TW',
                        'SDWAN_VTM',
                        'GIP_CHN',
                        'Resale_CHN',
                        'GIP_TWN',
                        'Resale_TW',
                        'GIP_JP',
                        'Resale_JP',
                        'GIP_KR',
                        'Resale_KR',
                        'GIP_BGD',
                        'RESALE_BGD'
                    )
                    AND NPP.status != 'Cancel'
                    AND ACT.completed_date BETWEEN '{report.start_date}'
                    AND '{report.end_date}'
                ORDER BY
                    ORD.order_code,
                    ACT.name;
            """

    result = report.orion_db.query_to_list(query)

    logger.info("Creating SGO report ...")
    df = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # /* START */
    # There is no value in 'PO No' for 'NPP Level' == 'Mainline'.
    # It is only available when 'NPP Level' == 'VAS'
    # Since 'PO No' is required in the report, the value from VAS will be copied to the Mainline record,
    # even if it doesn't belong to this record.

    # Find the rows where NPPLevel is "VAS"
    df_vas = df[df['NPP Level'] == 'VAS']

    # Iterate over the rows
    for index, row in df_vas.iterrows():
        # Get the WorkorderNo and PoNo values
        workorder = row['Workorder']
        po_number = row['PO No']

        # Update the corresponding row where NPP Level is "Mainline" and Workorder is the same
        df.loc[(df['NPP Level'] == 'Mainline') & (
            df['Workorder'] == workorder), 'PO No'] = po_number
    # /* END */

    # Get the unique orders with VAS product
    vas_orders = df_vas['Workorder'].unique().tolist()
    # Get the unique orders with Mainline product
    mainline_orders = df[df['NPP Level']
                         == 'Mainline']['Workorder'].unique().tolist()
    # Compare vas_orders and mainline_orders to get the orders without a Mainline product
    orders_wo_mainline = [
        element for element in vas_orders if element not in mainline_orders]

    # /* START */
    # Remove the row where 'NPP Level' is 'VAS' but only for workorders than have a Mainline product
    # Check for VAS records
    condition1 = df['NPP Level'] != 'VAS'
    # Check if the orders in df do not have a Mainline product
    condition2 = df['Workorder'].isin(orders_wo_mainline)
    # Use 'loc' to filter and extract records that satisfy both conditions
    df = df.loc[condition1 | condition2]
    # Remove the 'NPP Level' column
    df = df.drop('NPP Level', axis=1)
    # /* END */

    # Convert columns to date
    for column in const.DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column]).dt.date

    # Write to CSV
    csv_file = report.create_csv_from_df(df)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file)
    # Send Email
    report.set_email_subject(report.add_timestamp(report.subject))
    report.attach_file_to_email(zip_file)
    report.send_email()
