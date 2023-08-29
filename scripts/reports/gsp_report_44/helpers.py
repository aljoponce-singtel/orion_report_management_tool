# Import built-in packages
import os
from datetime import datetime
import logging

# Import third-party packages
import numpy as np
import pandas as pd

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(config_file)

    report.subject = 'GSP Report 44'
    report.filename = 'gsp_report_44'

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        report.start_date = report.debug_config['report_start_date']
        report.end_date = report.debug_config['report_end_date']

    else:
        report.start_date, report.end_date = utils.get_prev_month_first_last_day_date(
            datetime.now().date())

    logger.info("report start date: " + str(report.start_date))
    logger.info("report end date: " + str(report.end_date))

    logger.info("Generating report ...")

    query = f"""
                SELECT
                    DISTINCT 1 AS qty,
                    ORD.order_taken_by AS user_id,
                    "TBD" AS group_code,
                    ORD.arbor_disp AS arbor_service_type,
                    BRN.brn AS cust_id,
                    ORD.service_number AS service_number,
                    ORD.service_action_type AS service_action_type,
                    ORD.order_code,
                    ORD.order_type,
                    ORD.order_status,
                    ORD.current_crd AS cus_req_dt,
                    ORD.taken_date AS ord_created_dt,
                    (
                        CASE
                            WHEN ORD.taken_date >= ORD.current_crd THEN "Y"
                            ELSE "N"
                        END
                    ) AS backdated_order,
                    PRJ.project_code AS project_code,
                    "TBD" AS application_mode,
                    (
                        CASE
                            WHEN CON.contact_type = "AM" THEN CONCAT(CON.family_name, ' ', CON.given_name)
                            ELSE ''
                        END
                    ) AS am,
                    (
                        CASE
                            WHEN CON.contact_type = "SDE" THEN CONCAT(CON.family_name, ' ', CON.given_name)
                            ELSE ''
                        END
                    ) AS sde,
                    ORD.applied_date AS applied_date,
                    ORD.received_date AS received_date,
                    ORD.sde_received_date AS sde_received_date,
                    ORD.operation_received_date AS operation_received_date,
                    ORD.taken_date AS submitted_date,
                    "POST-PROCESS" AS tat_for_sales,
                    "POST-PROCESS" AS tat_for_am_to_ord_raised,
                    "POST-PROCESS" AS sde_to_pd_tat,
                    "POST-PROCESS" AS tat_mcc_raising_ord,
                    "POST-PROCESS" AS tat_sde_processing_time,
                    ORD.order_priority AS priority,
                    "TBD" AS letter_generated,
                    ORD.business_sector AS cus_facing_unit,
                    "TBD" AS responsibility_code,
                    CUS.name AS customer_name,
                    ORD.ord_action_type AS order_action_type,
                    "TBD" AS sub_rc_description,
                    (
                        CASE
                            WHEN ORD.is_bulk_order = 1 THEN "YES"
                            ELSE "NO"
                        END
                    ) AS bulk_order,
                    ORD.am_id AS am_id,
                    "TBD" AS channel_partner,
                    "TBD" AS icon_act_ref,
                    "TBD" AS focalscope_ticket,
                    "TBD" AS impact_mobile_svc_order,
                    "TBD" AS impact_svc_ord_number,
                    "TBD" AS package_description,
                    "POST-PROCESS" AS sde_wo_tat,
                    (
                        CASE
                            WHEN CON.contact_type = "Project Manager" THEN CONCAT(CON.family_name, ' ', CON.given_name)
                            ELSE ''
                        END
                    ) AS project_manager,
                    "TBD" AS fdd,
                    "TBD" AS foc_date,
                    "TBD" AS date_recv_from_parter_foc,
                    "TBD" AS ollc_order_placed,
                    "TBD" AS site_survey_date,
                    "TBD" AS dpe_received_date,
                    "TBD" AS parter_ack_date,
                    ORD.initial_crd AS initial_crd,
                    "POST-PROCESS" AS tat_ord_applied_to_sde_rcvd,
                    "POST-PROCESS" AS tat_sde_rcvd_to_ord_submtd,
                    "TBD" AS new_installation_name,
                    "TBD" AS currency,
                    "TBD" AS estimated_mrc,
                    "TBD" AS estimated_otc,
                    SINOTE.categoty AS notes_category,
                    SINOTE.sub_categoty AS notes_subcategory,
                    "TBD" AS mainline_component_desc,
                    "TBD" AS otc_component_desc
                FROM
                    o2pprod.RestInterface_order ORD
                    LEFT JOIN o2pprod.RestInterface_project PRJ ON PRJ.id = ORD.project_id
                    LEFT JOIN o2pprod.RestInterface_contactdetails CON ON CON.order_id = ORD.id
                    AND CON.contact_type IN ("AM", "SDE", "Project Manager")
                    LEFT JOIN o2pprod.RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN o2pprod.RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                    LEFT JOIN o2pprod.RestInterface_usercustomerbrnmapping USRBRN ON USRBRN.customer_brn_id = ORD.customer_brn_id
                    LEFT JOIN o2pprod.RestInterface_billing BILL ON BILL.order_id = ORD.id
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
                                    categoty = 'eRequest'
                                    OR categoty = 'Non eRequest'
                                GROUP BY
                                    order_id
                            ) SINOTEMAX ON SINOTEMAX.order_id = SINOTEINNER.order_id
                            AND SINOTEMAX.note_code = SINOTEINNER.note_code
                    ) SINOTE ON SINOTE.order_id = ORD.id
                WHERE
                    ORD.taken_date BETWEEN '{report.start_date}' AND '{report.end_date}'
            """

    logger.info("Querying db ...")
    result = report.orion_db.query_to_list(query)
    logger.info("Creating report 44 report ...")
    df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # set columns to datetime type
    df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
        pd.to_datetime)

    # Get the list of holiday dates excluding weekends
    df_holidays = get_holidays(report)

    # calculate the number of days between two dates
    df_raw['tat_for_sales'] = df_raw.apply(
        lambda row: count_weekdays(row['received_date'], row['sde_received_date'], df_holidays), axis=1)
    df_raw['tat_for_am_to_ord_raised'] = df_raw.apply(
        lambda row: count_weekdays(row['applied_date'], row['submitted_date'], df_holidays), axis=1)
    df_raw['sde_to_pd_tat'] = df_raw.apply(
        lambda row: count_weekdays(row['received_date'], row['submitted_date'], df_holidays), axis=1)
    df_raw['tat_mcc_raising_ord'] = df_raw.apply(
        lambda row: count_weekdays(row['operation_received_date'], row['submitted_date'], df_holidays), axis=1)
    df_raw['tat_sde_processing_time'] = df_raw.apply(
        lambda row:
            np.NaN if row['operation_received_date'] is pd.NaT
            else
            (
                0 if row['sde_received_date'] is pd.NaT and row['operation_received_date'] == row['received_date']
                else (
                    count_weekdays(row['received_date'],
                                   row['operation_received_date'], df_holidays) if row['sde_received_date'] is pd.NaT
                    else (
                        0 if row['operation_received_date'] == row['sde_received_date']
                        else count_weekdays(row['sde_received_date'], row['operation_received_date'], df_holidays)
                    )
                )
            ),
            axis=1
    )
    df_raw['sde_wo_tat'] = df_raw.apply(
        lambda row: count_weekdays(row['sde_received_date'], row['submitted_date'], df_holidays), axis=1)
    df_raw['tat_ord_applied_to_sde_rcvd'] = df_raw.apply(
        lambda row:
            count_weekdays(row['applied_date'], row['operation_received_date'], df_holidays) if row['sde_received_date'] is pd.NaT
            else count_weekdays(row['applied_date'], row['sde_received_date'], df_holidays),
            axis=1
    )
    df_raw['tat_sde_rcvd_to_ord_submtd'] = df_raw.apply(
        lambda row:
            count_weekdays(row['operation_received_date'], row['submitted_date'], df_holidays) if row['sde_received_date'] is pd.NaT
            else count_weekdays(row['sde_received_date'], row['submitted_date'], df_holidays),
            axis=1
    )

    # Write to CSV
    csv_file = report.create_csv_from_df(df_raw)
    # Add CSV to zip file
    # zip_file = report.add_to_zip_file(csv_file)

    # Send Email
    report.set_email_subject(report.add_timestamp(report.subject))
    report.attach_file_to_email(csv_file)
    report.send_email()


def get_holidays(report: OrionReport):

    query = ("""
                SELECT
                    Holiday,
                    Date
                FROM
                    o2ptableau.t_GSP_holidays ORD
                ;
            """)

    result = report.tableau_db.query_to_list(query)
    df = pd.DataFrame(data=result, columns=['Holiday', 'Date'])

    # set columns to datetime type
    df[['Date']] = df[['Date']].apply(
        pd.to_datetime)

    # Exclude weekend holidays
    df['Weekday'] = df['Date'].dt.dayofweek
    filtered_dates = df.loc[(df['Weekday'] != 5) & (df['Weekday'] != 6)]

    # logger.info(filtered_dates)

    return filtered_dates


def count_weekdays(start_date: datetime, end_date: datetime, df_holidays: pd.DataFrame):
    num_weekdays = np.nan

    if pd.notnull(start_date) and pd.notnull(end_date):
        num_weekdays = np.busday_count(start_date.date(), end_date.date())
        num_wkday_holidays = sum(start_date <= date
                                 <= end_date for date in df_holidays['Date'])

        num_weekdays = num_weekdays - num_wkday_holidays + 1

    return num_weekdays
