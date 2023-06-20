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


def generate_report_44():

    report = OrionReport(configFile)

    email_subject = 'GSP Report 44'
    filename = 'gsp_report_44'
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
                SELECT    DISTINCT
                  "TBD"                   AS qty
                , "TBD"                   AS user_id
                , "TBD"                   AS group_code
                , ORD.arbor_disp          AS arbor_service_type
                , "TBD"                   AS cust_id
                , ORD.service_number      AS service_number
                , ORD.service_action_type AS service_action_type
                , ORD.order_code
                , ORD.order_type
                , ORD.order_status
                , ORD.current_crd AS cus_req_dt
                , ORD.taken_date  AS ord_created_dt
                ,
                    (
                        CASE
                            WHEN ORD.taken_date >= ORD.current_crd
                                THEN "Y"
                                ELSE "N"
                        END
                    )
                                    AS backdated_order
                , PRJ.project_code AS project_code
                , "TBD"            AS application_mode
                ,
                    (
                        CASE
                            WHEN CON.contact_type = "AM"
                                THEN CONCAT(CON.family_name, ' ', CON.given_name)
                                ELSE ''
                        END
                    )
                    AS am
                ,
                    (
                        CASE
                            WHEN CON.contact_type = "SDE"
                                THEN CONCAT(CON.family_name, ' ', CON.given_name)
                                ELSE ''
                        END
                    )
                                                AS sde
                , ORD.applied_date            AS applied_date
                , ORD.received_date           AS received_date
                , ORD.sde_received_date       AS sde_received_date
                , ORD.operation_received_date AS operation_received_date
                , ORD.submitted_date          AS submitted_date
                , "TBD"                       AS tat_for_sales
                , "TBD"                       AS tat_for_am_to_ord_raised
                , "TBD"                       AS sde_to_pd_tat
                , "TBD"                       AS tat_mcc_raising_ord
                , "TBD"                       AS tat_sde_processing_time
                , ORD.order_priority          AS priority
                , "TBD"                       AS letter_generated
                , ORD.business_sector         AS cus_facing_unit
                , "TBD"                       AS responsibility_code
                , CUS.name                    AS customer_name
                , ORD.ord_action_type         AS order_action_type
                , "TBD"                       AS sub_rc_description
                ,
                    (
                        CASE
                            WHEN ORD.is_bulk_order = 1
                                THEN "YES"
                                ELSE "NO"
                        END
                    )
                                            AS bulk_order
                , "TBD"                    AS am_name
                , ORD.am_id                AS am_id
                , ORD.am_email             AS am_email
                , "TBD"                    AS icon_act_ref
                , "TBD"                    AS focalscope_ticket
                , "TBD"                    AS impact_mobile_svc_order
                , "TBD"                    AS impact_svc_ord_number
                , "TBD"                    AS package_description
                , "TBD"                    AS sde_wo_tat
                ,
                    (
                        CASE
                            WHEN CON.contact_type = "Project Manager"
                                THEN CONCAT(CON.family_name, ' ', CON.given_name)
                                ELSE ''
                        END
                    )
                                        AS project_manager
                , "TBD"               AS fdd
                , "TBD"               AS foc_date
                , "TBD"               AS date_recv_from_parter_foc
                , "TBD"               AS ollc_order_placed
                , "TBD"               AS site_survey_date
                , "TBD"               AS dpe_received_date
                , "TBD"               AS parter_ack_date
                , ORD.initial_crd     AS initial_crd
                , "TBD"               AS tat_ord_applied_to_sde_rcvd
                , "TBD"               AS tat_sde_rcvd_to_ord_submtd
                , "TBD"               AS new_installation_name
                , "TBD"               AS currency
                , "TBD"               AS estimated_mrc
                , "TBD"               AS estimated_otc
                , SINOTE.categoty     AS notes_category
                , SINOTE.sub_categoty AS notes_subcategory
                , "TBD"               AS mainline_component_desc
                , "TBD"               AS otc_component_desc
                FROM
                    o2pprod.RestInterface_order ORD
                    LEFT JOIN
                        o2pprod.RestInterface_project PRJ
                        ON
                            PRJ.id = ORD.project_id
                    LEFT JOIN
                        o2pprod.RestInterface_contactdetails CON
                        ON
                            CON.order_id = ORD.id
                            AND CON.contact_type IN ("AM", "SDE", "Project Manager")
                    LEFT JOIN
                        o2pprod.RestInterface_customer CUS
                        ON
                            CUS.id = ORD.customer_id
                    LEFT JOIN
                        o2pprod.RestInterface_customerbrnmapping BRN
                        ON
                            BRN.id = ORD.customer_brn_id
                    LEFT JOIN
                        o2pprod.RestInterface_usercustomerbrnmapping USRBRN
                        ON
                            USRBRN.customer_brn_id = ORD.customer_brn_id
                    LEFT JOIN
                        o2pprod.RestInterface_billing BILL
                        ON
                            BILL.order_id = ORD.id
                    LEFT JOIN
                        (
                            SELECT
                                SINOTEINNER.*
                            FROM
                                o2pprod.RestInterface_ordersinote SINOTEINNER
                                JOIN
                                    (
                                        SELECT
                                            order_id
                                        , MAX(note_code) AS note_code
                                        FROM
                                            o2pprod.RestInterface_ordersinote
                                        WHERE
                                            categoty    = 'eRequest'
                                            OR categoty = 'Non eRequest'
                                        GROUP BY
                                            order_id
                                    )
                                    SINOTEMAX
                                    ON
                                        SINOTEMAX.order_id      = SINOTEINNER.order_id
                                        AND SINOTEMAX.note_code = SINOTEINNER.note_code
                        )
                        SINOTE
                        ON
                            SINOTE.order_id = ORD.id
                WHERE
                    ORD.taken_date BETWEEN '{}' AND '{}'
            """).format(start_date, end_date)

    result = report.orion_db.query_to_list(query)
    df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # set columns to datetime type
    df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
        pd.to_datetime)

    # Write to CSV
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(df_raw, csv_main_file_path)

    # # Add CSV to zip file
    # zip_file = ("{}_{}.zip").format(filename, utils.get_current_datetime())
    # zip_file_path = os.path.join(report.reports_folder_path, zip_file)
    # report.add_to_zip_file(csv_main_file_path, zip_file_path)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.attach_file_to_email(csv_main_file_path)
    report.send_email()


def generate_report_44_no_contacts():

    report = OrionReport(configFile)

    email_subject = 'GSP Report 44 (No Contacts)'
    filename = 'gsp_report_44_no_contacts'
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

    # START: BILLING
    # df_billing = get_billing_data(report, start_date, end_date)

    # # Write to CSV
    # csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    # csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    # report.create_csv_from_df(df_billing, csv_main_file_path)
    # END: BILLING

    query = ("""
                SELECT
                    DISTINCT "TBD" AS qty,
                    "TBD" AS user_id,
                    "TBD" AS group_code,
                    ORD.arbor_disp AS arbor_service_type,
                    "TBD" AS cust_id,
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
                    ORD.submitted_date AS submitted_date,
                    "TBD" AS tat_for_sales,
                    "TBD" AS tat_for_am_to_ord_raised,
                    "TBD" AS sde_to_pd_tat,
                    "TBD" AS tat_mcc_raising_ord,
                    "TBD" AS tat_sde_processing_time,
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
                    "TBD" AS am_name,
                    ORD.am_id AS am_id,
                    ORD.am_email AS am_email,
                    "TBD" AS icon_act_ref,
                    "TBD" AS focalscope_ticket,
                    "TBD" AS impact_mobile_svc_order,
                    "TBD" AS impact_svc_ord_number,
                    "TBD" AS package_description,
                    "TBD" AS sde_wo_tat,
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
                    "TBD" AS tat_ord_applied_to_sde_rcvd,
                    "TBD" AS tat_sde_rcvd_to_ord_submtd,
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
                    ORD.taken_date BETWEEN '{}'
                    AND '{}';
            """).format(start_date, end_date)

    result = report.orion_db.query_to_list(query)
    logger.info("Processing data ...")
    df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # set columns to datetime type
    df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
        pd.to_datetime)

    # Get the list of holiday dates excluding weekends
    df_holidays = get_holidays(report)

    # calculate the number of days betwee two dates
    df_raw['tat_for_sales'] = df_raw.apply(
        lambda row: count_weekdays(row['received_date'], row['sde_received_date'], df_holidays), axis=1)
    df_raw['tat_for_am_to_ord_raised'] = df_raw.apply(
        lambda row: count_weekdays(row['applied_date'], row['submitted_date'], df_holidays), axis=1)
    df_raw['sde_to_pd_tat'] = df_raw.apply(
        lambda row: count_weekdays(row['received_date'], row['submitted_date'], df_holidays), axis=1)
    df_raw['tat_mcc_raising_ord'] = df_raw.apply(
        lambda row: count_weekdays(row['operation_received_date'], row['submitted_date'], df_holidays), axis=1)
    # tat_sde_processing_time ?
    df_raw['sde_wo_tat'] = df_raw.apply(
        lambda row: count_weekdays(row['sde_received_date'], row['submitted_date'], df_holidays), axis=1)
    # tat_ord_applied_to_sde_rcvd ?
    # tat_sde_rcvd_to_ord_submtd ?

    # Create df_main dataframe for report generation
    # df_main = pd.DataFrame(columns=[const.RAW_COLUMNS_NO_CONTACTS])
    # df_main['order_code'] = df_raw['order_code'].drop_duplicates()

    # Write to CSV
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


def get_billing_data(report, start_date, end_date):

    query = ("""
                SELECT
                    DISTINCT ORD.order_code,
                    BILL.*
                FROM
                    o2pprod.RestInterface_order ORD
                    JOIN (
                        SELECT
                            BILLINNER.*
                        FROM
                            o2pprod.RestInterface_billing BILLINNER
                            JOIN (
                                SELECT
                                    BILLMAX.component_id,
                                    BILLMAX.order_id,
                                    MAX(BILLMAX.id) AS id
                                FROM
                                    o2pprod.RestInterface_billing BILLMAX
                                    -- WHERE order_id = 4112704
                                GROUP BY
                                    BILLMAX.component_id,
                                    BILLMAX.order_id
                            ) BILLMAX ON BILLMAX.id = BILLINNER.id
                    ) BILL ON BILL.order_id = ORD.id -- AND ORD.id = 4112704
                WHERE
                    -- ORD.order_code = "YQH1722003";
                    -- ORD.taken_date BETWEEN '2023-03-01' AND '2023-03-31'
                    ORD.taken_date = '2023-03-01'
            """).format(start_date, end_date)

    result = report.orion_db.query_to_list(query)
    # df = pd.DataFrame(data=result, columns=const.RAW_COLUMNS_NO_CONTACTS)
    df = pd.DataFrame(data=result)

    # set columns to datetime type
    df[['rc_active_date', 'rc_inactive_date', 'rc_deactive_date', 'created_at', 'updated_at']] = df[['rc_active_date', 'rc_inactive_date', 'rc_deactive_date', 'created_at', 'updated_at']].apply(
        pd.to_datetime)

    # create new biling dataframe
    df_billing = pd.DataFrame(columns=['order_code', 'package_description', 'currency', 'estimated_mrc',
                              'estimated_otc', 'mainline_component_desc', 'otc_component_desc'])
    df_billing['order_code'] = df['order_code'].drop_duplicates()

    # Iterate over the unique df_billing workorders
    for idx, row in df_billing.iterrows():
        # Get the df workorders records based from the current df_billing workorder
        df_order = df[df['order_code'] == row['order_code']]

        # Iterate through the records of the extracted df_raw workorder records
        for ind in df_order.index:
            # Get the parameter values for the following parameter names
            if '(Main)' in df_order['component_description'][ind]:
                df_billing.loc[idx,
                               'mainline_component_desc'] = df_order['component_description'][ind]
                df_billing.loc[idx,
                               'currency'] = df_order['rc_billing_frequency'][ind]
            if '(VAS)' in df_order['component_description'][ind]:
                df_billing.loc[idx,
                               'otc_component_desc'] = df_order['component_description'][ind]

    # logger.info(df_billing)

    return df_billing


def get_holidays(report):

    query = ("""
                SELECT
                    Holiday,
                    Date
                FROM
                    o2ptableau.t_GSP_holidays ORD
                ;
            """)

    result = report.tableau_db.query_to_list(query)
    # df = pd.DataFrame(data=result, columns=const.RAW_COLUMNS_NO_CONTACTS)
    df = pd.DataFrame(data=result, columns=['Holiday', 'Date'])

    # set columns to datetime type
    df[['Date']] = df[['Date']].apply(
        pd.to_datetime)

    # logger.info(df)

    # Exclude weekend holidays
    df['Weekday'] = df['Date'].dt.dayofweek
    filtered_dates = df.loc[(df['Weekday'] != 5) & (df['Weekday'] != 6)]

    # logger.info(filtered_dates)

    return filtered_dates


def count_weekdays(start_date, end_date, df_holidays):
    num_weekdays = np.nan

    if pd.notnull(start_date) and pd.notnull(end_date):
        num_weekdays = np.busday_count(start_date.date(), end_date.date())
        num_wkday_holidays = sum(start_date <= date
                                 <= end_date for date in df_holidays['Date'])

        num_weekdays = num_weekdays - num_wkday_holidays + 1

    return num_weekdays
