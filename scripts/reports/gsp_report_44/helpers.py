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


def generate_report():

    report = OrionReport(configFile)

    generate_report_44(report)
    generate_report_44_no_contacts(report)


def generate_report_44(report):

    filename = 'gsp_report_44'

    query = ("""
                SELECT
                    DISTINCT STP.OrderTakenBy AS user_id,
                    O2P.*
                FROM
                    (
                        SELECT
                            DISTINCT ORD.id AS o2p_ord_id,
                            "TBD" AS qty,
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
                            USRBRN.staff_name AS am,
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
                            "TBD" AS notes_category,
                            "TBD" AS notes_subcategory,
                            "TBD" AS mainline_component_desc,
                            "TBD" AS otc_component_desc
                        FROM
                            o2pprod.RestInterface_order ORD
                            LEFT JOIN o2pprod.RestInterface_project PRJ ON PRJ.id = ORD.project_id
                            LEFT JOIN o2pprod.RestInterface_contactdetails CON ON CON.order_id = ORD.id
                            AND CON.contact_type IN ("SDE", "Project Manager")
                            LEFT JOIN o2pprod.RestInterface_customer CUS ON CUS.id = ORD.customer_id
                            LEFT JOIN o2pprod.RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                            LEFT JOIN o2pprod.RestInterface_usercustomerbrnmapping USRBRN ON USRBRN.customer_brn_id = ORD.customer_brn_id
                            LEFT JOIN o2pprod.RestInterface_user AMUSR ON USRBRN.user_id = AMUSR.id
                        WHERE
                            ORD.taken_date BETWEEN '2023-03-01'
                            AND '2023-03-31'
                    ) O2P
                    LEFT JOIN o2pprod.tmp_ecom_max STP ON STP.order_id = O2P.o2p_ord_id;
            """)

    result = report.orion_db.query_to_list(query)
    df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

    # set columns to datetime type
    df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
        pd.to_datetime)

    # Write to CSV
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(df_raw, csv_main_file_path)


def generate_report_44_no_contacts(report):

    filename = 'gsp_report_44_no_contacts'

    query = ("""
                SELECT
                    DISTINCT STP.OrderTakenBy AS user_id,
                    O2P.*
                FROM
                    (
                        SELECT
                            DISTINCT ORD.id AS o2p_ord_id,
                            "TBD" AS qty,
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
                            "TBD" AS notes_category,
                            "TBD" AS notes_subcategory,
                            "TBD" AS mainline_component_desc,
                            "TBD" AS otc_component_desc
                        FROM
                            o2pprod.RestInterface_order ORD
                            LEFT JOIN o2pprod.RestInterface_project PRJ ON PRJ.id = ORD.project_id
                            LEFT JOIN o2pprod.RestInterface_customer CUS ON CUS.id = ORD.customer_id
                            LEFT JOIN o2pprod.RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                            LEFT JOIN o2pprod.RestInterface_usercustomerbrnmapping USRBRN ON USRBRN.customer_brn_id = ORD.customer_brn_id
                            LEFT JOIN o2pprod.RestInterface_user AMUSR ON USRBRN.user_id = AMUSR.id
                        WHERE
                            ORD.taken_date BETWEEN '2023-03-01'
                            AND '2023-03-31'
                    ) O2P
                    LEFT JOIN o2pprod.tmp_ecom_max STP ON STP.order_id = O2P.o2p_ord_id;
            """)

    result = report.orion_db.query_to_list(query)
    df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS_NO_CONTACTS)

    # set columns to datetime type
    df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
        pd.to_datetime)

    # Write to CSV
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(df_raw, csv_main_file_path)

    # Add CSV to zip file
    zip_file = ("{}_{}.zip").format(filename, utils.get_current_datetime())
    zip_file_path = os.path.join(report.reports_folder_path, zip_file)
    # report.add_to_zip_file(csv_main_file_path, zip_file_path)
