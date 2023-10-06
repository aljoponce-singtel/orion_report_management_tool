# Import built-in packages
import os
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(config_file, 'Operation War room - DPE/MPE')
    report.set_filename('pm_dpe_mpe')
    report.set_prev_week_monday_sunday_date()

    query = f"""
                SELECT
                    DISTINCT ORD.order_code,
                    CUS.name AS customer_name,
                    ORD.service_number,
                    ORD.arbor_disp,
                    ORD.order_type,
                    ORD.current_crd,
                    PRJ.project_code,
                    ORD.order_priority,
                    ORD.complete_activity,
                    ORD.total_activity,
                    ORD.taken_date,
                    ORD.job_effective_date,
                    ORD.close_date,
                    ORD.completed_date,
                    (
                        CASE
                            WHEN ORD.delivery_status = 1 THEN "WIP On Time"
                            WHEN ORD.delivery_status = 2 THEN "WIP At Risk"
                            WHEN ORD.delivery_status = 3 THEN "WIP Delay"
                            WHEN ORD.delivery_status = 4 THEN "Delivered"
                            WHEN ORD.delivery_status = 5 THEN "Delivered Delay"
                            WHEN ORD.delivery_status = 6 THEN "NA"
                        END
                    ) AS delivery_status,
                    (
                        CASE
                            WHEN ORD.closure_status = 1 THEN "Closed"
                            WHEN ORD.closure_status = 2 THEN "Closed Delay"
                            WHEN ORD.closure_status = 3 THEN "Not Closed"
                            WHEN ORD.closure_status = 4 THEN "Not Closed Delay"
                            WHEN ORD.closure_status = 5 THEN "Cancelled"
                            WHEN ORD.closure_status = 6 THEN "Pending Cancellation"
                        END
                    ) AS closure_status,
                    ORD.business_sector,
                    (
                        CASE
                            WHEN ORD.customer_crd IS NULL THEN "not_amended"
                            ELSE "customer_amended"
                        END
                    ) AS amended_reason,
                    (
                        CASE
                            WHEN MAXSINOTE.category = 1 THEN "Customer"
                            WHEN MAXSINOTE.category = 2 THEN "Singtel - Sales"
                            WHEN MAXSINOTE.category = 3 THEN "Singtel - Ops"
                            WHEN MAXSINOTE.category = 4 THEN "Overseas Service Provider"
                            WHEN MAXSINOTE.category = 5 THEN "System Integrator"
                            WHEN MAXSINOTE.category = 6 THEN "Singtel IS"
                            WHEN MAXSINOTE.category = 7 THEN "Others"
                            ELSE ""
                        END
                    ) AS category,
                    MAXSINOTE.requestor,
                    IFNULL(MAXSINOTE.delay_reason, '') AS delay_reason,
                    (
                        CASE
                            WHEN is_bulk_order = 0 THEN "FALSE"
                            WHEN is_bulk_order = 1 THEN "TRUE"
                        END
                    ) AS is_bulk_order,
                    PRD.network_product_desc AS product_description,
                    GSP.department,
                    GSP.group_id,
                    CAST(ACT.activity_code AS SIGNED INTEGER) AS act_stepno,
                    ACT.name AS activity,
                    ACT.status AS act_status,
                    ACT.due_date AS act_due_date,
                    ACT.ready_date AS act_rdy_date,
                    ACT.completed_date AS act_com_date,
                    ACTDLY.reason AS act_dly_reason
                FROM
                    o2pprod.RestInterface_order ORD
                    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                    JOIN RestInterface_person PER ON PER.id = ACT.person_id
                    JOIN GSP_Q_ownership GSP ON GSP.group_id = PER.role
                    LEFT JOIN (
                        SELECT
                            RMKINNER.*
                        FROM
                            auto_escalation_remarks RMKINNER
                            JOIN (
                                SELECT
                                    activity_id,
                                    MAX(id) AS id
                                FROM
                                    auto_escalation_remarks
                                GROUP BY
                                    activity_id
                            ) RMKMAX ON RMKMAX.id = RMKINNER.id
                            AND RMKMAX.activity_id = RMKINNER.activity_id
                    ) RMK ON RMK.activity_id = ACT.id
                    LEFT JOIN auto_escalation_queueownerdelayreasons ACTDLY ON ACTDLY.id = RMK.delay_reason_id
                    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = "Mainline"
                    AND NPP.status != "Cancel"
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN (
                        SELECT
                            SINOTE.order_id,
                            SINOTE.note_code,
                            REGEXP_SUBSTR(
                                SINOTE.details,
                                '(?<=Category Code:)(.*)(?= Reason Code:)'
                            ) AS category,
                            REGEXP_SUBSTR(
                                SINOTE.details,
                                '(?<=Original Requestor:)(.*)(?= Requestor:)'
                            ) AS requestor,
                            REGEXP_SUBSTR(
                                SINOTE.details,
                                '(?<=Remarks:)(.*)(?= Original Requestor:)'
                            ) AS delay_reason,
                            date_created
                        FROM
                            RestInterface_ordersinote SINOTE
                            JOIN (
                                SELECT
                                    order_id,
                                    MAX(note_code) AS note_code
                                FROM
                                    RestInterface_ordersinote
                                WHERE
                                    order_id IS NOT NULL
                                    AND categoty = 'CRD'
                                    AND sub_categoty = 'CRD Change History'
                                    AND order_id IN (
                                        SELECT
                                            DISTINCT ORD.id
                                        FROM
                                            o2pprod.RestInterface_order ORD
                                            JOIN o2pprod.RestInterface_ordersinote SINOTE ON SINOTE.order_id = ORD.id
                                            AND SINOTE.categoty = 'CRD'
                                            AND SINOTE.sub_categoty = 'CRD Change History'
                                    )
                                GROUP BY
                                    order_id
                            ) WRSINOTE ON WRSINOTE.order_id = SINOTE.order_id
                            AND WRSINOTE.note_code = SINOTE.note_code
                    ) MAXSINOTE ON MAXSINOTE.order_id = ORD.id
                WHERE
                    (
                        GSP.department = "GD_OMS"
                        AND ACT.name IN (
                            "OLLC Order Ack",
                            "FOC Date Received",
                            "Raise Impact Vendor Order"
                        )
                        AND ACT.status = "COM"
                        AND ACT.completed_date BETWEEN '{report.start_date}'
                        AND '{report.end_date}'
                    )
                    OR (
                        ACT.order_id IN (
                            SELECT
                                DISTINCT ACTSUB.order_id
                            FROM
                                RestInterface_activity ACTSUB
                                JOIN RestInterface_person PERSUB ON PERSUB.id = ACTSUB.person_id
                                JOIN GSP_Q_ownership GSPSUB ON GSPSUB.group_id = PERSUB.role
                            WHERE
                                GSPSUB.department = "GD_OMS"
                                AND ACTSUB.name IN ("FOC Date Received")
                                AND ACTSUB.status = "COM"
                                AND ACT.completed_date BETWEEN '{report.start_date}'
                                AND '{report.end_date}'
                        )
                        AND ACT.name LIKE ("%OLLC%")
                        AND ACT.status = "COM"
                    );
            """

    df_raw = report.query_to_dataframe(query)
    df_raw['delay_reason'] = df_raw['delay_reason'].astype(str)
    df_raw['category'] = df_raw['category'].astype(str)

    # /****** START ******/
    # There are records where the category is included in the delay reason
    # E.g.
    # category = Others
    # delay reason = Others Deployed on 15 Jan 2023
    #
    # The code blow below will remove the category as a substring in the delay reason
    # output: delay reason = Deployed on 15 Jan 2023

    # define a custom function to remove a substring in column 'delay_reason'
    # using the string in column 'category'
    def remove_substring(row: pd.DataFrame):
        return row['delay_reason'].replace(row['category'], '')
    # apply the custom function to each row in the DataFrame
    df_raw['delay_reason'] = df_raw.apply(remove_substring, axis=1)
    # trim the string
    df_raw['delay_reason'] = df_raw['delay_reason'].str.strip()
    # /****** END ******/

    # remove leading '-' character from strings in the requestor column
    df_raw['requestor'] = df_raw['requestor'].str.lstrip('-')
    # Sort records in ascending order by order_code and note_code
    df_raw = df_raw.sort_values(
        by=['order_code', 'act_stepno'], ascending=[True, True])

    # Write to CSV
    csv_file = report.create_csv_from_df(df_raw)
    # Send Email
    report.attach_file_to_email(csv_file)
    report.send_email()

    return
