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
        AND ACT.completed_date BETWEEN '2023-08-21'
        AND '2023-08-27'
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
                AND ACT.completed_date BETWEEN '2023-08-21'
                AND '2023-08-27'
        )
        AND ACT.name LIKE ("%OLLC%")
        AND ACT.status = "COM"
    );