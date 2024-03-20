SELECT
    DISTINCT ORD.order_code,
    ORD.order_taken_by AS created_by,
    ORD.service_order_number AS service_order_no,
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
    DATE_FORMAT(
        REGEXP_SUBSTR(
            SINOTE.details,
            BINARY '(?<=Old CRD:)(.*)(?= New CRD:[0-9]{{8}})'
        ),
        '%Y-%m-%d'
    ) AS old_crd,
    DATE_FORMAT(
        REGEXP_SUBSTR(
            SINOTE.details,
            BINARY '(?<=New CRD:)(.*)(?= Category Code:)'
        ),
        '%Y-%m-%d'
    ) AS new_crd,
    NOTEDLY.reason AS crd_amendment_reason,
    NOTEDLY.reason_gsp AS crd_amendment_reason_gsp,
    ORD.assignee,
    (
        CASE
            WHEN CON.order_id IS NOT NULL THEN 'PM'
            ELSE 'Non-PM'
        END
    ) AS 'pm_nonpm',
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
            AND ORD2.current_crd <= DATE_ADD('{report_date}', INTERVAL 3 MONTH)
    ) ORDGD ON ORDGD.id = ORD.id
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    JOIN RestInterface_person PER ON PER.id = ACT.person_id
    AND ACT.tag_name = 'Pegasus'
    JOIN GSP_Q_ownership GSP ON GSP.group_id = PER.role
    LEFT JOIN auto_escalation_remarks RMK ON RMK.activity_id = ACT.id
    LEFT JOIN auto_escalation_queueownerdelayreasons ACTDLY ON RMK.delay_reason_id = ACTDLY.id
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
    AND NPP.level = 'Mainline'
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
    AND PAR.parameter_name = 'Type'
    AND PAR.parameter_value IN ('1', '2', '010', '020')
    LEFT JOIN RestInterface_contactdetails CON ON CON.order_id = ORD.id
    AND CON.contact_type = "Project Manager";