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