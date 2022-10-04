SELECT
    DISTINCT ORD.order_code,
    CUS.name AS customer_name,
    ORD.order_type,
    ORD.order_status,
    ORD.order_priority,
    ORD.business_sector,
    ORD.current_crd,
    ORD.initial_crd,
    ORD.taken_date,
    ORD.sde_received_date,
    ORD.arbor_service_type,
    ORD.service_number,
    ORD.service_type,
    PRJ.project_code,
    ORD.circuit_id,
    PRD.network_product_code,
    PRD.network_product_desc,
    ACT.name AS activity_name,
    ACT.due_date,
    ACT.status,
    CAST(ACT.activity_code AS SIGNED INTEGER) AS step_no,
    ACT.ready_date,
    ACT.completed_date,
    GRP.department,
    PER.role AS group_id,
    CKT.circuit_code,
    PAR.parameter_value AS ed_pd_diversity,
    ORD.assignee,
    SITE.site_code AS exchange_code_a,
    SITE.site_code_second AS exchange_code_b,
    BRN.brn,
    RMK.created_at AS act_dly_reason_date,
    ACTDLY.reason AS act_delay_reason,
    SINOTE.date_created AS crd_amendment_date,
    NOTEDLY.reason_gsp AS crd_amendment_reason
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN auto_escalation_remarks RMK ON RMK.activity_id = ACT.id
    LEFT JOIN auto_escalation_queueownerdelayreasons ACTDLY ON RMK.delay_reason_id = ACTDLY.id
    LEFT JOIN auto_escalation_escalationgroup GRP ON GRP.person_id = PER.id
    LEFT JOIN RestInterface_ordersinote SINOTE ON SINOTE.order_id = ORD.id
    AND SINOTE.categoty = 'CRD'
    AND SINOTE.sub_categoty = 'CRD Change History'
    AND SINOTE.reason_code IS NOT NULL
    AND DATE_FORMAT(SINOTE.date_created, '%y-%m') = DATE_FORMAT(NOW(), '%y-%m')
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
    ACT.tag_name = 'Pegasus'
    AND ORD.order_status IN (
        'Submitted',
        'PONR',
        'Pending Cancellation',
        'Completed'
    )
    AND ORD.current_crd <= DATE_ADD(NOW(), INTERVAL 3 MONTH);