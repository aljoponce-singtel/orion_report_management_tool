SELECT
    DISTINCT ORD.order_code,
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
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    AND ACT.name IN (
        'OLLC Order Ack',
        'OLLC Site Survey',
        'FOC Date Received',
        'LLC Accepted by Singtel'
    )
    JOIN RestInterface_person PER ON PER.id = ACT.person_id
    JOIN GSP_Q_ownership GSP ON GSP.group_id = PER.role
    LEFT JOIN (
        SELECT
            RMKINNER.*
        FROM
            o2pprod.auto_escalation_remarks RMKINNER
            JOIN (
                SELECT
                    activity_id,
                    MAX(id) AS id
                FROM
                    o2pprod.auto_escalation_remarks
                GROUP BY
                    activity_id
            ) RMKMAX ON RMKMAX.activity_id = RMKINNER.activity_id
            AND RMKMAX.id = RMKINNER.id
    ) RMK ON RMK.activity_id = ACT.id
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
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
    AND PAR.parameter_name IN (
        'PartnerNm',
        'OLLCAPartnerContractStartDt',
        'OLLCBPartnerContractStartDt',
        'OLLCAPartnerContractTerm',
        'OLLCATax',
        'LLC_Partner_Name',
        'LLC_Partner_Ref',
        'PartnerCctRef',
        'STPoNo',
        'STIntSvcNo',
        'IMPGcode',
        'Model'
    )
WHERE
    GSP.department LIKE "GD_%"
    AND ORD.order_status IN ('Submitted', 'Closed')
    AND ORD.current_crd >= DATE_SUB(NOW(), INTERVAL 3 MONTH);