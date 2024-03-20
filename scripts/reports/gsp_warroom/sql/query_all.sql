SELECT DISTINCT
    ORD.order_code,
    ORD.order_taken_by AS created_by,
    ORD.service_order_number AS service_order_no,
    ORD.service_number,
    CUS.name AS customer,
    ORD.order_type,
    ORD.order_status,
    ORD.taken_date,
    ORD.current_crd,
    ORD.initial_crd,
    -- ORD.close_date,
    ORD.assignee,
    (
        CASE
            WHEN CON.pm_contact IS NOT NULL THEN 'PM'
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
    SITE.location AS a_end_address,
    SITE.second_location AS b_end_address,
    BRN.brn,
    CON.a_end_cust_contact,
    CON.b_end_cust_contact,
    CON.aam_contact,
    CON.am_contact,
    CON.pm_contact,
    CON.reseller_contact,
    CON.techincal_cust_contact,
    -- ORD.am_id,
    ORD.sde_received_date,
    ORD.arbor_disp AS arbor_service,
    ORD.service_type,
    ORD.order_priority,
    ORD.speed,
    ORD.ord_action_type AS order_action_type,
    -- PAR.parameter_name,
    PAR.parameter_value AS ed_pd_diversity,
    GSP.department,
    GSP.group_id,
    CAST(
        ACT.activity_code AS SIGNED INTEGER
    ) AS step_no,
    ACT.name AS activity_name,
    ACT.due_date,
    ACT.status,
    ACT.ready_date,
    ACT.completed_date,
    RMK.created_at AS act_dly_reason_date,
    ACTDLY.reason AS act_delay_reason,
    SINOTE.note_code,
    SINOTE.date_created AS crd_amendment_date,
    DATE_FORMAT(
        REGEXP_SUBSTR(
            SINOTE.details, BINARY '(?<=Old CRD:)(.*)(?= New CRD:[0-9]{{8}})'
        ), '%Y-%m-%d'
    ) AS old_crd,
    DATE_FORMAT(
        REGEXP_SUBSTR(
            SINOTE.details, BINARY '(?<=New CRD:)(.*)(?= Category Code:)'
        ), '%Y-%m-%d'
    ) AS new_crd,
    -- SINOTE.details AS crd_amendment_details,
    NOTEDLY.reason AS crd_amendment_reason,
    NOTEDLY.reason_gsp AS crd_amendment_reason_gsp
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    JOIN RestInterface_person PER ON PER.id = ACT.person_id
    AND ACT.tag_name = 'Pegasus'
    JOIN GSP_Q_ownership GSP ON GSP.group_id = PER.role
    LEFT JOIN auto_escalation_remarks RMK ON RMK.activity_id = ACT.id
    LEFT JOIN auto_escalation_queueownerdelayreasons ACTDLY ON RMK.delay_reason_id = ACTDLY.id
    LEFT JOIN (
        SELECT SINOTEINNER.*
        FROM
            o2pprod.RestInterface_ordersinote SINOTEINNER
            JOIN (
                SELECT order_id, MAX(note_code) AS note_code
                FROM o2pprod.RestInterface_ordersinote
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
    LEFT JOIN (
        SELECT
            order_id, MAX(
                CASE
                    WHEN contact_type = "A-end-Cust" THEN email_address
                END
            ) a_end_cust_contact, MAX(
                CASE
                    WHEN contact_type = "B-end-Cust" THEN email_address
                END
            ) b_end_cust_contact, MAX(
                CASE
                    WHEN contact_type = "AAM" THEN email_address
                END
            ) aam_contact, MAX(
                CASE
                    WHEN contact_type = "AM" THEN email_address
                END
            ) am_contact, MAX(
                CASE
                    WHEN contact_type = "Project Manager" THEN email_address
                END
            ) pm_contact, MAX(
                CASE
                    WHEN contact_type = "Reseller" THEN email_address
                END
            ) reseller_contact, MAX(
                CASE
                    WHEN contact_type = "Technical-Cust" THEN email_address
                END
            ) techincal_cust_contact
        FROM RestInterface_contactdetails
        GROUP BY
            order_id
    ) CON ON CON.order_id = ORD.id
WHERE
    ORD.order_status IN (
        'Submitted', 'PONR', 'Pending Cancellation', 'Completed'
    )
    AND ORD.current_crd <= DATE_ADD(
        '{report_date}', INTERVAL 3 MONTH
    )
ORDER BY ORD.current_crd DESC, ORD.order_code, step_no;